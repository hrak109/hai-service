from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from jose import JWTError, jwt
import redis
import uuid
import os
import datetime
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Setup
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/hai_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    google_id = Column(String, unique=True, index=True)
    messages = relationship("ChatMessage", back_populates="user")

class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    role = Column(String) # 'user' or 'assistant'
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    user = relationship("User", back_populates="messages")

# Auth Config
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Create tables if they don't exist
    # Wrapped in try/except to handle race conditions with multiple workers
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created (or already matched).")
    except Exception as e:
        # Ignore race conditions where another worker created it simultaneously
        logger.warning(f"DB Init race condition (likely safe to ignore): {e}")
    yield

app = FastAPI(lifespan=lifespan)

origins = os.getenv("ALLOWED_ORIGINS").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class GoogleAuth(BaseModel):
    id_token: str

class Token(BaseModel):
    access_token: str
    token_type: str

class Question(BaseModel):
    q_text: str
    model: str = "hbb-llama3.2:3b" # Default if not provided

@app.post("/auth/google", response_model=Token)
def google_login(auth: GoogleAuth, db: Session = Depends(get_db)):
    try:
        # Verify Google Token
        # In production, pass the CLIENT_ID as the second argument to verify_oauth2_token
        idinfo = id_token.verify_oauth2_token(auth.id_token, google_requests.Request(), GOOGLE_CLIENT_ID)
        
        userid = idinfo['sub']
        email = idinfo.get('email', '')
        
        # Check/Create User
        user = db.query(User).filter(User.google_id == userid).first()
        if not user:
            user = User(email=email, google_id=userid)
            db.add(user)
            db.commit()
            db.refresh(user)
            
        # Create JWT
        access_token_expires = datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode = {"sub": str(user.id), "email": user.email}
        expire = datetime.datetime.utcnow() + access_token_expires
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        
        return {"access_token": encoded_jwt, "token_type": "bearer"}
        
    except ValueError as e:
        logger.error(f"Auth Error: {e}")
        raise HTTPException(status_code=401, detail="Invalid Google Token")

# Dependency for protected routes
def get_current_user(authorization: str = Header(None), db: Session = Depends(get_db)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization Header")
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
             raise HTTPException(status_code=401, detail="Invalid Authentication Scheme")
             
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid Token")
            
        user = db.query(User).filter(User.id == int(user_id)).first()
        if user is None:
             raise HTTPException(status_code=401, detail="User not found")
        return user
        
    except (JWTError, ValueError):
        raise HTTPException(status_code=401, detail="Could not validate credentials")

@app.post("/ask")
def ask_question(q: Question, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not q.q_text.strip():
        raise HTTPException(status_code=400, detail="Empty question")

    question_id = str(uuid.uuid4())
    
    # NEW: Save User Message
    user_msg = ChatMessage(user_id=user.id, role="user", content=q.q_text)
    db.add(user_msg)
    db.commit()

    r.rpush("questions:private", f"{question_id}|{q.q_text}|{q.model}")
    return {"question_id": question_id, "status": "queued"}

@app.get("/get_answer/{question_id}")
def get_answer(question_id: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    key = f"answer:{question_id}"
    answer = r.get(key)

    if answer:
        assistant_msg = ChatMessage(user_id=user.id, role="assistant", content=answer)
        db.add(assistant_msg)
        db.commit()

        return {"question_id": question_id, "status": "answered", "answer": answer}
    else:
        return {"question_id": question_id, "status": "queued"}

@app.get("/history")
def get_history(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Retrieve last 50 messages
    messages = db.query(ChatMessage).filter(ChatMessage.user_id == user.id).order_by(ChatMessage.created_at.desc()).limit(50).all()
    # Reverse to return in chronological order [Older ... Newer]
    return messages[::-1]
