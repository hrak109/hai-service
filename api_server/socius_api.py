from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from jose import JWTError, jwt
from jose import JWTError, jwt
from aiokafka import AIOKafkaProducer
import json
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
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/hai_db"
engine = create_engine(DATABASE_URL, pool_size=20, max_overflow=10)
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
    model = Column(String, default="hbb-llama3.2:3b") # NEW: Store model used
    request_id = Column(String) # For correlation
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
        
        # MIGRATION HACK: Check if 'model' column exists, if not add it.
        # This is needed because create_all doesn't update existing tables.
        with engine.connect() as conn:
            from sqlalchemy import text
            try:
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN model VARCHAR"))
                conn.commit()
                logger.info("Added 'model' column to chat_messages via migration hack.")
            except Exception as e:
                pass

            try:
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN request_id VARCHAR"))
                conn.commit()
                logger.info("Added 'request_id' column to chat_messages via migration hack.")
            except Exception as e:
                pass
                
    except Exception as e:
        logger.warning(f"DB Init race condition: {e}")
    
    # Init Kafka Producer
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    await producer.start()
    
    yield
    
    await producer.stop()

app = FastAPI(lifespan=lifespan)
producer = None

origins = os.getenv("ALLOWED_ORIGINS").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



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
    model: str = None # Allow None, fallback in endpoint

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
async def ask_question(q: Question, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not q.q_text.strip():
        raise HTTPException(status_code=400, detail="Empty question")

    question_id = str(uuid.uuid4())
    
    # Determine model
    selected_model = q.model if q.model else os.getenv("DEFAULT_MODEL")
    
    # NEW: Save User Message with Model
    user_msg = ChatMessage(user_id=user.id, role="user", content=q.q_text, model=selected_model, request_id=question_id)
    db.add(user_msg)
    db.commit()

    # Send to Kafka
    payload = {
        "question_id": question_id,
        "text": q.q_text,
        "model": selected_model, 
        "service": "socius",
        "user_id": user.id
    }
    await producer.send_and_wait("questions-socius", json.dumps(payload).encode('utf-8'))
    
    return {"question_id": question_id, "status": "queued"}

@app.get("/get_answer/{question_id}")
def get_answer(question_id: str, model: str = "hbb-llama3.2:3b", user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Poll DB for answer
    msg = db.query(ChatMessage).filter(ChatMessage.request_id == question_id, ChatMessage.role == 'assistant').first()

    if msg:
        return {"question_id": question_id, "status": "answered", "answer": msg.content}
    else:
        return {"question_id": question_id, "status": "queued"}

@app.get("/history")
def get_history(model: str = None, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    query = db.query(ChatMessage).filter(ChatMessage.user_id == user.id)
    
    if model:
        # Filter by specific model if provided
        query = query.filter(ChatMessage.model == model)
        
    # Retrieve last 50 messages
    messages = query.order_by(ChatMessage.created_at.desc()).limit(50).all()
    # Reverse to return in chronological order [Older ... Newer]
    return messages[::-1]
