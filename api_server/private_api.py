from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
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

# Create tables (simple migration)
Base.metadata.create_all(bind=engine)

# Auth Config
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

app = FastAPI()

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
    # auth_params is now derived from the authenticated user if needed, 
    # or passed explicitly but verified against user permissions.
    # For now, we'll keep it simple.

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
def ask_question(q: Question, user: User = Depends(get_current_user)):
    if not q.q_text.strip():
        raise HTTPException(status_code=400, detail="Empty question")

    question_id = str(uuid.uuid4())
    # We can inject user-specific auth params here
    auth_params = f"user:{user.id}" 
    
    r.rpush("questions:private", f"{question_id}|{q.q_text}|{auth_params}")
    return {"question_id": question_id, "status": "queued"}

@app.get("/get_answer/{question_id}")
def get_answer(question_id: str, user: User = Depends(get_current_user)):
    key = f"answer:{question_id}"
    answer = r.get(key)

    if answer:
        return {"question_id": question_id, "status": "answered", "answer": answer}
    else:
        return {"question_id": question_id, "status": "queued"}
