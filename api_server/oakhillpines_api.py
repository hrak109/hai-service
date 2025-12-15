from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from aiokafka import AIOKafkaProducer
import json
import uuid
import os
import datetime
import logging
import asyncio
from contextlib import asynccontextmanager

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB Setup
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/hai_db"

engine = create_engine(DATABASE_URL, pool_size=20, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=True) # OHP might not have users
    role = Column(String)
    content = Column(Text)
    model = Column(String)
    request_id = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        logger.warning(f"DB Init: {e}")
        
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    
    # Retry loop for Kafka connection
    while True:
        try:
            await producer.start()
            logger.info("Kafka producer connected successfully.")
            break
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            
    yield
    await producer.stop()

app = FastAPI(lifespan=lifespan)
producer = None

origins = os.environ["ALLOWED_ORIGINS"].split(",")

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

class Question(BaseModel):
    q_text: str

@app.post("/ask")
async def ask_question(q: Question, db: Session = Depends(get_db)):
    if not q.q_text.strip():
        raise HTTPException(status_code=400, detail="Empty question")

    if "|" in q.q_text:
        raise HTTPException(status_code=400, detail="Invalid characters in question")

    question_id = str(uuid.uuid4())
    
    # Save Request
    req = ChatMessage(
        request_id=question_id,
        content=q.q_text,
        role='user',
        model=os.getenv("DEFAULT_MODEL", "ohp-llama3.2:3b")
    )
    db.add(req)
    db.commit()
    
    # Send to Kafka

    # Send to Kafka
    payload = {
        "question_id": question_id,
        "text": q.q_text,
        "model": os.getenv("DEFAULT_MODEL", "ohp-llama3.2:3b"),
        "service": "oakhillpines"
    }
    await producer.send_and_wait("questions-oakhillpines", json.dumps(payload).encode('utf-8'))

    return {"question_id": question_id, "status": "queued"}

@app.get("/get_answer/{question_id}")
def get_answer(question_id: str, db: Session = Depends(get_db)):
    msg = db.query(ChatMessage).filter(ChatMessage.request_id == question_id, ChatMessage.role == 'assistant').first()

    if msg:
        return {"question_id": question_id, "status": "answered", "answer": msg.content}
    else:
        return {"question_id": question_id, "status": "queued"}
