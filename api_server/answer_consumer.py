import os
import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import text
import datetime

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Access Environment Variables for DB connections
# We assume we have access to all DB credentials or they are uniform
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# DB Engines
# Map service_name -> engine
engines = {}

def get_engine(host):
    url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:5432/hai_db"
    return create_engine(url, pool_size=20, max_overflow=10)

# Initialize engines
try:
    engines['private'] = get_engine(os.environ["POSTGRES_HOST_PRIVATE"])
    engines['socius'] = get_engine(os.environ["POSTGRES_HOST_SOCIUS"])
    engines['oakhillpines'] = get_engine(os.environ["POSTGRES_HOST_OAKHILLPINES"]) 
except Exception as e:
    logger.error(f"Failed to init engines: {e}")

Base = declarative_base()

# Minimal Models for Insertion
class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=True)
    role = Column(String)
    content = Column(Text)
    model = Column(String)
    request_id = Column(String) # For correlation
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    expo_push_token = Column(String, nullable=True)

def create_tables():
    # Attempt to create tables in all DBs if not exist
    for name, engine in engines.items():
        try:
            Base.metadata.create_all(engine)
            # Ensure request_id column exists (migration hack for existing ChatMessage)
            if name in ['private', 'socius']:
                with engine.connect() as conn:
                    try:
                        conn.execute(text("ALTER TABLE chat_messages ADD COLUMN request_id VARCHAR"))
                        conn.commit()
                        logger.info(f"Added request_id to {name}")
                    except Exception as e:
                        pass # Ignore if exists
        except Exception as e:
            logger.error(f"Table create error for {name}: {e}")

# Push Notification Logic
from exponent_server_sdk import (
    PushClient,
    PushMessage,
    PushServerError,
    PushTicketError,
)
import requests.exceptions

def send_push_notification(token, title, message, data=None):
    try:
        response = PushClient().publish(
            PushMessage(to=token,
                        title=title,
                        body=message,
                        data=data,
                        sound="default",
                        channel_id="default",
                        badge=1)) # Increment badge? Or just set? For now just send.
    except PushServerError as exc:
        logger.error(f"Push Server Error: {exc.errors} | {exc.response_data}")
    except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as exc:
         logger.error(f"Push Connection Error: {exc}")


async def consume():
    consumer = AIOKafkaConsumer(
        'answers',
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id="answer-consumer-group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode('utf-8'))
                logger.info(f"Received answer for {payload.get('question_id')}")
                
                service = payload.get('service')
                engine = engines.get(service)
                
                if not engine:
                    logger.error(f"Unknown service: {service}")
                    continue
                
                Session = sessionmaker(bind=engine)
                session = Session()
                
                try:
                    # Generic Insert ChatMessage
                    new_msg = ChatMessage(
                        user_id=payload.get('user_id'),
                        role='assistant',
                        content=payload['answer'],
                        model=payload.get('model'),
                        request_id=payload.get('question_id')
                    )
                    session.add(new_msg)
                    
                    # Fetch User for Push Token (only valid for 'socius' usually, assuming same DB for user)
                    # Note: User table might be in 'private' DB but 'socius' service? 
                    # Assuming User table is in the SAME DB as the message for simplicity, OR we check 'private' DB if user_id is global.
                    # In this architecture, it seems users are in 'private' db mostly? 
                    # Let's check where 'socius_api.py' connects. It uses 'private' for auth?
                    # socius_api.py uses `engines['private']` for `get_db`.
                    # So we should look up user in `engines['private']`.
                    
                    user = None
                    if payload.get('user_id'):
                        # Look up user in the same engine as the service (e.g., socius users in socius DB)
                        # socius_api uses the socius engine for users.
                        user_session = sessionmaker(bind=engine)()
                        try:
                            user = user_session.query(User).filter(User.id == payload.get('user_id')).first()
                        finally:
                            user_session.close()

                    session.commit()
                    logger.info(f"Saved answer for user {payload.get('user_id')} (Service: {service})")
                    
                    # Send Notification
                    if user and user.expo_push_token:
                        send_push_notification(
                            user.expo_push_token,
                            "Socius",
                            "Socius has replied to you.",
                            {"type": "new_answer", "question_id": payload.get('question_id')}
                        )
                        logger.info(f"Sent push notification to user {user.id}")
                        
                except Exception as e:
                    logger.error(f"DB Error: {e}")
                    session.rollback()
                finally:
                    session.close()

            except Exception as e:
                logger.error(f"Message process error: {e}")
                
    finally:
        await consumer.stop()

if __name__ == "__main__":
    create_tables()
    while True:
        try:
            asyncio.run(consume())
        except Exception as e:
            logger.error(f"Consumer crashed: {e}. Retrying in 5 seconds...")
            import time
            time.sleep(5)
