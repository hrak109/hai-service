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
                    session.commit()
                    logger.info(f"Saved answer for user {payload.get('user_id')} (Service: {service})")
                        
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
    asyncio.run(consume())
