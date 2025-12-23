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
import datetime
import logging
import random
import string

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
    username = Column(String, unique=True, index=True) # NEW: Username
    messages = relationship("ChatMessage", back_populates="user")
    
    # Relationships
    # Removed problematic bidirectional relationship for now, querying directly is safer.

class Friendship(Base):
    __tablename__ = "friendships"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    friend_id = Column(Integer, ForeignKey("users.id"))
    status = Column(String, default="pending") # pending, accepted, blocked
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # No back_populates needed if User doesn't have the relationship
    user = relationship("User", foreign_keys=[user_id])
    friend = relationship("User", foreign_keys=[friend_id])

class DirectMessage(Base):
    __tablename__ = "direct_messages"
    id = Column(Integer, primary_key=True, index=True)
    sender_id = Column(Integer, ForeignKey("users.id"))
    receiver_id = Column(Integer, ForeignKey("users.id"))
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    read_at = Column(DateTime, nullable=True)

    sender = relationship("User", foreign_keys=[sender_id])
    receiver = relationship("User", foreign_keys=[receiver_id])

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

class UserDiary(Base):
    __tablename__ = "user_diaries"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    content = Column(Text)
    date = Column(String) # YYYY-MM-DD
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    user = relationship("User", back_populates="diaries")

User.diaries = relationship("UserDiary", back_populates="user")

# Auth Config
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
import asyncio
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
            # MIGRATION: Add username
            try:
                conn.execute(text("ALTER TABLE users ADD COLUMN username VARCHAR UNIQUE"))
                conn.commit()
                logger.info("Added 'username' column to users via migration hack.")
            except Exception as e:
                pass
            
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
    username: str # Return username
    user_id: int

def generate_username(email: str) -> str:
    base = email.split('@')[0]
    # Remove dots and special chars
    base = "".join(c for c in base if c.isalnum())
    # Add random suffix
    suffix = ''.join(random.choices(string.digits, k=4))
    return f"{base}{suffix}"

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
        user = db.query(User).filter(User.google_id == userid).first()
        if not user:
            new_username = generate_username(email)
            # Ensure uniqueness (simple retry logic could be better but sufficient for now)
            while db.query(User).filter(User.username == new_username).first():
                new_username = generate_username(email)
            
            user = User(email=email, google_id=userid, username=new_username)
            db.add(user)
            db.commit()
            db.refresh(user)
        
        # Backfill username if missing for existing users
        if not user.username:
            new_username = generate_username(email)
            while db.query(User).filter(User.username == new_username).first():
                new_username = generate_username(email)
            user.username = new_username
            db.commit()
            db.refresh(user)
            
        # Create JWT
        access_token_expires = datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode = {"sub": str(user.id), "email": user.email}
        expire = datetime.datetime.utcnow() + access_token_expires
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        
        return {"access_token": encoded_jwt, "token_type": "bearer", "username": user.username, "user_id": user.id}
        
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

# --- DIARY ENDPOINTS ---

class DiaryCreate(BaseModel):
    content: str
    date: str # YYYY-MM-DD

class DiaryUpdate(BaseModel):
    content: str

class DiaryResponse(BaseModel):
    id: int
    content: str
    date: str
    created_at: datetime.datetime

    class Config:
        orm_mode = True

@app.post("/diary", response_model=DiaryResponse)
def create_diary_entry(entry: DiaryCreate, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Check if entry already exists for this date? Optional. For now, allow multiple or just insert.
    # Let's assume one entry per day for simplicity, or just append distinct entries. 
    # Current requirement doesn't specify strict one-per-day, so we'll just create new entries.
    
    new_entry = UserDiary(
        user_id=user.id,
        content=entry.content,
        date=entry.date
    )
    db.add(new_entry)
    db.commit()
    db.refresh(new_entry)
    return new_entry

@app.get("/diary", response_model=list[DiaryResponse])
def get_diary_entries(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    entries = db.query(UserDiary).filter(UserDiary.user_id == user.id).order_by(UserDiary.date.desc(), UserDiary.created_at.desc()).all()
    return entries

@app.put("/diary/{entry_id}", response_model=DiaryResponse)
def update_diary_entry(entry_id: int, entry: DiaryUpdate, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    db_entry = db.query(UserDiary).filter(UserDiary.id == entry_id, UserDiary.user_id == user.id).first()
    if not db_entry:
        raise HTTPException(status_code=404, detail="Diary entry not found")
    
    db_entry.content = entry.content
    db.commit()
    db.refresh(db_entry)
    return db_entry

@app.delete("/diary/{entry_id}")
def delete_diary_entry(entry_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    db_entry = db.query(UserDiary).filter(UserDiary.id == entry_id, UserDiary.user_id == user.id).first()
    if not db_entry:
        raise HTTPException(status_code=404, detail="Diary entry not found")
    
    db.delete(db_entry)
    db.commit()
    return {"status": "success"}

# --- SOCIAL ENDPOINTS ---

class UserSearchResponse(BaseModel):
    id: int
    username: str
    email: str # Maybe hide this? showing for now to verify.

class FriendRequest(BaseModel):
    username: str

class FriendshipResponse(BaseModel):
    id: int
    friend_id: int
    friend_username: str
    status: str
    created_at: datetime.datetime

class MessageSend(BaseModel):
    receiver_id: int
    content: str

class MessageResponse(BaseModel):
    id: int
    sender_id: int
    receiver_id: int
    content: str
    created_at: datetime.datetime
    is_me: bool

class UserUpdate(BaseModel):
    username: str

@app.get("/users/me", response_model=UserSearchResponse)
def get_user_profile(user: User = Depends(get_current_user)):
    return {"id": user.id, "username": user.username, "email": user.email}

@app.put("/users/me", response_model=UserSearchResponse)
def update_user_profile(update: UserUpdate, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Validate username format (simple check)
    new_username = update.username.strip()
    if not new_username:
        raise HTTPException(status_code=400, detail="Username cannot be empty")
    if len(new_username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
    
    # Check if unchanged
    if new_username == user.username:
        return {"id": user.id, "username": user.username, "email": user.email}

    # Check uniqueness
    existing = db.query(User).filter(User.username == new_username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already taken")
    
    user.username = new_username
    db.commit()
    db.refresh(user)
    
    return {"id": user.id, "username": user.username, "email": user.email}

@app.get("/users/search", response_model=list[UserSearchResponse])
def search_users(q: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not q:
        return []
    # Find users by username match (exclude self)
    users = db.query(User).filter(User.username.ilike(f"%{q}%"), User.id != user.id).limit(10).all()
    return [{"id": u.id, "username": u.username, "email": u.email} for u in users]

@app.post("/friends/request")
def send_friend_request(req: FriendRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    target_user = db.query(User).filter(User.username == req.username).first()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
    if target_user.id == user.id:
        raise HTTPException(status_code=400, detail="Cannot add self")

    # Check existing
    existing = db.query(Friendship).filter(
        ((Friendship.user_id == user.id) & (Friendship.friend_id == target_user.id)) |
        ((Friendship.user_id == target_user.id) & (Friendship.friend_id == user.id))
    ).first()
    
    if existing:
        if existing.status == 'accepted':
             raise HTTPException(status_code=400, detail="Already friends")
        if existing.status == 'pending':
             raise HTTPException(status_code=400, detail="Request already pending")
    
    # Create request
    # Store as User -> Target (pending)
    new_friendship = Friendship(user_id=user.id, friend_id=target_user.id, status="pending")
    db.add(new_friendship)
    db.commit()
    return {"status": "sent"}

@app.get("/friends/requests", response_model=list[FriendshipResponse])
def get_friend_requests(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Incoming requests: I am the friend_id, status is pending
    reqs = db.query(Friendship).filter(Friendship.friend_id == user.id, Friendship.status == 'pending').all()
    
    results = []
    for r in reqs:
        # The 'user' of the friendship is the one who SENT the request
        sender = db.query(User).filter(User.id == r.user_id).first()
        
        # Lazy backfill username
        if not sender.username:
            sender.username = generate_username(sender.email)
            db.commit()
            
        results.append({
            "id": r.id, # Friendship ID
            "friend_id": sender.id,
            "friend_username": sender.username,
            "status": "incoming",
            "created_at": r.created_at
        })
    return results

@app.post("/friends/accept/{friendship_id}")
def accept_friend_request(friendship_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Find request where I am the target
    req = db.query(Friendship).filter(Friendship.id == friendship_id, Friendship.friend_id == user.id, Friendship.status == 'pending').first()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    
    req.status = 'accepted'
    db.commit()
    return {"status": "accepted"}

@app.post("/friends/reject/{friendship_id}")
def reject_friend_request(friendship_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    req = db.query(Friendship).filter(Friendship.id == friendship_id, Friendship.friend_id == user.id, Friendship.status == 'pending').first()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    
    db.delete(req)
    db.commit()
    return {"status": "rejected"}

@app.get("/friends", response_model=list[FriendshipResponse])
def get_friends(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Find all accepted friendships where I am user_id OR friend_id
    friends_rel = db.query(Friendship).filter(
        ((Friendship.user_id == user.id) | (Friendship.friend_id == user.id)) & 
        (Friendship.status == 'accepted')
    ).all()
    
    results = []
    for f in friends_rel:
        # Determine who the 'other' person is
        is_me_sender = f.user_id == user.id
        other_id = f.friend_id if is_me_sender else f.user_id
        
        other_user = db.query(User).filter(User.id == other_id).first()
        
        # Lazy backfill username
        if not other_user.username:
            other_user.username = generate_username(other_user.email)
            db.commit()

        results.append({
            "id": f.id,
            "friend_id": other_user.id,
            "friend_username": other_user.username,
            "status": "accepted",
            "created_at": f.created_at
        })
    return results

@app.delete("/friends/{friend_id}")
def unfriend(friend_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Find friendship
    rel = db.query(Friendship).filter(
        ((Friendship.user_id == user.id) & (Friendship.friend_id == friend_id)) |
        ((Friendship.user_id == friend_id) & (Friendship.friend_id == user.id))
    ).first()
    
    if not rel:
        raise HTTPException(status_code=404, detail="Friendship not found")
    
    # Also delete DMs? Spec says "messages and friends list should be removed"
    # So we delete relevant DMs too? Or just hide? "removed of that person" implies deletion or hiding.
    # Let's delete DMs for privacy/safety as requested.
    db.query(DirectMessage).filter(
        ((DirectMessage.sender_id == user.id) & (DirectMessage.receiver_id == friend_id)) |
        ((DirectMessage.sender_id == friend_id) & (DirectMessage.receiver_id == user.id))
    ).delete()
    
    db.delete(rel)
    db.commit()
    return {"status": "removed"}


# --- DM ENDPOINTS ---

def check_friendship(user_id: int, other_id: int, db: Session):
    return db.query(Friendship).filter(
        ((Friendship.user_id == user_id) & (Friendship.friend_id == other_id)) |
        ((Friendship.user_id == other_id) & (Friendship.friend_id == user_id)),
        Friendship.status == 'accepted'
    ).first()

@app.get("/messages/recent")
def get_recent_conversations(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Get all friends
    friends_list = get_friends(user, db)
    
    convos = []
    for f in friends_list:
        fid = f.friend_id
        # Get last message
        last_msg = db.query(DirectMessage).filter(
            ((DirectMessage.sender_id == user.id) & (DirectMessage.receiver_id == fid)) |
            ((DirectMessage.sender_id == fid) & (DirectMessage.receiver_id == user.id))
        ).order_by(DirectMessage.created_at.desc()).first()
        
        convos.append({
            "friend_id": fid,
            "friend_username": f.friend_username,
            "last_message": last_msg.content if last_msg else "No messages yet",
            "last_message_time": last_msg.created_at if last_msg else None
        })
    
    # Sort by time
    convos.sort(key=lambda x: x['last_message_time'] or datetime.datetime.min, reverse=True)
    return convos

@app.get("/messages/{friend_id}", response_model=list[MessageResponse])
def get_messages(friend_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not check_friendship(user.id, friend_id, db):
        raise HTTPException(status_code=403, detail="Not friends")

    msgs = db.query(DirectMessage).filter(
        ((DirectMessage.sender_id == user.id) & (DirectMessage.receiver_id == friend_id)) |
        ((DirectMessage.sender_id == friend_id) & (DirectMessage.receiver_id == user.id))
    ).order_by(DirectMessage.created_at.asc()).all() # Oldest first for chat UI
    
    return [
        {
            "id": m.id,
            "sender_id": m.sender_id,
            "receiver_id": m.receiver_id,
            "content": m.content,
            "created_at": m.created_at,
            "is_me": m.sender_id == user.id
        }
        for m in msgs
    ]

@app.post("/messages")
def send_message(msg: MessageSend, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not check_friendship(user.id, msg.receiver_id, db):
        raise HTTPException(status_code=403, detail="Not friends")
    
    new_msg = DirectMessage(
        sender_id=user.id,
        receiver_id=msg.receiver_id,
        content=msg.content
    )
    db.add(new_msg)
    db.commit()
    return {"status": "sent"}
