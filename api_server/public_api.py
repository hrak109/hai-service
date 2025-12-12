
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis
import uuid
import os

app = FastAPI()

origins = os.getenv("ALLOWED_ORIGINS").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

class Question(BaseModel):
    q_text: str

@app.post("/ask")
def ask_question(q: Question):
    if not q.q_text.strip():
        raise HTTPException(status_code=400, detail="Empty question")

    if "|" in q.q_text:
        raise HTTPException(status_code=400, detail="Invalid characters in question")

    question_id = str(uuid.uuid4())
    # auth_params is hardcoded for anonymous users
    # We could restrict this further (e.g., only "public" access)
    auth_params = "user:anonymous"

    # Push to Redis Queue (Public Queue)
    r.rpush("questions:public", f"{question_id}|{q.q_text}|{auth_params}")
    return {"question_id": question_id, "status": "queued"}

@app.get("/get_answer/{question_id}")
def get_answer(question_id: str):
    key = f"answer:{question_id}"
    answer = r.get(key)

    if answer:
        return {"question_id": question_id, "status": "answered", "answer": answer}
    else:
        return {"question_id": question_id, "status": "queued"}
