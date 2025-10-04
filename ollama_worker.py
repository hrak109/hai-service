import requests
import time
import subprocess
import json

WP_URL = "https://oakhillpines.com"
SECRET = "a8Fj39sdlfjKJ!93jf02"

def fetch_pending():
    url = f"{WP_URL}/wp-admin/admin-ajax.php"
    payload = {"action": "ollama_get_pending"}
    # We'll create this endpoint in a moment
    return requests.post(url, data=payload).json()

def update_answer(qid, answer):
    url = f"{WP_URL}/wp-admin/admin-ajax.php"
    payload = {
        "action": "ollama_update_answer",
        "id": qid,
        "answer": answer,
        "secret": SECRET
    }
    requests.post(url, data=payload)

def run_ollama(question):
    # Example call to local Ollama (replace model if needed)
    proc = subprocess.Popen(
        ["ollama", "run", "oakhillpines-gemma3", '--keepalive 30m'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    out, _ = proc.communicate(input=question)
    return out.strip()

# Main loop
while True:
    try:
        # For simplicity, fetch one pending question directly from DB via WP
        r = requests.get(f"{WP_URL}/wp-json/ollama/v1/next")
        if r.status_code == 200:
            data = r.json()
            if data and data.get("id"):
                qid = data["id"]
                question = data["question"]
                print(f"Processing #{qid}: {question}")
                answer = run_ollama(question)
                update_answer(qid, answer)
        time.sleep(5)
    except Exception as e:
        print("Error:", e)
        time.sleep(10)