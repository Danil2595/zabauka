import os
import requests

TOKEN = os.environ["TG_BOT_TOKEN"]              # берём из Secrets
CHANNEL = os.environ.get("TG_CHANNEL")          # задан в workflow

TEXT = "✅ Zabauka на связи! Первый тестовый пост из GitHub Actions."

def send_message(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": CHANNEL, "text": text}
    r = requests.post(url, data=data, timeout=20)
    r.raise_for_status()
    return r.json()

if name == "__main__":
    resp = send_message(TEXT)
    print(resp)
