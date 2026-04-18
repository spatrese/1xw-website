import requests

BOT_TOKEN = "8789123634:AAHRd1iBDl6Qarz3q13PSgg9v6FP7D4ouR0"
CHAT_ID = "-1003523484797"

url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

payload = {
    "chat_id": CHAT_ID,
    "text": "1XW Telegram alert working"
}

r = requests.post(url, json=payload)

print(r.status_code)
print(r.text)