import requests

BOT_TOKEN = "8676635408:AAH1A6DSw3P7cyvhUtpzqx8s9-8Nq3RfIM4"
CHAT_ID = "-1003523484797"

url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

payload = {
    "chat_id": CHAT_ID,
    "text": "1XW Telegram alert working"
}

r = requests.post(url, json=payload)

print(r.status_code)
print(r.text)