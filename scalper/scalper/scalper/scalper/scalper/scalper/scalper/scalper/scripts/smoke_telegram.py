# scripts/smoke_telegram.py
"""Direct Telegram API send — bypass bot, confirm token/chat actually send."""
import os
import json
import urllib.request
import urllib.parse
from dotenv import load_dotenv, find_dotenv


def main():
    p = find_dotenv(usecwd=True)
    load_dotenv(p)

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    print("dotenv:", p)
    print("token_set:", bool(token), "chat_set:", bool(chat), "chat_len:", len(chat or ""))

    if not token or not chat:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat, "text": "SMOKE TEST ✅ from scripts/smoke_telegram.py"}
    data = urllib.parse.urlencode(payload).encode()

    resp = urllib.request.urlopen(url, data=data, timeout=15).read().decode("utf-8")
    j = json.loads(resp)
    print("telegram_ok:", j.get("ok"))
    if not j.get("ok"):
        print(resp)


if __name__ == "__main__":
    main()
