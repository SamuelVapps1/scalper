import requests

def send_telegram(token: str, chat_id: str, text: str) -> None:
    """
    Send Telegram message with explicit credentials.
    Caller validates inputs and handles logging to avoid leaking secrets.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, json=payload, timeout=2.5)
    response.raise_for_status()
