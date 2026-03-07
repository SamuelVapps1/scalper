import os
from typing import List

from dotenv import load_dotenv

load_dotenv()

BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")

WATCHLIST_RAW = os.getenv("WATCHLIST", "")
WATCHLIST: List[str] = [s.strip().upper() for s in WATCHLIST_RAW.split(",") if s.strip()]

INTERVAL = os.getenv("INTERVAL", "15")
LOOKBACK = int(os.getenv("LOOKBACK", "300"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "60"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", os.getenv("CHAT_ID", ""))
