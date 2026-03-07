import os
import importlib.util
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()

BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")

WATCHLIST_RAW = os.getenv("WATCHLIST", "")
WATCHLIST: List[str] = [s.strip().upper() for s in WATCHLIST_RAW.split(",") if s.strip()]

WATCHLIST_MODE = os.getenv("WATCHLIST_MODE", "static").strip().lower()
WATCHLIST_UNIVERSE_N = int(os.getenv("WATCHLIST_UNIVERSE_N", "200"))
WATCHLIST_BATCH_N = int(os.getenv("WATCHLIST_BATCH_N", "20"))
WATCHLIST_REFRESH_SECONDS = int(os.getenv("WATCHLIST_REFRESH_SECONDS", "900"))
WATCHLIST_ROTATE_MODE = os.getenv("WATCHLIST_ROTATE_MODE", "roundrobin").strip().lower()
WATCHLIST_ROTATE_SEED = int(os.getenv("WATCHLIST_ROTATE_SEED", "0"))

WATCHLIST_MIN_TURNOVER_24H = float(os.getenv("WATCHLIST_MIN_TURNOVER_24H", "2000000"))
MIN_VOL_PCT = float(os.getenv("MIN_VOL_PCT", "0.2"))
MAX_VOL_PCT = float(os.getenv("MAX_VOL_PCT", "25.0"))

_watchlist_min_price_raw = os.getenv("WATCHLIST_MIN_PRICE", "").strip()
WATCHLIST_MIN_PRICE = float(_watchlist_min_price_raw) if _watchlist_min_price_raw else None

WATCHLIST_EXCLUDE_PREFIXES = [s.strip().upper() for s in os.getenv("WATCHLIST_EXCLUDE_PREFIXES", "").split(",") if s.strip()]
WATCHLIST_EXCLUDE_SYMBOLS = [s.strip().upper() for s in os.getenv("WATCHLIST_EXCLUDE_SYMBOLS", "").split(",") if s.strip()]
WATCHLIST_EXCLUDE_REGEX = os.getenv("WATCHLIST_EXCLUDE_REGEX", "").strip()
_watchlist_max_spread_bps_raw = os.getenv("WATCHLIST_MAX_SPREAD_BPS", "").strip()
WATCHLIST_MAX_SPREAD_BPS = float(_watchlist_max_spread_bps_raw) if _watchlist_max_spread_bps_raw else None

INTERVAL = os.getenv("INTERVAL", "15")
LOOKBACK = int(os.getenv("LOOKBACK", "300"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "60"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", os.getenv("CHAT_ID", ""))

_ROOT_CONFIG = None


def _root_config():
    global _ROOT_CONFIG
    if _ROOT_CONFIG is None:
        root_config_path = Path(__file__).resolve().parent.parent / "config.py"
        spec = importlib.util.spec_from_file_location("_root_config", root_config_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project root config module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ROOT_CONFIG = module
    return _ROOT_CONFIG


def __getattr__(name: str):
    return getattr(_root_config(), name)
