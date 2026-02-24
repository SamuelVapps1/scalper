import os
import logging
from pathlib import Path
from typing import List

try:
    from dotenv import dotenv_values, find_dotenv, load_dotenv
except Exception:  # pragma: no cover - runtime safety if dependency is unavailable
    dotenv_values = None
    find_dotenv = None
    load_dotenv = None


_ENV_BOOTSTRAP_STATE = {
    "dotenv_path": None,
    "dotenv_loaded": False,
    "dotenv_key_forensics": {},
}
_TELEGRAM_KEYS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")


def _emit_env_debug(debug_data: dict, logger=None) -> None:
    key_forensics = debug_data.get("key_forensics", {})
    token_info = key_forensics.get("TELEGRAM_BOT_TOKEN", {})
    chat_info = key_forensics.get("TELEGRAM_CHAT_ID", {})
    msg = (
        "Env bootstrap: cwd=%s, config_dir=%s, dotenv_path=%s, dotenv_loaded=%s, "
        "TELEGRAM_BOT_TOKEN(env_present=%s, env_nonempty=%s, file_present=%s, file_nonempty=%s), "
        "TELEGRAM_CHAT_ID(env_present=%s, env_nonempty=%s, file_present=%s, file_nonempty=%s)"
    )
    target_logger = logger or logging.getLogger(__name__)
    target_logger.debug(
        msg,
        debug_data["cwd"],
        debug_data["config_dir"],
        debug_data["dotenv_path"],
        debug_data["dotenv_loaded"],
        token_info.get("env_present", False),
        token_info.get("env_nonempty", False),
        token_info.get("file_present", False),
        token_info.get("file_nonempty", False),
        chat_info.get("env_present", False),
        chat_info.get("env_nonempty", False),
        chat_info.get("file_present", False),
        chat_info.get("file_nonempty", False),
    )


def _resolve_env_path():
    env_path_override = os.getenv("ENV_PATH", "").strip()
    if env_path_override:
        resolved_override = Path(env_path_override).expanduser()
        if not resolved_override.is_absolute():
            resolved_override = Path.cwd() / resolved_override
        return str(resolved_override.resolve(strict=False))

    if find_dotenv is None:
        return None

    dotenv_path = find_dotenv(filename=".env", usecwd=True)
    if not dotenv_path:
        dotenv_path = find_dotenv(filename=".env")
    return dotenv_path or None


def _parse_dotenv(dotenv_path):
    if not dotenv_path or dotenv_values is None:
        return {}
    try:
        parsed = dotenv_values(dotenv_path)
    except Exception:  # pragma: no cover - defensive safety for startup path
        return {}
    return dict(parsed or {})


def _collect_key_forensics(parsed_dotenv: dict) -> dict:
    key_forensics = {}
    for key in _TELEGRAM_KEYS:
        env_present = key in os.environ
        env_nonempty = bool(os.getenv(key))
        file_present = key in parsed_dotenv
        file_nonempty = bool(parsed_dotenv.get(key))
        key_forensics[key] = {
            "env_present": env_present,
            "env_nonempty": env_nonempty,
            "file_present": file_present,
            "file_nonempty": file_nonempty,
        }
    return key_forensics


def _apply_telegram_env_fallback(parsed_dotenv: dict, key_forensics: dict) -> None:
    for key in _TELEGRAM_KEYS:
        forensic = key_forensics.get(key, {})
        if (not forensic.get("env_nonempty")) and forensic.get("file_nonempty"):
            os.environ[key] = str(parsed_dotenv.get(key))


def _collect_env_debug_data(dotenv_path, dotenv_loaded: bool, key_forensics: dict) -> dict:
    return {
        "cwd": os.getcwd(),
        "config_dir": str(Path(__file__).resolve().parent),
        "dotenv_path": dotenv_path or "<not found>",
        "dotenv_loaded": bool(dotenv_loaded),
        "key_forensics": key_forensics,
    }


def _bootstrap_env(logger=None):
    dotenv_path = _resolve_env_path()
    parsed_dotenv = _parse_dotenv(dotenv_path)
    key_forensics = _collect_key_forensics(parsed_dotenv)
    dotenv_loaded = False
    if dotenv_path and load_dotenv is not None:
        try:
            dotenv_loaded = bool(load_dotenv(dotenv_path=dotenv_path, override=False))
        except Exception:  # pragma: no cover - defensive safety for startup path
            dotenv_loaded = False

    _apply_telegram_env_fallback(parsed_dotenv, key_forensics)

    _ENV_BOOTSTRAP_STATE["dotenv_path"] = dotenv_path
    _ENV_BOOTSTRAP_STATE["dotenv_loaded"] = dotenv_loaded
    _ENV_BOOTSTRAP_STATE["dotenv_key_forensics"] = key_forensics

    debug_data = _collect_env_debug_data(dotenv_path, dotenv_loaded, key_forensics)
    _emit_env_debug(debug_data, logger=logger)
    return debug_data


def debug_env(logger=None):
    dotenv_path = _ENV_BOOTSTRAP_STATE.get("dotenv_path")
    parsed_dotenv = _parse_dotenv(dotenv_path)
    key_forensics = _collect_key_forensics(parsed_dotenv)
    debug_data = _collect_env_debug_data(
        dotenv_path,
        bool(_ENV_BOOTSTRAP_STATE.get("dotenv_loaded")),
        key_forensics,
    )
    _emit_env_debug(debug_data, logger=logger)
    return debug_data


def _env_bool(name: str, default: bool = False) -> bool:
    """Robust env bool: 1,true,yes,on -> True; 0,false,no,off,empty -> False."""
    raw = os.getenv(name)
    if raw is None:
        return default
    v = str(raw).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_csv(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    if raw is None:
        return []
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def debug_risk_config(logger=None):
    risk_data = {
        "WATCHLIST_MODE": WATCHLIST_MODE,
        "WATCHLIST_TOP_N": WATCHLIST_TOP_N,
        "WATCHLIST_REFRESH_MINUTES": WATCHLIST_REFRESH_MINUTES,
        "WATCHLIST_MIN_PRICE": WATCHLIST_MIN_PRICE,
        "WATCHLIST_MIN_TURNOVER_24H": WATCHLIST_MIN_TURNOVER_24H,
        "WATCHLIST_EXCLUDE_PREFIXES": WATCHLIST_EXCLUDE_PREFIXES,
        "WATCHLIST_EXCLUDE_SYMBOLS": WATCHLIST_EXCLUDE_SYMBOLS,
        "WATCHLIST_EXCLUDE_REGEX": WATCHLIST_EXCLUDE_REGEX,
        "WATCHLIST_MAX_SPREAD_BPS": WATCHLIST_MAX_SPREAD_BPS,
        "MIN_VOL_PCT": MIN_VOL_PCT,
        "MAX_VOL_PCT": MAX_VOL_PCT,
        "POSITION_MODE": POSITION_MODE,
        "MAX_CONCURRENT_POSITIONS": MAX_CONCURRENT_POSITIONS,
        "RISK_KILL_SWITCH": RISK_KILL_SWITCH,
        "RISK_MAX_TRADES_PER_DAY": RISK_MAX_TRADES_PER_DAY,
        "RISK_MAX_DAILY_LOSS_SIM": RISK_MAX_DAILY_LOSS_SIM,
        "RISK_MAX_CONSECUTIVE_LOSSES": RISK_MAX_CONSECUTIVE_LOSSES,
        "RISK_COOLDOWN_MINUTES": RISK_COOLDOWN_MINUTES,
        "RISK_ONE_POSITION_PER_SYMBOL": RISK_ONE_POSITION_PER_SYMBOL,
        "MAX_OPEN_POSITIONS": MAX_OPEN_POSITIONS,
        "RISK_NOTIFY_BLOCKED_TELEGRAM": RISK_NOTIFY_BLOCKED_TELEGRAM,
        "DASHBOARD_TELEGRAM": DASHBOARD_TELEGRAM,
        "DASHBOARD_TOP_N": DASHBOARD_TOP_N,
        "DASHBOARD_INCLUDE_BLOCKED": DASHBOARD_INCLUDE_BLOCKED,
        "DASHBOARD_INCLUDE_MARKET_SNAPSHOT": DASHBOARD_INCLUDE_MARKET_SNAPSHOT,
        "DASHBOARD_INCLUDE_DEBUG_WHY_NONE": DASHBOARD_INCLUDE_DEBUG_WHY_NONE,
        "PAPER_POSITION_NOTIONAL_USDT": PAPER_POSITION_NOTIONAL_USDT,
        "PAPER_POSITION_USDT": PAPER_POSITION_USDT,
        "PAPER_FEES_BPS": PAPER_FEES_BPS,
        "SPREAD_BPS": SPREAD_BPS,
        "SLIPPAGE_BPS": SLIPPAGE_BPS,
        "PAPER_SL_ATR": PAPER_SL_ATR,
        "PAPER_TP_ATR": PAPER_TP_ATR,
        "PAPER_TIMEOUT_BARS": PAPER_TIMEOUT_BARS,
        "PAPER_EQUITY_USDT": PAPER_EQUITY_USDT,
        "RISK_PER_TRADE_PCT": RISK_PER_TRADE_PCT,
        "MAX_NOTIONAL_USDT": MAX_NOTIONAL_USDT,
        "SIGNAL_DEBUG": SIGNAL_DEBUG,
        "RANGE_LOOKBACK_BARS": RANGE_LOOKBACK_BARS,
        "RANGE_EXCLUDE_TAIL": RANGE_EXCLUDE_TAIL,
        "MIN_RANGE_ATR": MIN_RANGE_ATR,
        "RB_RANGE_BARS": RB_RANGE_BARS,
        "RB_MIN_RANGE_ATR": RB_MIN_RANGE_ATR,
        "RB_BREAKOUT_BUFFER_ATR": RB_BREAKOUT_BUFFER_ATR,
        "RB_RETEST_TOL_ATR": RB_RETEST_TOL_ATR,
        "RB_CONFIRM_CLOSE_BUFFER_ATR": RB_CONFIRM_CLOSE_BUFFER_ATR,
        "FB_SWEEP_WICK_ATR": FB_SWEEP_WICK_ATR,
        "FB_CLOSE_BACK_INSIDE_BUFFER_ATR": FB_CLOSE_BACK_INSIDE_BUFFER_ATR,
        "FB_MIN_DIST_FROM_EMA200_PCT": FB_MIN_DIST_FROM_EMA200_PCT,
        "FB_MIN_CONFIDENCE": FB_MIN_CONFIDENCE,
        "DEDUP_BARS": DEDUP_BARS,
        "EARLY_MIN_CONF": EARLY_MIN_CONF,
        "EARLY_ENABLED": EARLY_ENABLED,
        "EARLY_TF": EARLY_TF,
        "EARLY_LOOKBACK_5M": EARLY_LOOKBACK_5M,
        "EARLY_REQUIRE_15M_CONTEXT": EARLY_REQUIRE_15M_CONTEXT,
        "EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M": EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M,
        "THRESHOLD_PROFILE": THRESHOLD_PROFILE,
        "TELEGRAM_FORMAT": TELEGRAM_FORMAT,
        "TELEGRAM_MAX_CHARS_COMPACT": TELEGRAM_MAX_CHARS_COMPACT,
        "TELEGRAM_MAX_CHARS_VERBOSE": TELEGRAM_MAX_CHARS_VERBOSE,
        "TELEGRAM_SEND_BLOCKED": TELEGRAM_SEND_BLOCKED,
        "TELEGRAM_SEND_DASHBOARD": TELEGRAM_SEND_DASHBOARD,
        "NOTIFY_BLOCKED": NOTIFY_BLOCKED,
        "ALWAYS_NOTIFY_INTENTS": ALWAYS_NOTIFY_INTENTS,
        "HEARTBEAT_MINUTES": HEARTBEAT_MINUTES,
        "NOTIFY_SCAN_SUMMARY": NOTIFY_SCAN_SUMMARY,
        "DISABLE_SCAN_SUMMARY": DISABLE_SCAN_SUMMARY,
        "DASHBOARD_HOST": DASHBOARD_HOST,
        "DASHBOARD_PORT": DASHBOARD_PORT,
        "TF_BIAS": TF_BIAS,
        "TF_SETUP": TF_SETUP,
        "TF_TRIGGER": TF_TRIGGER,
        "TF_TIMING": TF_TIMING,
        "LOOKBACK_4H": LOOKBACK_4H,
        "LOOKBACK_1H": LOOKBACK_1H,
        "LOOKBACK_15M": LOOKBACK_15M,
        "LOOKBACK_5M": LOOKBACK_5M,
        "CANDLES_CACHE_TTL_SECONDS": CANDLES_CACHE_TTL_SECONDS,
        "TELEGRAM_POLICY": TELEGRAM_POLICY,
        "SCAN_SUMMARY_MINUTES": SCAN_SUMMARY_MINUTES,
    }
    target_logger = logger or logging.getLogger(__name__)
    target_logger.debug("Risk config: %s", risk_data)
    return risk_data


_bootstrap_env()

BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")

WATCHLIST_RAW = os.getenv("WATCHLIST", "")
WATCHLIST: List[str] = [s.strip().upper() for s in WATCHLIST_RAW.split(",") if s.strip()]
WATCHLIST_MODE = os.getenv("WATCHLIST_MODE", "static").strip().lower()
if WATCHLIST_MODE not in {"static", "topn", "dynamic"}:
    WATCHLIST_MODE = "static"
WATCHLIST_TOP_N = _env_int("WATCHLIST_TOP_N", 10)
if WATCHLIST_TOP_N < 1:
    WATCHLIST_TOP_N = 10
WATCHLIST_REFRESH_MINUTES = _env_int("WATCHLIST_REFRESH_MINUTES", _env_int("WATCHLIST_REFRESH_MIN", 60))
if WATCHLIST_REFRESH_MINUTES < 1:
    WATCHLIST_REFRESH_MINUTES = 60
WATCHLIST_REFRESH_MIN = WATCHLIST_REFRESH_MINUTES
WATCHLIST_MIN_PRICE = _env_float("WATCHLIST_MIN_PRICE", 0.01)
if WATCHLIST_MIN_PRICE < 0:
    WATCHLIST_MIN_PRICE = 0.0
WATCHLIST_MIN_TURNOVER_24H = _env_float("WATCHLIST_MIN_TURNOVER_24H", 100000000.0)
if WATCHLIST_MIN_TURNOVER_24H < 0:
    WATCHLIST_MIN_TURNOVER_24H = 0.0
MIN_TURNOVER_USDT = _env_float("MIN_TURNOVER_USDT", WATCHLIST_MIN_TURNOVER_24H)
WATCHLIST_EXCLUDE_PREFIXES = _env_csv("WATCHLIST_EXCLUDE_PREFIXES", "1000,10000")
WATCHLIST_EXCLUDE_SYMBOLS = [s.upper() for s in _env_csv("WATCHLIST_EXCLUDE_SYMBOLS", "")]
WATCHLIST_EXCLUDE_REGEX = os.getenv("WATCHLIST_EXCLUDE_REGEX", "").strip()
WATCHLIST_MAX_SPREAD_BPS = _env_float("WATCHLIST_MAX_SPREAD_BPS", 0.0)
if WATCHLIST_MAX_SPREAD_BPS < 0:
    WATCHLIST_MAX_SPREAD_BPS = 0.0
MIN_VOL_PCT = _env_float("MIN_VOL_PCT", 0.8)
if MIN_VOL_PCT < 0:
    MIN_VOL_PCT = 0.0
MAX_VOL_PCT = _env_float("MAX_VOL_PCT", 8.0)
if MAX_VOL_PCT <= 0:
    MAX_VOL_PCT = 8.0
WATCHLIST_POOL_N = _env_int("WATCHLIST_POOL_N", 30)
if WATCHLIST_POOL_N < 1:
    WATCHLIST_POOL_N = 30
WATCHLIST_RANK = os.getenv("WATCHLIST_RANK", "turnover").strip().lower()
if WATCHLIST_RANK not in {"turnover", "turnover_vol", "momentum_1h"}:
    WATCHLIST_RANK = "turnover"

INTERVAL = os.getenv("INTERVAL", "15")
LOOKBACK = int(os.getenv("LOOKBACK", "300"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "60"))

# MTF (Multi-Timeframe) pipeline: 4H/1H/15m/5m
TF_BIAS = _env_int("TF_BIAS", 240)
TF_SETUP = _env_int("TF_SETUP", 60)
TF_TRIGGER = _env_int("TF_TRIGGER", 15)
TF_TIMING = _env_int("TF_TIMING", 5)
LOOKBACK_4H = _env_int("LOOKBACK_4H", 300)
LOOKBACK_1H = _env_int("LOOKBACK_1H", 300)
LOOKBACK_15M = _env_int("LOOKBACK_15M", 500)
LOOKBACK_5M = _env_int("LOOKBACK_5M", 800)
CANDLES_CACHE_TTL_SECONDS = _env_int("CANDLES_CACHE_TTL_SECONDS", 20)
if CANDLES_CACHE_TTL_SECONDS < 1:
    CANDLES_CACHE_TTL_SECONDS = 20

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", os.getenv("CHAT_ID", ""))
TELEGRAM_FORMAT = os.getenv("TELEGRAM_FORMAT", "compact").strip().lower()
if TELEGRAM_FORMAT not in {"compact", "verbose"}:
    TELEGRAM_FORMAT = "compact"
TELEGRAM_MAX_CHARS_COMPACT = _env_int("TELEGRAM_MAX_CHARS_COMPACT", 900)
if TELEGRAM_MAX_CHARS_COMPACT < 80:
    TELEGRAM_MAX_CHARS_COMPACT = 900
TELEGRAM_MAX_CHARS_VERBOSE = _env_int("TELEGRAM_MAX_CHARS_VERBOSE", 2500)
if TELEGRAM_MAX_CHARS_VERBOSE < 200:
    TELEGRAM_MAX_CHARS_VERBOSE = 2500
TELEGRAM_SEND_BLOCKED = _env_bool("TELEGRAM_SEND_BLOCKED", False)
TELEGRAM_SEND_DASHBOARD = _env_bool("TELEGRAM_SEND_DASHBOARD", False)
NOTIFY_BLOCKED = _env_bool("NOTIFY_BLOCKED", False)
ALWAYS_NOTIFY_INTENTS = _env_bool("ALWAYS_NOTIFY_INTENTS", False)
HEARTBEAT_MINUTES = _env_int("HEARTBEAT_MINUTES", 10)
if HEARTBEAT_MINUTES < 1:
    HEARTBEAT_MINUTES = 10
NOTIFY_SCAN_SUMMARY = _env_bool("NOTIFY_SCAN_SUMMARY", False)
DISABLE_SCAN_SUMMARY = _env_bool("DISABLE_SCAN_SUMMARY", True)
TELEGRAM_POLICY = str(os.getenv("TELEGRAM_POLICY", "events")).strip().lower()
if TELEGRAM_POLICY not in {"events", "periodic", "off"}:
    TELEGRAM_POLICY = "events"
SCAN_SUMMARY_MINUTES = _env_int("SCAN_SUMMARY_MINUTES", 30)
if SCAN_SUMMARY_MINUTES < 1:
    SCAN_SUMMARY_MINUTES = 30

POSITION_MODE = str(os.getenv("POSITION_MODE", "global") or "global").strip().lower()
try:
    MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "1"))
except (TypeError, ValueError):
    MAX_CONCURRENT_POSITIONS = 1
if MAX_CONCURRENT_POSITIONS < 0:
    MAX_CONCURRENT_POSITIONS = 0

RISK_KILL_SWITCH = _env_bool("RISK_KILL_SWITCH", False)
RISK_MAX_TRADES_PER_DAY = _env_int("RISK_MAX_TRADES_PER_DAY", 10)
RISK_MAX_DAILY_LOSS_SIM = _env_float("RISK_MAX_DAILY_LOSS_SIM", 100.0)
RISK_MAX_CONSECUTIVE_LOSSES = _env_int("RISK_MAX_CONSECUTIVE_LOSSES", 3)
RISK_COOLDOWN_MINUTES = _env_int("RISK_COOLDOWN_MINUTES", 30)
RISK_ONE_POSITION_PER_SYMBOL = _env_bool("RISK_ONE_POSITION_PER_SYMBOL", True)
# Backward compatibility:
# - If MAX_OPEN_POSITIONS is not set, keep legacy RISK_ONE_POSITION_ONLY behavior.
# - RISK_ONE_POSITION_ONLY=True -> 1, False -> 0 (disabled limit).
_LEGACY_ONE_POSITION_ONLY = _env_bool("RISK_ONE_POSITION_ONLY", True)
MAX_OPEN_POSITIONS = _env_int(
    "MAX_OPEN_POSITIONS",
    1 if _LEGACY_ONE_POSITION_ONLY else 0,
)
if MAX_OPEN_POSITIONS < 0:
    MAX_OPEN_POSITIONS = 0
RISK_NOTIFY_BLOCKED_TELEGRAM = _env_bool("RISK_NOTIFY_BLOCKED_TELEGRAM", False)

DASHBOARD_TELEGRAM = _env_bool("DASHBOARD_TELEGRAM", False)
DASHBOARD_TOP_N = _env_int("DASHBOARD_TOP_N", 3)
if DASHBOARD_TOP_N < 1:
    DASHBOARD_TOP_N = 1
DASHBOARD_INCLUDE_BLOCKED = _env_bool("DASHBOARD_INCLUDE_BLOCKED", True)
DASHBOARD_INCLUDE_MARKET_SNAPSHOT = _env_bool("DASHBOARD_INCLUDE_MARKET_SNAPSHOT", True)
DASHBOARD_INCLUDE_DEBUG_WHY_NONE = _env_bool(
    "DASHBOARD_INCLUDE_DEBUG_WHY_NONE",
    _env_bool("SIGNAL_DEBUG", False),
)

PAPER_POSITION_NOTIONAL_USDT = _env_float("PAPER_POSITION_NOTIONAL_USDT", 50.0)
if PAPER_POSITION_NOTIONAL_USDT < 0:
    PAPER_POSITION_NOTIONAL_USDT = 0.0
PAPER_POSITION_USDT = _env_float("PAPER_POSITION_USDT", 20.0)
if PAPER_POSITION_USDT < 0:
    PAPER_POSITION_USDT = 0.0
PAPER_FEES_BPS = _env_float("PAPER_FEES_BPS", 6.0)
if PAPER_FEES_BPS < 0:
    PAPER_FEES_BPS = 0.0
SPREAD_BPS = _env_float("SPREAD_BPS", 2.0)
if SPREAD_BPS < 0:
    SPREAD_BPS = 0.0
SLIPPAGE_BPS = _env_float("SLIPPAGE_BPS", 3.0)
if SLIPPAGE_BPS < 0:
    SLIPPAGE_BPS = 0.0
PAPER_SL_ATR = _env_float("PAPER_SL_ATR", 1.0)
if PAPER_SL_ATR <= 0:
    PAPER_SL_ATR = 1.0
PAPER_TP_ATR = _env_float("PAPER_TP_ATR", 1.5)
if PAPER_TP_ATR <= 0:
    PAPER_TP_ATR = 1.5
PAPER_TIMEOUT_BARS = _env_int("PAPER_TIMEOUT_BARS", 12)
if PAPER_TIMEOUT_BARS < 1:
    PAPER_TIMEOUT_BARS = 12
PAPER_EQUITY_USDT = _env_float("PAPER_EQUITY_USDT", 200.0)
if PAPER_EQUITY_USDT < 0:
    PAPER_EQUITY_USDT = 0.0
RISK_PER_TRADE_PCT = _env_float("RISK_PER_TRADE_PCT", 0.25)
if RISK_PER_TRADE_PCT < 0:
    RISK_PER_TRADE_PCT = 0.0
MAX_NOTIONAL_USDT = _env_float("MAX_NOTIONAL_USDT", 50.0)
if MAX_NOTIONAL_USDT < 0:
    MAX_NOTIONAL_USDT = 0.0

SIGNAL_DEBUG = _env_bool("SIGNAL_DEBUG", False)

RANGE_LOOKBACK_BARS = _env_int("RANGE_LOOKBACK_BARS", 80)
if RANGE_LOOKBACK_BARS < 10:
    RANGE_LOOKBACK_BARS = 80
RANGE_EXCLUDE_TAIL = _env_int("RANGE_EXCLUDE_TAIL", 2)
if RANGE_EXCLUDE_TAIL < 0:
    RANGE_EXCLUDE_TAIL = 2
MIN_RANGE_ATR = _env_float("MIN_RANGE_ATR", 2.0)
if MIN_RANGE_ATR < 0:
    MIN_RANGE_ATR = 2.0

RB_RANGE_BARS = _env_int("RB_RANGE_BARS", 24)
if RB_RANGE_BARS < 5:
    RB_RANGE_BARS = 24
RB_MIN_RANGE_ATR = _env_float("RB_MIN_RANGE_ATR", 2.0)
if RB_MIN_RANGE_ATR < 0:
    RB_MIN_RANGE_ATR = 2.0
RB_BREAKOUT_BUFFER_ATR = _env_float("RB_BREAKOUT_BUFFER_ATR", 0.10)
if RB_BREAKOUT_BUFFER_ATR < 0:
    RB_BREAKOUT_BUFFER_ATR = 0.10
RB_RETEST_TOL_ATR = _env_float("RB_RETEST_TOL_ATR", 0.15)
if RB_RETEST_TOL_ATR < 0:
    RB_RETEST_TOL_ATR = 0.15
RB_CONFIRM_CLOSE_BUFFER_ATR = _env_float("RB_CONFIRM_CLOSE_BUFFER_ATR", 0.05)
if RB_CONFIRM_CLOSE_BUFFER_ATR < 0:
    RB_CONFIRM_CLOSE_BUFFER_ATR = 0.05

FB_SWEEP_WICK_ATR = _env_float("FB_SWEEP_WICK_ATR", 0.10)
if FB_SWEEP_WICK_ATR < 0:
    FB_SWEEP_WICK_ATR = 0.10
FB_CLOSE_BACK_INSIDE_BUFFER_ATR = _env_float("FB_CLOSE_BACK_INSIDE_BUFFER_ATR", 0.02)
if FB_CLOSE_BACK_INSIDE_BUFFER_ATR < 0:
    FB_CLOSE_BACK_INSIDE_BUFFER_ATR = 0.02
FB_MIN_DIST_FROM_EMA200_PCT = _env_float("FB_MIN_DIST_FROM_EMA200_PCT", 0.2)
if FB_MIN_DIST_FROM_EMA200_PCT < 0:
    FB_MIN_DIST_FROM_EMA200_PCT = 0.2
FB_MIN_CONFIDENCE = _env_float("FB_MIN_CONFIDENCE", 0.60)
if FB_MIN_CONFIDENCE < 0:
    FB_MIN_CONFIDENCE = 0.60

DEDUP_BARS = _env_int("DEDUP_BARS", 2)
if DEDUP_BARS < 1:
    DEDUP_BARS = 2

EARLY_MIN_CONF = _env_float("EARLY_MIN_CONF", 0.35)
if EARLY_MIN_CONF < 0:
    EARLY_MIN_CONF = 0.35
EARLY_ENABLED = _env_bool("EARLY_ENABLED", True)
EARLY_TF = os.getenv("EARLY_TF", "5").strip() or "5"
EARLY_LOOKBACK_5M = _env_int("EARLY_LOOKBACK_5M", 180)
if EARLY_LOOKBACK_5M < 30:
    EARLY_LOOKBACK_5M = 180
EARLY_REQUIRE_15M_CONTEXT = _env_bool("EARLY_REQUIRE_15M_CONTEXT", True)
EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M = _env_int("EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M", 1)
if EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M < 1:
    EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M = 1
THRESHOLD_PROFILE = os.getenv("THRESHOLD_PROFILE", "A").strip().upper()
if THRESHOLD_PROFILE not in {"A", "B", "C"}:
    THRESHOLD_PROFILE = "A"
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1").strip() or "127.0.0.1"
DASHBOARD_PORT = _env_int("DASHBOARD_PORT", 8000)
if DASHBOARD_PORT < 1 or DASHBOARD_PORT > 65535:
    DASHBOARD_PORT = 8000
