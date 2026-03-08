from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, List, Optional

try:
    from pydantic.v1 import BaseModel, BaseSettings, Field, root_validator
except ImportError:  # pragma: no cover
    from pydantic import BaseModel, BaseSettings, Field, root_validator  # type: ignore[assignment]

try:
    from dotenv import dotenv_values, find_dotenv, load_dotenv
except Exception:  # pragma: no cover
    dotenv_values = None
    find_dotenv = None
    load_dotenv = None


# ----- Env string sanitization (before Pydantic parses) -----

def _strip_inline_comment(value: Any) -> str:
    """Remove inline comment after # or ; if not inside quotes. Trim whitespace."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # Simple: strip from first unquoted # or ;
    out = []
    i = 0
    quote = None
    while i < len(s):
        c = s[i]
        if quote:
            if c == quote and (i + 1 >= len(s) or s[i + 1] != quote):
                quote = None
            out.append(c)
            i += 1
            continue
        if c in ("'", '"'):
            quote = c
            out.append(c)
            i += 1
            continue
        if c == "#" or (c == ";" and i > 0):
            break
        out.append(c)
        i += 1
    return "".join(out).strip()


def _normalize_empty(value: Any) -> Optional[str]:
    """Treat '', 'none', 'null', 'None' as empty (return None). Otherwise return stripped string."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in ("none", "null"):
        return None
    return s


def _coerce_int(value: Any, default: int) -> int:
    """Coerce to int; empty/None/invalid => default."""
    s = _normalize_empty(value)
    if s is None:
        return default
    s = _strip_inline_comment(s)
    if not s:
        return default
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    """Coerce to float; empty/None/invalid => default."""
    s = _normalize_empty(value)
    if s is None:
        return default
    s = _strip_inline_comment(s)
    if not s:
        return default
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any, default: bool) -> bool:
    """Coerce to bool; empty/None => default; '1'/'true'/'yes'/'on' => True; else False."""
    s = _normalize_empty(value)
    if s is None:
        return default
    s = _strip_inline_comment(s).lower()
    if not s:
        return default
    return s in ("1", "true", "yes", "on")


def _coerce_csv_list(value: Any) -> List[str]:
    """Parse CSV string to list of stripped non-empty strings. Empty => []."""
    s = _normalize_empty(value)
    if s is None:
        return []
    s = _strip_inline_comment(s)
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


_ENV_BOOTSTRAP_STATE = {
    "dotenv_path": None,
    "dotenv_loaded": False,
    "dotenv_key_forensics": {},
}
_TELEGRAM_KEYS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")


def _resolve_env_path() -> str | None:
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


def _parse_dotenv(dotenv_path: str | None) -> dict:
    if not dotenv_path or dotenv_values is None:
        return {}
    try:
        parsed = dotenv_values(dotenv_path)
    except Exception:  # pragma: no cover
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


def _bootstrap_env(logger=None):
    dotenv_path = _resolve_env_path()
    parsed_dotenv = _parse_dotenv(dotenv_path)
    key_forensics = _collect_key_forensics(parsed_dotenv)
    dotenv_loaded = False
    if dotenv_path and load_dotenv is not None:
        try:
            dotenv_loaded = bool(load_dotenv(dotenv_path=dotenv_path, override=False))
        except Exception:  # pragma: no cover
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


class BybitSettings(BaseSettings):
    base_url: str = Field("https://api.bybit.com", env="BYBIT_BASE_URL")
    request_sleep_ms: int = Field(250, env="REQUEST_SLEEP_MS")
    execution_mode: str = Field("disabled", env="EXECUTION_MODE")
    explicit_confirm_execution: bool = Field(False, env="EXPLICIT_CONFIRM_EXECUTION")

    @root_validator(pre=True)
    def _coerce_bybit_env(cls, values):
        if values.get("request_sleep_ms") is not None and isinstance(values["request_sleep_ms"], str):
            values["request_sleep_ms"] = _coerce_int(values["request_sleep_ms"], 250)
        if values.get("explicit_confirm_execution") is not None and isinstance(values["explicit_confirm_execution"], str):
            values["explicit_confirm_execution"] = _coerce_bool(values["explicit_confirm_execution"], False)
        return values

    @root_validator(pre=False)
    def _normalize(cls, values):
        values["base_url"] = str(values.get("base_url") or "https://api.bybit.com").rstrip("/")
        values["request_sleep_ms"] = max(0, int(values.get("request_sleep_ms", 250)))
        mode = str(values.get("execution_mode") or "disabled").lower()
        values["execution_mode"] = mode if mode in ("disabled", "testnet", "live") else "disabled"
        return values


class TelegramSettings(BaseSettings):
    bot_token: str = Field("", env="TELEGRAM_BOT_TOKEN")
    chat_id: str = Field("", env="TELEGRAM_CHAT_ID")
    format: str = Field("compact", env="TELEGRAM_FORMAT")
    max_chars_compact: int = Field(900, env="TELEGRAM_MAX_CHARS_COMPACT")
    max_chars_verbose: int = Field(2500, env="TELEGRAM_MAX_CHARS_VERBOSE")
    send_blocked: bool = Field(False, env="TELEGRAM_SEND_BLOCKED")
    send_dashboard: bool = Field(False, env="TELEGRAM_SEND_DASHBOARD")
    policy: str = Field("events", env="TELEGRAM_POLICY")
    compact: bool = Field(True, env="TELEGRAM_COMPACT")
    early_enabled: bool = Field(False, env="TELEGRAM_EARLY_ENABLED")
    early_max_per_symbol_per_15m: int = Field(1, env="TELEGRAM_EARLY_MAX_PER_SYMBOL_PER_15M")

    @root_validator(pre=True)
    def _coerce_telegram_env(cls, values):
        if values.get("max_chars_compact") is not None and isinstance(values["max_chars_compact"], str):
            values["max_chars_compact"] = _coerce_int(values["max_chars_compact"], 900)
        if values.get("max_chars_verbose") is not None and isinstance(values["max_chars_verbose"], str):
            values["max_chars_verbose"] = _coerce_int(values["max_chars_verbose"], 2500)
        bool_defaults_telegram = {"send_blocked": False, "send_dashboard": False, "compact": True, "early_enabled": False}
        for k, default in bool_defaults_telegram.items():
            if values.get(k) is not None and isinstance(values[k], str):
                values[k] = _coerce_bool(values[k], default)
        if values.get("early_max_per_symbol_per_15m") is not None and isinstance(values["early_max_per_symbol_per_15m"], str):
            values["early_max_per_symbol_per_15m"] = _coerce_int(values["early_max_per_symbol_per_15m"], 1)
        return values

    @root_validator(pre=True)
    def _chat_fallback(cls, values):
        if not values.get("chat_id"):
            values["chat_id"] = os.getenv("CHAT_ID", "")
        return values

    @root_validator(pre=False)
    def _normalize(cls, values):
        fmt = str(values.get("format") or "compact").lower()
        values["format"] = fmt if fmt in {"compact", "verbose"} else "compact"
        values["max_chars_compact"] = 900 if int(values.get("max_chars_compact", 900)) < 80 else int(values["max_chars_compact"])
        values["max_chars_verbose"] = 2500 if int(values.get("max_chars_verbose", 2500)) < 200 else int(values["max_chars_verbose"])
        policy = str(values.get("policy") or "events").lower()
        values["policy"] = policy if policy in {"off", "signals", "events", "both"} else "events"
        values["early_max_per_symbol_per_15m"] = max(1, int(values.get("early_max_per_symbol_per_15m", 1)))
        return values


class RiskSettings(BaseSettings):
    interval: str = Field("15", env="INTERVAL")
    lookback: int = Field(300, env="LOOKBACK")
    scan_seconds: int = Field(60, env="SCAN_SECONDS")
    scan_cycle_timeout_seconds: int = Field(0, env="SCAN_CYCLE_TIMEOUT_SECONDS")
    watchlist_raw: str = Field("", env="WATCHLIST")
    watchlist_mode: str = Field("static", env="WATCHLIST_MODE")
    watchlist_universe_n: int = Field(200, env="WATCHLIST_UNIVERSE_N")
    watchlist_batch_n: int = Field(20, env="WATCHLIST_BATCH_N")
    watchlist_refresh_seconds: int = Field(900, env="WATCHLIST_REFRESH_SECONDS")
    watchlist_rotate_mode: str = Field("roundrobin", env="WATCHLIST_ROTATE_MODE")
    watchlist_rotate_seed: int = Field(0, env="WATCHLIST_ROTATE_SEED")
    rotation_state_file: str = Field("state.json", env="ROTATION_STATE_FILE")
    watchlist_top_n: int = Field(10, env="WATCHLIST_TOP_N")
    watchlist_refresh_minutes: int = Field(60, env="WATCHLIST_REFRESH_MINUTES")
    watchlist_min_price: float = Field(0.01, env="WATCHLIST_MIN_PRICE")
    watchlist_min_turnover_24h: float = Field(100000000.0, env="WATCHLIST_MIN_TURNOVER_24H")
    watchlist_exclude_prefixes_raw: str = Field("1000,10000", env="WATCHLIST_EXCLUDE_PREFIXES")
    watchlist_exclude_symbols_raw: str = Field("", env="WATCHLIST_EXCLUDE_SYMBOLS")
    watchlist_exclude_regex: str = Field("", env="WATCHLIST_EXCLUDE_REGEX")
    watchlist_max_spread_bps: float = Field(0.0, env="WATCHLIST_MAX_SPREAD_BPS")
    min_turnover_usdt: float = Field(100000000.0, env="MIN_TURNOVER_USDT")
    min_vol_pct: float = Field(0.8, env="MIN_VOL_PCT")
    max_vol_pct: float = Field(8.0, env="MAX_VOL_PCT")
    watchlist_pool_n: int = Field(30, env="WATCHLIST_POOL_N")
    watchlist_rank: str = Field("turnover", env="WATCHLIST_RANK")
    position_mode: str = Field("global", env="POSITION_MODE")
    max_concurrent_positions: int = Field(1, env="MAX_CONCURRENT_POSITIONS")
    max_open_positions: int = Field(1, env="MAX_OPEN_POSITIONS")
    risk_notify_blocked_telegram: bool = Field(False, env="RISK_NOTIFY_BLOCKED_TELEGRAM")
    risk_kill_switch: bool = Field(False, env="RISK_KILL_SWITCH")
    kill_switch: bool = Field(False, env="KILL_SWITCH")
    risk_max_trades_per_day: int = Field(10, env="RISK_MAX_TRADES_PER_DAY")
    risk_max_daily_loss_sim: float = Field(100.0, env="RISK_MAX_DAILY_LOSS_SIM")
    risk_max_consecutive_losses: int = Field(3, env="RISK_MAX_CONSECUTIVE_LOSSES")
    risk_cooldown_minutes: int = Field(30, env="RISK_COOLDOWN_MINUTES")
    risk_one_position_per_symbol: bool = Field(True, env="RISK_ONE_POSITION_PER_SYMBOL")
    signal_debug: bool = Field(False, env="SIGNAL_DEBUG")
    kpi_debug: bool = Field(False, env="KPI_DEBUG")
    notify_blocked: bool = Field(False, env="NOTIFY_BLOCKED")
    always_notify_intents: bool = Field(False, env="ALWAYS_NOTIFY_INTENTS")
    heartbeat_minutes: int = Field(15, env="HEARTBEAT_MINUTES")
    notify_scan_summary: bool = Field(False, env="NOTIFY_SCAN_SUMMARY")
    disable_scan_summary: bool = Field(True, env="DISABLE_SCAN_SUMMARY")
    threshold_profile: str = Field("A", env="THRESHOLD_PROFILE")
    early_enabled: bool = Field(True, env="EARLY_ENABLED")
    early_tf: str = Field("5", env="EARLY_TF")
    early_lookback_5m: int = Field(180, env="EARLY_LOOKBACK_5M")
    early_min_conf: float = Field(0.35, env="EARLY_MIN_CONF")
    early_require_15m_context: bool = Field(True, env="EARLY_REQUIRE_15M_CONTEXT")
    early_max_alerts_per_symbol_per_15m: int = Field(1, env="EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M")
    tf_bias: int = Field(240, env="TF_BIAS")
    tf_setup: int = Field(60, env="TF_SETUP")
    tf_trigger: int = Field(15, env="TF_TRIGGER")
    tf_timing: int = Field(5, env="TF_TIMING")
    lookback_4h: int = Field(250, env="LOOKBACK_4H")
    lookback_1h: int = Field(250, env="LOOKBACK_1H")
    lookback_15m: int = Field(400, env="LOOKBACK_15M")
    lookback_5m: int = Field(400, env="LOOKBACK_5M")
    paper_position_usdt: float = Field(20.0, env="PAPER_POSITION_USDT")
    paper_fees_bps: float = Field(6.0, env="PAPER_FEES_BPS")
    paper_equity_usdt: float = Field(200.0, env="PAPER_EQUITY_USDT")
    paper_timeout_bars: int = Field(12, env="PAPER_TIMEOUT_BARS")
    paper_sl_atr: float = Field(1.0, env="PAPER_SL_ATR")
    paper_tp_atr: float = Field(1.5, env="PAPER_TP_ATR")
    paper_start_equity_usdt: float = Field(1000.0, env="PAPER_START_EQUITY_USDT")
    paper_slippage_pct: float = Field(0.01, env="PAPER_SLIPPAGE_PCT")
    paper_fee_pct: float = Field(0.055, env="PAPER_FEE_PCT")
    spread_bps: float = Field(2.0, env="SPREAD_BPS")
    slippage_bps: float = Field(3.0, env="SLIPPAGE_BPS")
    risk_per_trade_pct: float = Field(0.15, env="RISK_PER_TRADE_PCT")
    daily_loss_limit_pct: float = Field(1.0, env="DAILY_LOSS_LIMIT_PCT")
    max_dd_pct: float = Field(12.0, env="MAX_DD_PCT")
    max_trades_day: int = Field(12, env="MAX_TRADES_DAY")
    min_seconds_between_trades: int = Field(180, env="MIN_SECONDS_BETWEEN_TRADES")
    min_seconds_between_symbol_trades: int = Field(900, env="MIN_SECONDS_BETWEEN_SYMBOL_TRADES")
    max_symbol_notional_pct: float = Field(30.0, env="MAX_SYMBOL_NOTIONAL_PCT")
    cluster_btc_eth_limit: int = Field(1, env="CLUSTER_BTC_ETH_LIMIT")
    fail_closed_on_snapshot_missing: bool = Field(True, env="FAIL_CLOSED_ON_SNAPSHOT_MISSING")

    @root_validator(pre=True)
    def _coerce_risk_env(cls, values):
        """Coerce raw env strings before Pydantic parses. Handles empty, inline comments, null/none."""
        int_defaults = {
            "lookback": 300, "scan_seconds": 60, "scan_cycle_timeout_seconds": 0,
            "watchlist_universe_n": 200, "watchlist_batch_n": 20, "watchlist_refresh_seconds": 900,
            "watchlist_rotate_seed": 0, "watchlist_top_n": 10, "watchlist_refresh_minutes": 60,
            "watchlist_pool_n": 30, "max_concurrent_positions": 1, "max_open_positions": 1,
            "risk_max_trades_per_day": 10, "risk_max_consecutive_losses": 3, "risk_cooldown_minutes": 30,
            "heartbeat_minutes": 15, "early_lookback_5m": 180, "early_max_alerts_per_symbol_per_15m": 1,
            "tf_bias": 240, "tf_setup": 60, "tf_trigger": 15, "tf_timing": 5,
            "lookback_4h": 250, "lookback_1h": 250, "lookback_15m": 400, "lookback_5m": 400,
            "paper_timeout_bars": 12, "max_trades_day": 12,
            "min_seconds_between_trades": 180, "min_seconds_between_symbol_trades": 900,
            "cluster_btc_eth_limit": 1,
        }
        float_defaults = {
            "watchlist_min_price": 0.01, "watchlist_min_turnover_24h": 100000000.0,
            "watchlist_max_spread_bps": 0.0, "min_turnover_usdt": 100000000.0,
            "min_vol_pct": 0.8, "max_vol_pct": 8.0,
            "risk_max_daily_loss_sim": 100.0, "early_min_conf": 0.35,
            "paper_position_usdt": 20.0, "paper_fees_bps": 6.0, "paper_equity_usdt": 200.0,
            "paper_sl_atr": 1.0, "paper_tp_atr": 1.5, "paper_start_equity_usdt": 1000.0,
            "paper_slippage_pct": 0.01, "paper_fee_pct": 0.055,
            "spread_bps": 2.0, "slippage_bps": 3.0, "risk_per_trade_pct": 0.15,
            "daily_loss_limit_pct": 1.0, "max_dd_pct": 12.0, "max_symbol_notional_pct": 30.0,
        }
        str_defaults = {
            "interval": "15", "watchlist_mode": "static", "watchlist_raw": "",
            "watchlist_rotate_mode": "roundrobin", "rotation_state_file": "state.json",
            "watchlist_exclude_prefixes_raw": "1000,10000", "watchlist_exclude_symbols_raw": "",
            "watchlist_exclude_regex": "", "watchlist_rank": "turnover", "position_mode": "global",
            "threshold_profile": "A", "early_tf": "5",
        }
        bool_defaults = {
            "risk_notify_blocked_telegram": False, "risk_kill_switch": False, "kill_switch": False,
            "risk_one_position_per_symbol": True, "signal_debug": False, "kpi_debug": False,
            "notify_blocked": False, "always_notify_intents": False, "notify_scan_summary": False,
            "disable_scan_summary": True, "early_enabled": True, "early_require_15m_context": True,
            "fail_closed_on_snapshot_missing": True,
        }
        for key, default in int_defaults.items():
            if key in values and (values[key] is None or (isinstance(values[key], str) and not values[key].strip())):
                values[key] = default
            elif key in values and isinstance(values[key], str):
                values[key] = _coerce_int(values[key], default)
        for key, default in float_defaults.items():
            if key in values and (values[key] is None or (isinstance(values[key], str) and not values[key].strip())):
                values[key] = default
            elif key in values and isinstance(values[key], str):
                values[key] = _coerce_float(values[key], default)
        for key, default in str_defaults.items():
            if key in values and values[key] is not None and isinstance(values[key], str):
                s = _strip_inline_comment(values[key])
                if _normalize_empty(s) is None:
                    values[key] = default
                else:
                    values[key] = s.strip() if s else default
        for key, default in bool_defaults.items():
            if key in values and (values[key] is None or (isinstance(values[key], str) and not str(values[key]).strip())):
                values[key] = default
            elif key in values and isinstance(values[key], str):
                values[key] = _coerce_bool(values[key], default)
        return values

    @root_validator(pre=True)
    def _legacy_refresh_alias(cls, values):
        # WATCHLIST_RAW fallback when WATCHLIST is empty
        raw_watch = values.get("watchlist_raw") or ""
        if (not raw_watch or not str(raw_watch).strip()) and os.getenv("WATCHLIST_RAW"):
            values["watchlist_raw"] = _strip_inline_comment(os.getenv("WATCHLIST_RAW", "") or "")
        # Legacy env aliases (only if canonical env not set)
        if "WATCHLIST_REFRESH_SECONDS" not in os.environ or not str(os.getenv("WATCHLIST_REFRESH_SECONDS", "")).strip():
            raw_min = os.getenv("WATCHLIST_REFRESH_MIN")
            if raw_min is not None and str(raw_min).strip():
                values["watchlist_refresh_minutes"] = _coerce_int(raw_min, 60)
            if "WATCHLIST_REFRESH_MINUTES" in os.environ:
                values["watchlist_refresh_seconds"] = _coerce_int(os.getenv("WATCHLIST_REFRESH_MINUTES", "15"), 15) * 60
        if ("WATCHLIST_MIN_TURNOVER_24H" not in os.environ or not str(os.getenv("WATCHLIST_MIN_TURNOVER_24H", "")).strip()) and os.getenv("MIN_24H_TURNOVER") is not None:
            values["watchlist_min_turnover_24h"] = _coerce_float(os.getenv("MIN_24H_TURNOVER", "0"), 0.0)
        if ("WATCHLIST_UNIVERSE_N" not in os.environ or not str(os.getenv("WATCHLIST_UNIVERSE_N", "")).strip()) and os.getenv("UNIVERSE_SIZE") is not None:
            values["watchlist_universe_n"] = _coerce_int(os.getenv("UNIVERSE_SIZE", "200"), 200)
        if ("WATCHLIST_BATCH_N" not in os.environ or not str(os.getenv("WATCHLIST_BATCH_N", "")).strip()) and os.getenv("BATCH_SIZE") is not None:
            values["watchlist_batch_n"] = _coerce_int(os.getenv("BATCH_SIZE", "20"), 20)
        if not values.get("max_open_positions") and "MAX_OPEN_POSITIONS" not in os.environ:
            legacy = os.getenv("RISK_ONE_POSITION_ONLY")
            if legacy is not None:
                values["max_open_positions"] = 1 if _coerce_bool(legacy, False) else 0
        return values

    @root_validator(pre=False)
    def _normalize(cls, values):
        mode = str(values.get("watchlist_mode") or "static").lower()
        values["watchlist_mode"] = mode if mode in {"static", "topn", "dynamic", "market"} else "static"
        values["watchlist_universe_n"] = max(1, int(values.get("watchlist_universe_n", 200)))
        values["watchlist_batch_n"] = max(1, int(values.get("watchlist_batch_n", 20)))
        values["watchlist_refresh_seconds"] = max(1, int(values.get("watchlist_refresh_seconds", 600)))
        rotate_mode = str(values.get("watchlist_rotate_mode") or "roundrobin").strip().lower()
        values["watchlist_rotate_mode"] = (
            rotate_mode if rotate_mode in {"roundrobin", "seeded_random"} else "roundrobin"
        )
        values["watchlist_rotate_seed"] = int(values.get("watchlist_rotate_seed", 0) or 0)
        values["rotation_state_file"] = str(
            values.get("rotation_state_file") or "state.json"
        ).strip()
        values["watchlist_top_n"] = max(1, int(values.get("watchlist_top_n", 10)))
        values["watchlist_refresh_minutes"] = max(1, int(values.get("watchlist_refresh_minutes", 60)))
        values["watchlist_min_price"] = max(0.0, float(values.get("watchlist_min_price", 0.01)))
        values["watchlist_min_turnover_24h"] = max(0.0, float(values.get("watchlist_min_turnover_24h", 100000000.0)))
        values["watchlist_max_spread_bps"] = max(0.0, float(values.get("watchlist_max_spread_bps", 0.0)))
        values["min_vol_pct"] = max(0.0, float(values.get("min_vol_pct", 0.8)))
        values["max_vol_pct"] = 8.0 if float(values.get("max_vol_pct", 8.0)) <= 0 else float(values.get("max_vol_pct", 8.0))
        values["watchlist_pool_n"] = max(1, int(values.get("watchlist_pool_n", 30)))
        rank = str(values.get("watchlist_rank") or "turnover").lower()
        values["watchlist_rank"] = rank if rank in {"turnover", "turnover_vol", "momentum_1h"} else "turnover"
        values["max_concurrent_positions"] = max(0, int(values.get("max_concurrent_positions", 1)))
        values["max_open_positions"] = max(0, int(values.get("max_open_positions", 1)))
        values["kill_switch"] = bool(values.get("kill_switch", values.get("risk_kill_switch", False)))
        values["heartbeat_minutes"] = max(1, int(values.get("heartbeat_minutes", 15)))
        values["threshold_profile"] = str(values.get("threshold_profile") or "A").upper()
        if values["threshold_profile"] not in {"A", "B", "C"}:
            values["threshold_profile"] = "A"
        values["early_lookback_5m"] = max(30, int(values.get("early_lookback_5m", 180)))
        values["early_max_alerts_per_symbol_per_15m"] = max(1, int(values.get("early_max_alerts_per_symbol_per_15m", 1)))
        values["paper_position_usdt"] = max(0.0, float(values.get("paper_position_usdt", 20.0)))
        values["paper_fees_bps"] = max(0.0, float(values.get("paper_fees_bps", 6.0)))
        values["paper_equity_usdt"] = max(0.0, float(values.get("paper_equity_usdt", 200.0)))
        values["paper_timeout_bars"] = max(1, int(values.get("paper_timeout_bars", 12)))
        values["paper_sl_atr"] = 1.0 if float(values.get("paper_sl_atr", 1.0)) <= 0 else float(values["paper_sl_atr"])
        values["paper_tp_atr"] = 1.5 if float(values.get("paper_tp_atr", 1.5)) <= 0 else float(values["paper_tp_atr"])
        values["paper_start_equity_usdt"] = max(0.0, float(values.get("paper_start_equity_usdt", 1000.0)))
        values["paper_slippage_pct"] = max(0.0, float(values.get("paper_slippage_pct", 0.01)))
        values["paper_fee_pct"] = max(0.0, float(values.get("paper_fee_pct", 0.055)))
        values["spread_bps"] = max(0.0, float(values.get("spread_bps", 2.0)))
        values["slippage_bps"] = max(0.0, float(values.get("slippage_bps", 3.0)))
        values["risk_per_trade_pct"] = max(0.0, float(values.get("risk_per_trade_pct", 0.15)))
        values["daily_loss_limit_pct"] = max(0.0, float(values.get("daily_loss_limit_pct", 1.0)))
        values["max_dd_pct"] = max(0.0, float(values.get("max_dd_pct", 12.0)))
        values["max_trades_day"] = max(0, int(values.get("max_trades_day", 12)))
        values["min_seconds_between_trades"] = max(0, int(values.get("min_seconds_between_trades", 180)))
        values["min_seconds_between_symbol_trades"] = max(
            0, int(values.get("min_seconds_between_symbol_trades", 900))
        )
        values["max_symbol_notional_pct"] = max(0.0, float(values.get("max_symbol_notional_pct", 30.0)))
        values["cluster_btc_eth_limit"] = max(0, int(values.get("cluster_btc_eth_limit", 1)))
        return values

    @property
    def watchlist(self) -> List[str]:
        return [s.strip().upper() for s in str(self.watchlist_raw or "").split(",") if s.strip()]

    @property
    def watchlist_refresh_min(self) -> int:
        return int(self.watchlist_refresh_minutes)

    @property
    def watchlist_exclude_prefixes(self) -> List[str]:
        return [item.strip() for item in str(self.watchlist_exclude_prefixes_raw or "").split(",") if item.strip()]

    @property
    def watchlist_exclude_symbols(self) -> List[str]:
        return [item.strip().upper() for item in str(self.watchlist_exclude_symbols_raw or "").split(",") if item.strip()]


class StrategyV3Settings(BaseSettings):
    strategy_v1: bool = Field(False, env="STRATEGY_V1")
    v1_setup_breakout: bool = Field(False, env="V1_SETUP_BREAKOUT")
    v1_setup_trap: bool = Field(False, env="V1_SETUP_TRAP")
    v2_trend_pullback: bool = Field(True, env="V2_TREND_PULLBACK")
    v3_trend_breakout: bool = Field(False, env="V3_TREND_BREAKOUT")
    donchian_n_15m: int = Field(20, env="DONCHIAN_N_15M")
    body_atr_15m: float = Field(0.25, env="BODY_ATR_15M")
    trend_sep_atr_1h: float = Field(0.8, env="TREND_SEP_ATR_1H")
    use_5m_confirm: bool = Field(True, env="USE_5M_CONFIRM")
    pullback_tol_atr: float = Field(0.10, env="PULLBACK_TOL_ATR")
    trend_min_sep_atr: float = Field(0.35, env="TREND_MIN_SEP_ATR")
    momo_min_body_atr_5m: float = Field(0.25, env="MOMO_MIN_BODY_ATR_5M")
    retest_confirm_mode: str = Field("bos", env="RETEST_CONFIRM_MODE")
    bos_lookback_5m: int = Field(20, env="BOS_LOOKBACK_5M")
    breakout_strong_market: bool = Field(False, env="BREAKOUT_STRONG_MARKET")
    breakout_strong_body_pct: float = Field(0.60, env="BREAKOUT_STRONG_BODY_PCT")
    breakout_buffer_atr: float = Field(0.10, env="BREAKOUT_BUFFER_ATR")
    trap_min_wick_atr: float = Field(0.8, env="TRAP_MIN_WICK_ATR")
    require_1h_ema200_align: bool = Field(False, env="REQUIRE_1H_EMA200_ALIGN")
    require_5m_ema20_confirm: bool = Field(False, env="REQUIRE_5M_EMA20_CONFIRM")
    min_atr_pct_15m: float = Field(0.2, env="MIN_ATR_PCT_15M")
    max_atr_pct_15m: float = Field(3.0, env="MAX_ATR_PCT_15M")
    log_v3_triggers: bool = Field(False, env="LOG_V3_TRIGGERS")

    @root_validator(pre=False)
    def _normalize(cls, values):
        values["donchian_n_15m"] = max(1, int(values.get("donchian_n_15m", 20)))
        values["body_atr_15m"] = max(0.0, float(values.get("body_atr_15m", 0.25)))
        values["trend_sep_atr_1h"] = max(0.0, float(values.get("trend_sep_atr_1h", 0.8)))
        values["pullback_tol_atr"] = max(0.0, float(values.get("pullback_tol_atr", 0.10)))
        values["trend_min_sep_atr"] = max(0.0, float(values.get("trend_min_sep_atr", 0.35)))
        values["momo_min_body_atr_5m"] = max(0.0, float(values.get("momo_min_body_atr_5m", 0.25)))
        values["breakout_buffer_atr"] = max(0.0, float(values.get("breakout_buffer_atr", 0.10)))
        values["trap_min_wick_atr"] = max(0.0, float(values.get("trap_min_wick_atr", 0.8)))
        mode = str(values.get("retest_confirm_mode") or "bos").lower()
        values["retest_confirm_mode"] = mode if mode in {"none", "ema20", "bos"} else "bos"
        values["bos_lookback_5m"] = max(1, int(values.get("bos_lookback_5m", 20)))
        return values


class ReplaySettings(BaseSettings):
    be_at_r: float = Field(1.0, env="BE_AT_R")
    partial_tp_at_r: float = Field(0.0, env="PARTIAL_TP_AT_R")
    trail_after_r: float = Field(0.0, env="TRAIL_AFTER_R")
    replay_exit_mode: str = Field("hard", env="REPLAY_EXIT_MODE")
    replay_progress_every: int = Field(0, env="REPLAY_PROGRESS_EVERY")

    @root_validator(pre=False)
    def _normalize(cls, values):
        values["be_at_r"] = max(0.0, float(values.get("be_at_r", 1.0)))
        values["partial_tp_at_r"] = max(0.0, float(values.get("partial_tp_at_r", 0.0)))
        values["trail_after_r"] = max(0.0, float(values.get("trail_after_r", 0.0)))
        mode = str(values.get("replay_exit_mode") or "hard").lower()
        values["replay_exit_mode"] = "hard" if mode == "hard" else "legacy"
        values["replay_progress_every"] = max(0, int(values.get("replay_progress_every", 0)))
        return values


class CacheSettings(BaseSettings):
    candles_cache_ttl_seconds: int = Field(120, env="CANDLES_CACHE_TTL_SECONDS")
    cache_only_gap_bars_max: int = Field(12, env="CACHE_ONLY_GAP_BARS_MAX")

    @root_validator(pre=False)
    def _normalize(cls, values):
        values["candles_cache_ttl_seconds"] = max(1, int(values.get("candles_cache_ttl_seconds", 120)))
        values["cache_only_gap_bars_max"] = max(0, int(values.get("cache_only_gap_bars_max", 12)))
        return values


class DashboardSettings(BaseSettings):
    host: str = Field("127.0.0.1", env="DASHBOARD_HOST")
    port: int = Field(8000, env="DASHBOARD_PORT")
    telegram: bool = Field(False, env="DASHBOARD_TELEGRAM")
    top_n: int = Field(3, env="DASHBOARD_TOP_N")
    include_blocked: bool = Field(True, env="DASHBOARD_INCLUDE_BLOCKED")
    include_market_snapshot: bool = Field(True, env="DASHBOARD_INCLUDE_MARKET_SNAPSHOT")
    include_debug_why_none: bool = Field(False, env="DASHBOARD_INCLUDE_DEBUG_WHY_NONE")

    @root_validator(pre=False)
    def _normalize(cls, values):
        values["host"] = str(values.get("host") or "127.0.0.1").strip() or "127.0.0.1"
        port = int(values.get("port", 8000))
        values["port"] = port if 1 <= port <= 65535 else 8000
        values["top_n"] = max(1, int(values.get("top_n", 3)))
        return values


class Settings(BaseModel):
    bybit: BybitSettings
    telegram: TelegramSettings
    risk: RiskSettings
    strategy_v3: StrategyV3Settings
    replay: ReplaySettings
    cache: CacheSettings
    dashboard: DashboardSettings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _bootstrap_env()
    return Settings(
        bybit=BybitSettings(),
        telegram=TelegramSettings(),
        risk=RiskSettings(),
        strategy_v3=StrategyV3Settings(),
        replay=ReplaySettings(),
        cache=CacheSettings(),
        dashboard=DashboardSettings(),
    )


def validate_env() -> tuple[bool, list[str]]:
    """
    Load settings and validate required env.
    Returns (ok, missing_keys). missing_keys is empty when ok is True.
    Fails when WATCHLIST is empty and WATCHLIST_MODE is static (or not set).
    """
    missing: list[str] = []
    try:
        s = get_settings()
    except Exception as e:
        return False, [f"Failed to load settings: {e}"]
    mode = str(s.risk.watchlist_mode or "static").strip().lower()
    watchlist = s.risk.watchlist
    if not watchlist and mode == "static":
        missing.append(
            "WATCHLIST is empty and WATCHLIST_MODE=static. "
            "Set WATCHLIST in .env (e.g. WATCHLIST=BTCUSDT,ETHUSDT) or set WATCHLIST_MODE=topn|dynamic|market."
        )
    return (len(missing) == 0, missing)


def debug_risk_config(logger=None):
    s = get_settings()
    risk_data = {
        "WATCHLIST_MODE": s.risk.watchlist_mode,
        "WATCHLIST_TOP_N": s.risk.watchlist_top_n,
        "WATCHLIST_REFRESH_MINUTES": s.risk.watchlist_refresh_minutes,
        "WATCHLIST": s.risk.watchlist,
        "POSITION_MODE": s.risk.position_mode,
        "MAX_CONCURRENT_POSITIONS": s.risk.max_concurrent_positions,
        "MAX_OPEN_POSITIONS": s.risk.max_open_positions,
        "RISK_KILL_SWITCH": s.risk.risk_kill_switch,
        "SCAN_SECONDS": s.risk.scan_seconds,
        "TF_BIAS": s.risk.tf_bias,
        "TF_SETUP": s.risk.tf_setup,
        "TF_TRIGGER": s.risk.tf_trigger,
        "TF_TIMING": s.risk.tf_timing,
    }
    target_logger = logger or logging.getLogger(__name__)
    target_logger.debug("Risk config: %s", risk_data)
    return risk_data

