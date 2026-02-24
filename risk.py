from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Tuple


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
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


def _parse_iso_to_epoch(raw_ts: str) -> int | None:
    value = str(raw_ts or "").strip()
    if not value:
        return None
    if value.isdigit():
        try:
            return int(value)
        except ValueError:
            return None
    value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def evaluate_intent(intent: Dict[str, Any], state: Dict[str, Any], now_ts: int) -> Tuple[str, str]:
    """
    DRY-RUN risk gate for trade intents.
    Returns: ("ALLOW"|"BLOCK", reason)
    """
    kill_switch = _env_bool("KILL_SWITCH", False)
    max_trades_day = max(0, _env_int("MAX_TRADES_DAY", 999))
    cooldown_after_loss_min = max(0, _env_int("COOLDOWN_AFTER_LOSS_MIN", 0))
    max_daily_loss_usdt = max(0.0, _env_float("MAX_DAILY_LOSS_USDT", 999999.0))

    if kill_switch:
        return "BLOCK", "KILL_SWITCH_ON"

    trade_count_today = int(state.get("trade_count_today", 0) or 0)
    if max_trades_day > 0 and trade_count_today >= max_trades_day:
        return "BLOCK", "MAX_TRADES_DAY"

    daily_pnl_sim = float(state.get("daily_pnl_sim", 0.0) or 0.0)
    if max_daily_loss_usdt > 0 and daily_pnl_sim <= -max_daily_loss_usdt:
        return "BLOCK", "MAX_DAILY_LOSS_USDT"

    if cooldown_after_loss_min > 0:
        cooldown_until_ts = _parse_iso_to_epoch(str(state.get("cooldown_until_utc", "") or ""))
        if cooldown_until_ts is not None and int(now_ts) < cooldown_until_ts:
            return "BLOCK", "COOLDOWN_ACTIVE"

    try:
        import config
    except ImportError:
        config = None

    position_mode = getattr(config, "POSITION_MODE", "global") if config else "global"
    max_conc = int(getattr(config, "MAX_CONCURRENT_POSITIONS", 1)) if config else 1

    open_positions = list(state.get("open_positions", []) or [])

    def is_open(p: dict) -> bool:
        status = str(p.get("status", "OPEN") or "").strip().upper()
        return status == "OPEN"

    open_positions = [p for p in open_positions if isinstance(p, dict) and is_open(p)]
    open_count = len(open_positions)

    if open_count >= max_conc:
        return "BLOCK", "MAX_CONCURRENT_POSITIONS"

    if position_mode == "per_symbol":
        sym = str(intent.get("symbol", "") or "").strip().upper()
        if any(str(p.get("symbol", "") or "").strip().upper() == sym for p in open_positions):
            return "BLOCK", "SYMBOL_ALREADY_OPEN"

    if position_mode == "global" and open_count > 0:
        return "BLOCK", "GLOBAL_ONE_POSITION"

    return "ALLOW", "OK"
