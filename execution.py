"""
Execution adapter skeleton. No real order calls; structure + logging only.
When EXECUTION_MODE != disabled, requires KILL_SWITCH=0 and EXPLICIT_CONFIRM_EXECUTION=1.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

_log = logging.getLogger(__name__)

EXECUTION_GUARDED = "EXECUTION_GUARDED"


def _get_execution_config() -> Tuple[str, bool, bool]:
    try:
        import config
        mode = str(getattr(config, "EXECUTION_MODE", "disabled")).lower()
        kill_switch = bool(getattr(config, "KILL_SWITCH", True))
        explicit_confirm = bool(getattr(config, "EXPLICIT_CONFIRM_EXECUTION", False))
        return mode, kill_switch, explicit_confirm
    except Exception:
        return "disabled", True, False


def check_execution_guard() -> Tuple[bool, str]:
    """
    If EXECUTION_MODE != disabled: require KILL_SWITCH=0 and EXPLICIT_CONFIRM_EXECUTION=1.
    Returns (allowed, reason). If allowed=False, reason=EXECUTION_GUARDED.
    """
    mode, kill_switch, explicit_confirm = _get_execution_config()
    if mode == "disabled":
        return True, ""
    if kill_switch:
        _log.debug("Execution blocked: KILL_SWITCH=1")
        return False, EXECUTION_GUARDED
    if not explicit_confirm:
        _log.debug("Execution blocked: EXPLICIT_CONFIRM_EXECUTION=0")
        return False, EXECUTION_GUARDED
    return True, ""


def build_order_plan(intent: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build order plan from intent. Returns dict with symbol, side, qty, entry_type, sl, tp.
    No real order calls; structure + logging only.
    """
    allowed, reason = check_execution_guard()
    if not allowed:
        _log.debug("build_order_plan skipped: %s", reason)
        return None

    symbol = str(intent.get("symbol", "") or "").strip().upper()
    side = str(intent.get("side", "") or intent.get("direction", "") or "").strip().upper()
    if not symbol or side not in ("LONG", "SHORT"):
        _log.debug("build_order_plan skipped: invalid symbol or side")
        return None

    strategy = str(intent.get("strategy", "") or intent.get("setup", "") or "").strip()
    raw_entry = str(intent.get("entry_type", "market") or "market").strip().lower()
    if raw_entry in ("market", "market_sim") or (raw_entry and "market" in raw_entry):
        entry_type = "market"
    elif raw_entry and "retest" in raw_entry:
        entry_type = "retest"
    else:
        entry_type = raw_entry or "market"

    try:
        import config
        notional_usdt = float(getattr(config, "PAPER_POSITION_USDT", 20.0))
    except Exception:
        notional_usdt = 20.0

    entry_price = float(intent.get("entry_price", 0.0) or intent.get("entry", 0.0) or 0.0)
    sl_price = float(intent.get("sl_price", 0.0) or intent.get("sl", 0.0) or 0.0)
    tp_price = float(intent.get("tp_price", 0.0) or intent.get("tp", 0.0) or 0.0)

    if entry_price <= 0:
        _log.debug("build_order_plan skipped: no entry_price in intent")
        return None

    qty = notional_usdt / max(entry_price, 1e-10)
    if sl_price <= 0:
        try:
            import config
            atr = float(intent.get("atr14", 0.0) or intent.get("atr", 0.0) or 0.0)
            sl_atr = float(getattr(config, "PAPER_SL_ATR", 1.0))
            if atr > 0:
                if side == "LONG":
                    sl_price = entry_price - sl_atr * atr
                else:
                    sl_price = entry_price + sl_atr * atr
        except Exception:
            pass
    if tp_price <= 0:
        try:
            import config
            atr = float(intent.get("atr14", 0.0) or intent.get("atr", 0.0) or 0.0)
            tp_atr = float(getattr(config, "PAPER_TP_ATR", 1.5))
            if atr > 0:
                if side == "LONG":
                    tp_price = entry_price + tp_atr * atr
                else:
                    tp_price = entry_price - tp_atr * atr
        except Exception:
            pass

    plan = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry_type": entry_type,
        "sl": sl_price,
        "tp": tp_price,
    }
    _log.info(
        "execution order_plan symbol=%s side=%s qty=%.6f entry_type=%s sl=%.4f tp=%.4f",
        symbol, side, qty, entry_type, sl_price, tp_price,
    )
    return plan
