from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

_log = logging.getLogger(__name__)


def _as_pos_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_overrides(raw: str) -> Dict[str, float]:
    if not raw or not str(raw).strip():
        return {}
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            return {}
        return {str(k).strip().upper(): float(v) for k, v in d.items() if v is not None}
    except (json.JSONDecodeError, TypeError, ValueError):
        _log.debug("levels: invalid overrides json %s", raw[:80] if raw else "")
        return {}


def resolve_sl_tp_multipliers(
    symbol: Optional[str],
    sl_atr_mult: float,
    tp_atr_mult: float,
    sl_overrides: Optional[Dict[str, float]] = None,
    tp_overrides: Optional[Dict[str, float]] = None,
) -> tuple[float, float]:
    """Resolve effective SL/TP ATR multipliers; symbol-specific overrides replace base when set."""
    sym = (str(symbol or "").strip().upper()) or None
    sl_mult = max(1e-6, float(sl_atr_mult))
    tp_mult = max(1e-6, float(tp_atr_mult))
    if sym and sl_overrides and sym in sl_overrides:
        sl_mult = max(1e-6, float(sl_overrides[sym]))
    if sym and tp_overrides and sym in tp_overrides:
        tp_mult = max(1e-6, float(tp_overrides[sym]))
    return (sl_mult, tp_mult)


def compute_tp_sl(
    entry: float,
    atr: float,
    side: str,
    sl_atr_mult: float,
    tp_atr_mult: float,
    symbol: Optional[str] = None,
    sl_overrides: Optional[Dict[str, float]] = None,
    tp_overrides: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    entry_val = _as_pos_float(entry)
    atr_val = _as_pos_float(atr)
    sl_mult, tp_mult = resolve_sl_tp_multipliers(
        symbol, sl_atr_mult, tp_atr_mult, sl_overrides, tp_overrides
    )
    sl_mult = _as_pos_float(sl_mult) or _as_pos_float(sl_atr_mult)
    tp_mult = _as_pos_float(tp_mult) or _as_pos_float(tp_atr_mult)
    side_norm = str(side or "").strip().upper()

    out: Dict[str, Any] = {
        "entry": entry_val,
        "sl": None,
        "tp": None,
        "sl_pct": None,
        "tp_pct": None,
        "reason": "ok",
    }
    if side_norm not in {"LONG", "SHORT"}:
        out["reason"] = f"invalid_side:{side_norm or 'missing'}"
        return out
    if entry_val is None:
        out["reason"] = "invalid_entry"
        return out
    if atr_val is None:
        out["reason"] = "invalid_atr"
        return out
    if sl_mult is None:
        out["reason"] = "invalid_sl_atr_mult"
        return out
    if tp_mult is None:
        out["reason"] = "invalid_tp_atr_mult"
        return out

    sl_delta = atr_val * sl_mult
    tp_delta = atr_val * tp_mult

    if side_norm == "LONG":
        sl_price = entry_val - sl_delta
        tp_price = entry_val + tp_delta
    else:
        sl_price = entry_val + sl_delta
        tp_price = entry_val - tp_delta

    if sl_price <= 0 or tp_price <= 0:
        out["reason"] = "non_positive_level"
        return out

    out["sl"] = sl_price
    out["tp"] = tp_price
    out["sl_pct"] = (abs(entry_val - sl_price) / max(entry_val, 1e-10)) * 100.0
    out["tp_pct"] = (abs(tp_price - entry_val) / max(entry_val, 1e-10)) * 100.0
    return out

