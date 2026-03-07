from __future__ import annotations

from typing import Any, Dict


def _as_pos_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def compute_tp_sl(
    entry: float,
    atr: float,
    side: str,
    sl_atr_mult: float,
    tp_atr_mult: float,
) -> Dict[str, Any]:
    entry_val = _as_pos_float(entry)
    atr_val = _as_pos_float(atr)
    sl_mult = _as_pos_float(sl_atr_mult)
    tp_mult = _as_pos_float(tp_atr_mult)
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

