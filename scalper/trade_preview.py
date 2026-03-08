from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple


_VALID_SIDES = {"LONG", "SHORT"}


def _as_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN guard
        return None
    return out


def _normalized_side(raw: Any) -> str:
    return str(raw or "").strip().upper()


def _resolve_atr(
    *,
    signal: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    mtf_snapshot: Optional[Dict[Any, Dict[str, Any]]],
    candles: List[Dict[str, Any]],
    trigger_tf: int,
    fallback_lookback: int = 5,
) -> Tuple[float, str, bool]:
    meta = signal.get("meta") if isinstance(signal.get("meta"), dict) else {}
    candidates: List[Tuple[str, Any, bool]] = [
        ("signal_meta_atr14", meta.get("atr14"), False),
        ("signal_atr14", signal.get("atr14"), False),
        ("market_snapshot_atr14", market_snapshot.get("atr14"), False),
    ]
    frames = mtf_snapshot or {}
    candidates.append(("mtf_15m_atr14", (frames.get(15) or {}).get("atr14"), False))
    candidates.append(
        (
            f"mtf_trigger_{int(trigger_tf)}m_atr14",
            (frames.get(int(trigger_tf)) or {}).get("atr14"),
            False,
        )
    )
    for source, raw, degraded in candidates:
        atr_val = _as_float(raw)
        if atr_val is not None and atr_val > 0:
            return (atr_val, source, degraded)

    window = candles[-max(1, int(fallback_lookback)) :]
    ranges: List[float] = []
    for candle in window:
        high = _as_float(candle.get("high"))
        low = _as_float(candle.get("low"))
        if high is None or low is None:
            continue
        rng = abs(high - low)
        if rng > 0:
            ranges.append(rng)
    if ranges:
        atr_val = sum(ranges) / len(ranges)
        return (atr_val, "recent_candle_range_fallback", True)

    return (0.0, "unavailable", True)


def _resolve_entry(
    *,
    signal: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    candles: List[Dict[str, Any]],
    side: str,
) -> Tuple[Optional[float], str, Optional[float]]:
    entry_type = str(signal.get("entry_type", "market") or "market").strip().lower()
    if "market" in entry_type:
        close_price = _as_float(market_snapshot.get("close"))
        if close_price is None and candles:
            close_price = _as_float((candles[-1] or {}).get("close"))
        if close_price is None or close_price <= 0:
            return (None, "ENTRY_PRICE_UNAVAILABLE", None)
        return (close_price, "market", None)

    if "retest" in entry_type:
        meta = signal.get("meta") if isinstance(signal.get("meta"), dict) else {}
        retest = _as_float(meta.get("retest_level"))
        if retest is None:
            retest = _as_float(signal.get("level_ref"))
        if retest is None or retest <= 0:
            return (None, "RETEST_LEVEL_UNAVAILABLE", None)
        return (retest, "retest", retest)

    if side in _VALID_SIDES:
        close_price = _as_float(market_snapshot.get("close"))
        if close_price is None and candles:
            close_price = _as_float((candles[-1] or {}).get("close"))
        if close_price is not None and close_price > 0:
            return (close_price, f"{entry_type}_as_market", None)
    return (None, "ENTRY_TYPE_UNSUPPORTED", None)


def build_trade_preview(
    *,
    signal: Dict[str, Any],
    market_snapshot: Optional[Dict[str, Any]],
    candles: Optional[List[Dict[str, Any]]] = None,
    mtf_snapshot: Optional[Dict[Any, Dict[str, Any]]] = None,
    risk_settings: Optional[Any] = None,
    equity_usdt: Optional[float] = None,
    for_execution: bool = True,
) -> Dict[str, Any]:
    signal = dict(signal or {})
    market_snapshot = dict(market_snapshot or {})
    candles = list(candles or [])

    symbol = str(signal.get("symbol", "") or "").strip().upper()
    side = _normalized_side(signal.get("side", signal.get("direction", "")))
    strategy = str(signal.get("strategy", signal.get("setup", "")) or "").strip()
    bar_ts_used = str(
        signal.get("bar_ts_used")
        or signal.get("timestamp_utc")
        or signal.get("ts")
        or market_snapshot.get("bar_ts_used")
        or market_snapshot.get("ts")
        or ""
    ).strip()

    preview: Dict[str, Any] = {
        "ok": False,
        "reason": "",
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "entry": None,
        "sl": None,
        "tp": None,
        "sl_pct": None,
        "tp_pct": None,
        "qty": None,
        "notional": None,
        "atr_used": None,
        "atr_source": "",
        "bar_ts_used": bar_ts_used,
        "preview_only": not bool(for_execution),
        "executable": False,
        "degraded_preview": False,
        "entry_source": "",
        "rr_ratio": None,
        "atr_pct": None,
    }

    if not symbol:
        preview["reason"] = "INTENT_MISSING_SYMBOL"
        return preview
    if side not in _VALID_SIDES:
        preview["reason"] = "INTENT_INVALID_SIDE"
        return preview

    trigger_tf = int(getattr(risk_settings, "tf_trigger", 15) or 15) if risk_settings else 15
    atr_value, atr_source, degraded = _resolve_atr(
        signal=signal,
        market_snapshot=market_snapshot,
        mtf_snapshot=mtf_snapshot,
        candles=candles,
        trigger_tf=trigger_tf,
    )
    preview["atr_used"] = atr_value
    preview["atr_source"] = atr_source
    preview["degraded_preview"] = bool(degraded)
    logging.debug(
        "PREVIEW ATR source=%s symbol=%s side=%s atr=%.8f degraded=%s",
        atr_source,
        symbol,
        side,
        atr_value,
        degraded,
    )
    if atr_value <= 0:
        preview["reason"] = "ATR_UNAVAILABLE"
        return preview
    if degraded:
        preview["reason"] = "ATR_DEGRADED"
        return preview

    entry, entry_source, retest_level = _resolve_entry(
        signal=signal,
        market_snapshot=market_snapshot,
        candles=candles,
        side=side,
    )
    preview["entry_source"] = entry_source
    if entry is None or entry <= 0:
        preview["reason"] = "LEVELS_UNAVAILABLE"
        return preview

    meta = signal.get("meta") if isinstance(signal.get("meta"), dict) else {}
    sl_hint = _as_float(meta.get("sl_hint"))
    tp_hint = _as_float(meta.get("tp_hint", signal.get("tp_hint")))
    tp_r_mult = _as_float(meta.get("tp_r_mult"))
    sl_atr_mult = float(getattr(risk_settings, "paper_sl_atr", 1.0) or 1.0) if risk_settings else 1.0
    tp_atr_mult = float(getattr(risk_settings, "paper_tp_atr", 1.5) or 1.5) if risk_settings else 1.5

    if sl_hint is not None and sl_hint > 0:
        sl = float(sl_hint)
    else:
        sl = entry - (sl_atr_mult * atr_value) if side == "LONG" else entry + (sl_atr_mult * atr_value)

    sl_distance = abs(entry - sl)
    if sl_distance <= 0:
        preview["reason"] = "SL_DISTANCE_INVALID"
        return preview

    if tp_hint is not None and tp_hint > 0:
        tp = float(tp_hint)
    elif tp_r_mult is not None and tp_r_mult > 0:
        tp = entry + (tp_r_mult * sl_distance if side == "LONG" else -tp_r_mult * sl_distance)
    else:
        tp = entry + (tp_atr_mult * atr_value if side == "LONG" else -tp_atr_mult * atr_value)

    tp_distance = abs(tp - entry)
    if tp_distance <= 0:
        preview["reason"] = "TP_DISTANCE_INVALID"
        return preview

    if side == "LONG" and not (sl < entry < tp):
        preview["reason"] = "LEVEL_GEOMETRY_INVALID_LONG"
        return preview
    if side == "SHORT" and not (tp < entry < sl):
        preview["reason"] = "LEVEL_GEOMETRY_INVALID_SHORT"
        return preview

    rr_ratio = tp_distance / max(sl_distance, 1e-10)
    min_rr = float(getattr(risk_settings, "preview_min_rr", 1.1) or 1.1) if risk_settings else 1.1
    if rr_ratio < min_rr:
        preview["reason"] = "RR_BELOW_MINIMUM"
        return preview

    atr_pct = (atr_value / max(entry, 1e-10)) * 100.0
    min_atr_pct = float(getattr(risk_settings, "preview_min_atr_pct", 0.05) or 0.05) if risk_settings else 0.05
    max_atr_pct = float(getattr(risk_settings, "preview_max_atr_pct", 12.0) or 12.0) if risk_settings else 12.0
    if atr_pct < min_atr_pct:
        preview["reason"] = "ATR_PCT_TOO_LOW"
        return preview
    if atr_pct > max_atr_pct:
        preview["reason"] = "ATR_PCT_TOO_HIGH"
        return preview

    if retest_level is not None:
        drift_pct = abs(entry - retest_level) / max(entry, 1e-10) * 100.0
        max_retest_drift_pct = float(
            getattr(risk_settings, "preview_max_retest_drift_pct", max(0.5, atr_pct * 0.75)) or max(0.5, atr_pct * 0.75)
        ) if risk_settings else max(0.5, atr_pct * 0.75)
        if drift_pct > max_retest_drift_pct:
            preview["reason"] = "ENTRY_TOO_FAR_FROM_RETEST"
            return preview

    if equity_usdt is None:
        if risk_settings is not None:
            equity_usdt = float(getattr(risk_settings, "paper_start_equity_usdt", 1000.0) or 1000.0)
        else:
            equity_usdt = 1000.0
    risk_pct = float(getattr(risk_settings, "risk_per_trade_pct", 0.15) or 0.15) if risk_settings else 0.15
    risk_usdt = max(0.0, float(equity_usdt)) * (max(0.0, risk_pct) / 100.0)
    qty = risk_usdt / sl_distance if sl_distance > 0 else 0.0
    notional = qty * entry
    if qty <= 0 or notional <= 0:
        preview["reason"] = "POSITION_SIZE_INVALID"
        return preview

    preview.update(
        {
            "ok": True,
            "reason": "OK",
            "entry": float(entry),
            "sl": float(sl),
            "tp": float(tp),
            "sl_pct": float(sl_distance / max(entry, 1e-10) * 100.0),
            "tp_pct": float(tp_distance / max(entry, 1e-10) * 100.0),
            "qty": float(qty),
            "notional": float(notional),
            "rr_ratio": float(rr_ratio),
            "atr_pct": float(atr_pct),
            "executable": bool(for_execution),
        }
    )
    return preview
