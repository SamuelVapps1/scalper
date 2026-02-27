"""
V3 Trend-Continuation Breakout (TCB) - pure strategy module.
Shared by replay and live scan. No logging. No new dependencies.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from scalper.models import StrategyResult, ensure_strategy_result

# TF key variants: "4H"/"1H"/"15m"/"5m" OR 240/60/15/5 OR "240m"/"60m"/"15m"/"5m"
_TF_ALIASES: Dict[str, List[Union[str, int]]] = {
    "4h": ["4H", "4h", "240", 240, "240m", "240M"],
    "1h": ["1H", "1h", "60", 60, "60m", "60M"],
    "15m": ["15m", "15M", "15", 15],
    "5m": ["5m", "5M", "5", 5],
}


def _resolve_tf(snapshot: Dict[Any, Any], tf_name: str) -> Optional[Dict[str, Any]]:
    """Resolve TF data from snapshot. Tries all key variants. Returns snapshot dict or None."""
    if not snapshot:
        return None
    aliases = _TF_ALIASES.get(tf_name.lower(), [tf_name])
    for key in aliases:
        if key in snapshot:
            val = snapshot[key]
            if isinstance(val, dict):
                return val
    return None


def _candle_ts_ms(candle: Dict[str, Any]) -> int:
    """Parse candle timestamp to ms. Supports timestamp, timestamp_utc (iso), ts."""
    ts = candle.get("timestamp") or candle.get("ts") or candle.get("timestamp_utc")
    if ts is None:
        return 0
    if isinstance(ts, (int, float)):
        v = int(ts)
        return v if v >= 1e12 else int(v * 1000)
    if isinstance(ts, str):
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            pass
    return 0


def _build_15m_to_5m_index(
    candles_15m: List[Dict[str, Any]],
    candles_5m: List[Dict[str, Any]],
) -> List[int]:
    """
    Precompute map: for each 15m index i15, map15_to_5[i15] = index j in 5m
    such that ts5[j] <= ts15[i15] and ts5[j+1] > ts15[i15] (or j is last).
    O(len(15m) + len(5m)) single forward pass.
    """
    ts15 = [_candle_ts_ms(c) for c in candles_15m]
    ts5 = [_candle_ts_ms(c) for c in candles_5m]
    map15_to_5: List[int] = []
    j = 0
    for i15 in range(len(ts15)):
        while j + 1 < len(ts5) and ts5[j + 1] <= ts15[i15]:
            j += 1
        map15_to_5.append(j)
    return map15_to_5


def _atr14(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[Optional[float]]:
    """ATR14 Wilder smoothing. Returns list with None until index period."""
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if not (len(highs) == len(lows) == n) or n <= period:
        return out
    tr = [0.0] * n
    tr[0] = abs(highs[0] - lows[0])
    for i in range(1, n):
        tr[i] = max(
            abs(highs[i] - lows[i]),
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
    atr_val = sum(tr[1 : period + 1]) / period
    out[period] = atr_val
    for i in range(period + 1, n):
        atr_val = ((atr_val * (period - 1)) + tr[i]) / period
        out[i] = atr_val
    return out


def v3_tcb_evaluate(
    symbol: str,
    snapshot_symbol: Dict[Any, Any],
    candles_15m: List[Dict[str, Any]],
    candles_5m: Optional[List[Dict[str, Any]]],
    i15: int,
    params: Dict[str, Any],
    *,
    map15_to_5: Optional[List[int]] = None,
    close5: Optional[List[float]] = None,
) -> StrategyResult:
    """
    V3 Trend-Continuation Breakout evaluation.

    Returns StrategyResult.
    """
    debug: Dict[str, Any] = {"symbol": symbol, "i15": i15}

    def _ret(payload: Dict[str, Any]) -> StrategyResult:
        return ensure_strategy_result(payload)
    donchian_n = int(params.get("DONCHIAN_N_15M", 20))
    body_atr_min = float(params.get("BODY_ATR_15M", 0.25))
    trend_sep_atr_1h = float(params.get("TREND_SEP_ATR_1H", 0.8))
    use_5m_confirm = bool(params.get("USE_5M_CONFIRM", True))

    snap_4h = _resolve_tf(snapshot_symbol, "4h")
    if not snap_4h:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_missing_4h_slope_inputs",
            "debug": {**debug, "snap_4h": "missing"},
        })
    close_4h = float(snap_4h.get("close", 0) or 0)
    ema200_4h = float(snap_4h.get("ema200", 0) or 0)
    slope10 = snap_4h.get("ema200_slope_10")
    if slope10 is None:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_missing_4h_slope_inputs",
            "debug": {**debug, "slope10": None},
        })
    slope10 = float(slope10)

    if close_4h > ema200_4h and slope10 > 0:
        bias = "LONG"
    elif close_4h < ema200_4h and slope10 < 0:
        bias = "SHORT"
    else:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_bias_none",
            "debug": {**debug, "close_4h": close_4h, "ema200_4h": ema200_4h, "slope10": slope10},
        })

    snap_1h = _resolve_tf(snapshot_symbol, "1h")
    if not snap_1h:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_missing_1h_inputs",
            "debug": {**debug, "bias": bias},
        })
    ema20_1h = float(snap_1h.get("ema20", 0) or 0)
    ema50_1h = float(snap_1h.get("ema50", 0) or 0)
    ema200_1h = float(snap_1h.get("ema200", 0) or 0)
    atr_1h = float(snap_1h.get("atr14", 0) or 0)
    if atr_1h <= 0:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_missing_1h_inputs",
            "debug": {**debug, "bias": bias, "atr_1h": atr_1h},
        })

    if bias == "LONG":
        trend_ok = ema20_1h > ema50_1h > ema200_1h
        sep = (ema20_1h - ema200_1h) / atr_1h if trend_ok else 0.0
    else:
        trend_ok = ema20_1h < ema50_1h < ema200_1h
        sep = (ema200_1h - ema20_1h) / atr_1h if trend_ok else 0.0

    if not trend_ok:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_trend_align_fail",
            "debug": {**debug, "bias": bias, "ema20_1h": ema20_1h, "ema50_1h": ema50_1h, "ema200_1h": ema200_1h},
        })
    if sep < trend_sep_atr_1h:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_sep_fail",
            "debug": {**debug, "bias": bias, "sep": sep, "trend_sep_atr_1h": trend_sep_atr_1h},
        })

    if not candles_15m or i15 >= len(candles_15m):
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_not_enough_15m_bars",
            "debug": {**debug, "len_15m": len(candles_15m or []), "i15": i15},
        })
    if i15 < donchian_n:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_not_enough_15m_bars",
            "debug": {**debug, "i15": i15, "donchian_n": donchian_n},
        })

    try:
        highs = [float(c.get("high", 0) or 0) for c in candles_15m[: i15 + 1]]
        lows = [float(c.get("low", 0) or 0) for c in candles_15m[: i15 + 1]]
        closes = [float(c.get("close", 0) or 0) for c in candles_15m[: i15 + 1]]
        opens = [float(c.get("open", 0) or 0) for c in candles_15m[: i15 + 1]]
    except (TypeError, ValueError, KeyError):
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_not_enough_15m_bars",
            "debug": {**debug},
        })

    atr_list = _atr14(highs, lows, closes, 14)
    if atr_list[i15] is None:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_not_enough_15m_bars",
            "debug": {**debug, "atr_not_ready": True},
        })
    atr15m = float(atr_list[i15])
    close_15m = closes[i15]
    open_15m = opens[i15]
    body = abs(close_15m - open_15m)
    ts_15m_ms = _candle_ts_ms(candles_15m[i15])

    donch_high = max(highs[i15 - donchian_n : i15])
    donch_low = min(lows[i15 - donchian_n : i15])
    breakout_level = donch_high if bias == "LONG" else donch_low

    if bias == "LONG":
        broken = close_15m > donch_high
    else:
        broken = close_15m < donch_low

    if not broken:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_donchian_not_broken",
            "debug": {**debug, "bias": bias, "close_15m": close_15m, "donch_high": donch_high, "donch_low": donch_low},
        })

    body_min = body_atr_min * atr15m
    if body < body_min:
        return _ret({
            "ok": False,
            "side": None,
            "breakout_level": None,
            "reason": "v3_body_too_small",
            "debug": {**debug, "bias": bias, "body": body, "body_min": body_min},
        })

    if use_5m_confirm:
        if map15_to_5 is not None and close5 is not None:
            j5 = map15_to_5[i15] if i15 < len(map15_to_5) else -1
            if j5 < 0 or j5 >= len(close5):
                return _ret({
                    "ok": False,
                    "side": None,
                    "breakout_level": None,
                    "reason": "v3_missing_5m",
                    "debug": {**debug, "bias": bias, "ts_15m_ms": ts_15m_ms},
                })
            close_5m = close5[j5]
        elif candles_5m:
            map15_to_5_local = _build_15m_to_5m_index(candles_15m, candles_5m)
            j5 = map15_to_5_local[i15]
            if j5 < 0 or j5 >= len(candles_5m) or _candle_ts_ms(candles_5m[j5]) > ts_15m_ms:
                return _ret({
                    "ok": False,
                    "side": None,
                    "breakout_level": None,
                    "reason": "v3_missing_5m",
                    "debug": {**debug, "bias": bias, "ts_15m_ms": ts_15m_ms},
                })
            close_5m = float(candles_5m[j5].get("close", 0) or 0)
        else:
            return _ret({
                "ok": False,
                "side": None,
                "breakout_level": None,
                "reason": "v3_missing_5m",
                "debug": {**debug, "bias": bias},
            })
        if bias == "LONG":
            confirm_ok = close_5m > breakout_level
        else:
            confirm_ok = close_5m < breakout_level
        if not confirm_ok:
            return _ret({
                "ok": False,
                "side": None,
                "breakout_level": None,
                "reason": "v3_5m_confirm_fail",
                "debug": {**debug, "bias": bias, "close_5m": close_5m, "breakout_level": breakout_level},
            })

    return _ret({
        "ok": True,
        "side": bias,
        "breakout_level": breakout_level,
        "reason": "",
        "debug": {
            **debug,
            "bias": bias,
            "close_15m": close_15m,
            "donch_high": donch_high,
            "donch_low": donch_low,
            "atr15m": atr15m,
        },
    })
