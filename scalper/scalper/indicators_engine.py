"""
Shared indicator precompute engine.

Precomputes EMA20/50/200 and ATR14 arrays once per TF candle series.
Uses safe_atr and forward-fill so ATR/OHLC/EMA are never None in latest bar.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from indicators import ema, safe_atr

_log = logging.getLogger(__name__)


def _forward_fill(values: List[Any], default: Any = None) -> List[Any]:
    """Replace None / 0 with previous valid value so latest is never missing."""
    out = list(values)
    prev = default
    for i in range(len(out)):
        v = out[i]
        if v is not None and (not isinstance(v, (int, float)) or float(v) != 0.0):
            prev = v
        elif prev is not None:
            out[i] = prev
    return out


def candle_ts_ms(c: Dict[str, Any]) -> int:
    ts = c.get("timestamp") or c.get("ts") or c.get("timestamp_utc")
    if isinstance(ts, (int, float)):
        return int(ts) if ts >= 1e12 else int(ts * 1000)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            return 0
    return 0


def precompute_tf_indicators(candles: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    ts_list = [candle_ts_ms(c) for c in candles]
    open_list = [float(c.get("open", 0) or 0) for c in candles]
    close_list = [float(c.get("close", 0) or 0) for c in candles]
    high_list = [float(c.get("high", 0) or 0) for c in candles]
    low_list = [float(c.get("low", 0) or 0) for c in candles]

    close_filled = _forward_fill(close_list, close_list[0] if close_list else 0.0)
    ema20 = ema(close_filled, 20)
    ema50 = ema(close_filled, 50)
    ema200 = ema(close_filled, 200)
    atr14_raw = safe_atr(high_list, low_list, close_list, 14, fallback_last=None)
    atr14_filled = _forward_fill([(float(x) if x is not None and float(x) > 0 else None) for x in atr14_raw])
    last_valid_atr = next((v for v in reversed(atr14_filled) if v is not None and float(v) > 0), None)
    atr14 = [v if (v is not None and float(v) > 0) else last_valid_atr for v in atr14_filled]
    if (not atr14 or atr14[-1] is None or float(atr14[-1]) <= 0) and len(high_list) >= 2 and len(low_list) >= 2:
        rng = (max(high_list[-14:]) - min(low_list[-14:])) / max(len(high_list[-14:]), 1)
        atr14 = atr14[:-1] + [max(1e-10, rng)]
    ema200_prev10 = [ema200[i - 10] if i >= 10 else None for i in range(len(ema200))]

    last_atr = atr14[-1] if atr14 else None
    if last_atr is None or (isinstance(last_atr, (int, float)) and float(last_atr) == 0.0):
        _log.warning("precompute_tf_indicators: atr14 still missing for latest bar after fallback")

    return {
        "ts": ts_list,
        "open": _forward_fill(open_list, open_list[0] if open_list else 0.0),
        "close": close_filled,
        "high": _forward_fill(high_list, high_list[0] if high_list else 0.0),
        "low": _forward_fill(low_list, low_list[0] if low_list else 0.0),
        "ema20": _forward_fill(ema20, ema20[-1] if ema20 else None),
        "ema50": _forward_fill(ema50, ema50[-1] if ema50 else None),
        "ema200": _forward_fill(ema200, ema200[-1] if ema200 else None),
        "ema200_prev10": ema200_prev10,
        "atr14": atr14,
    }

