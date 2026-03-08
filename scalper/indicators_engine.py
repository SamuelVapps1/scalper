"""
Shared indicator precompute engine.

Precomputes EMA20/50/200 and ATR14 arrays once per TF candle series.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from indicators import atr_wilder, ema


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

    ema20 = ema(close_list, 20)
    ema50 = ema(close_list, 50)
    ema200 = ema(close_list, 200)
    atr14 = atr_wilder(high_list, low_list, close_list, 14)
    ema200_prev10 = [ema200[i - 10] if i >= 10 else None for i in range(len(ema200))]

    return {
        "ts": ts_list,
        "open": open_list,
        "close": close_list,
        "high": high_list,
        "low": low_list,
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "ema200_prev10": ema200_prev10,
        "atr14": atr14,
    }

