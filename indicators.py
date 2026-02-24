"""
Indicators helper: EMA and ATR (Wilder). Deterministic, DRY RUN only.
"""
from __future__ import annotations

from typing import List, Optional


def ema(values: List[float], period: int) -> List[float]:
    """EMA on close. Alpha = 2/(period+1)."""
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [values[0]]
    for idx in range(1, len(values)):
        out.append((values[idx] * alpha) + (out[idx - 1] * (1.0 - alpha)))
    return out


def atr_wilder(
    high: List[float],
    low: List[float],
    close: List[float],
    period: int = 14,
) -> List[Optional[float]]:
    """ATR14 (Wilder smoothing). First value at index period."""
    n = len(close)
    out: List[Optional[float]] = [None] * n
    if not (len(high) == len(low) == n) or n <= period:
        return out

    tr = [0.0] * n
    tr[0] = abs(high[0] - low[0])
    for idx in range(1, n):
        tr[idx] = max(
            abs(high[idx] - low[idx]),
            abs(high[idx] - close[idx - 1]),
            abs(low[idx] - close[idx - 1]),
        )

    atr_value = sum(tr[1 : period + 1]) / period
    out[period] = atr_value
    for idx in range(period + 1, n):
        atr_value = ((atr_value * (period - 1)) + tr[idx]) / period
        out[idx] = atr_value
    return out
