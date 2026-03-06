from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from scalper.types import Intent


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    S4 Volatility Expansion:
    - BB width low (squeeze) then expansion candle with ATR spike.
    - Direction filter with EMA50 slope and price vs EMA200.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    tf = str(ctx.get("tf", "15"))

    if df.empty or len(df) < 40:
        return []

    # Compute simple Bollinger Bands on close.
    closes = df["close"]
    window = 20
    rolling_mean = closes.rolling(window=window).mean()
    rolling_std = closes.rolling(window=window).std()
    bb_upper = rolling_mean + 2 * rolling_std
    bb_lower = rolling_mean - 2 * rolling_std
    bb_width = (bb_upper - bb_lower) / rolling_mean.replace(0, pd.NA)

    df = df.copy()
    df["bb_width"] = bb_width

    # Consider last 30 bars for squeeze and expansion.
    recent = df.iloc[-30:]
    last = recent.iloc[-1]
    prev = recent.iloc[-2]

    if pd.isna(last["bb_width"]) or pd.isna(prev["bb_width"]):
        return []

    # Squeeze: previous width in lowest 30% of recent, expansion: current width > previous * 1.5.
    width_values = recent["bb_width"].dropna()
    if len(width_values) < 10:
        return []

    threshold = width_values.quantile(0.3)
    squeeze = prev["bb_width"] <= threshold
    expansion = last["bb_width"] >= prev["bb_width"] * 1.5

    atr14 = float(last.get("atr14", 0) or 0)
    if atr14 <= 0:
        return []

    # ATR spike: current true range significantly larger than median of recent.
    tr = recent["high"] - recent["low"]
    tr_median = float(tr.median())
    tr_last = float(last["high"] - last["low"])
    atr_spike = tr_last >= tr_median * 1.5

    ema50 = float(last.get("ema50", 0) or 0)
    ema200 = float(last.get("ema200", 0) or 0)
    if ema50 <= 0 or ema200 <= 0:
        return []

    # EMA50 slope proxy: difference vs 10 bars ago.
    ema50_now = ema50
    ema50_prev = float(recent["ema50"].iloc[-10])
    ema50_slope = (ema50_now - ema50_prev) / max(abs(ema50_prev), 1e-10)

    bar_ts = str(last.get("ts", ctx.get("bar_ts_used", "")) or "")
    entry = float(last["close"])
    high = float(last["high"])
    low = float(last["low"])

    intents: List[Intent] = []
    if not (squeeze and expansion and atr_spike):
        return intents

    # LONG: price above EMA200 and EMA50 sloping up.
    if entry > ema200 and ema50_slope > 0:
        sl = low
        risk = abs(entry - sl)
        tp = entry + 2.0 * risk
        sl_pct = risk / max(entry, 1e-10) * 100.0
        tp_pct = 2.0 * sl_pct
        intents.append(
            Intent(
                symbol=symbol,
                tf=tf,
                side="LONG",
                setup="S4_VOL_EXPANSION",
                confidence=0.6,
                entry=entry,
                sl=sl,
                tp=tp,
                sl_pct=sl_pct,
                tp_pct=tp_pct,
                bar_ts_used=bar_ts,
                reason="Volatility expansion long: BB squeeze then ATR spike with uptrend",
                meta={
                    "bb_width": float(last["bb_width"]),
                    "ema50_slope": ema50_slope,
                    "ema200": ema200,
                    "atr14": atr14,
                },
            )
        )

    # SHORT: price below EMA200 and EMA50 sloping down.
    if entry < ema200 and ema50_slope < 0:
        sl = high
        risk = abs(sl - entry)
        tp = entry - 2.0 * risk
        sl_pct = risk / max(entry, 1e-10) * 100.0
        tp_pct = 2.0 * sl_pct
        intents.append(
            Intent(
                symbol=symbol,
                tf=tf,
                side="SHORT",
                setup="S4_VOL_EXPANSION",
                confidence=0.6,
                entry=entry,
                sl=sl,
                tp=tp,
                sl_pct=sl_pct,
                tp_pct=tp_pct,
                bar_ts_used=bar_ts,
                reason="Volatility expansion short: BB squeeze then ATR spike with downtrend",
                meta={
                    "bb_width": float(last["bb_width"]),
                    "ema50_slope": ema50_slope,
                    "ema200": ema200,
                    "atr14": atr14,
                },
            )
        )

    return intents

