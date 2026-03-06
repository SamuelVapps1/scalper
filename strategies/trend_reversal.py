from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from scalper.types import Intent


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    S3 Trend Reversal (simplified initial version):
    - Impulse move over IMPULSE_BARS with large percent change.
    - Pullback/bounce into EMA50.
    - Compression: smaller candle ranges vs impulse.
    - Rejection wick at local support/resistance.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    tf = str(ctx.get("tf", "15"))
    settings = ctx.get("settings")

    impulse_bars = int(ctx.get("impulse_bars", 8) or 8)
    impulse_pct = float(ctx.get("impulse_pct", 3.0) or 3.0)

    if df.empty or len(df) < impulse_bars + 5:
        return []

    # Use recent window for impulse detection.
    window = df.iloc[-(impulse_bars + 5) : -5]
    first_close = float(window["close"].iloc[0])
    last_close = float(window["close"].iloc[-1])
    if first_close <= 0:
        return []

    change_pct = (last_close - first_close) / first_close * 100.0
    up_impulse = change_pct >= impulse_pct
    down_impulse = change_pct <= -impulse_pct
    if not (up_impulse or down_impulse):
        return []

    # Pullback/compression region just before current bar.
    pullback = df.iloc[-5:-1]
    go_bar = df.iloc[-1]
    ema50 = float(go_bar.get("ema50", 0) or 0)
    ema200 = float(go_bar.get("ema200", 0) or 0)
    atr14 = float(go_bar.get("atr14", 0) or 0)
    if ema50 <= 0 or ema200 <= 0 or atr14 <= 0:
        return []

    bar_ts = str(go_bar.get("ts", ctx.get("bar_ts_used", "")) or "")
    intents: List[Intent] = []

    # Compute average range in impulse vs pullback window.
    impulse_range = (window["high"] - window["low"]).mean()
    pullback_range = (pullback["high"] - pullback["low"]).mean()
    compression_ok = pullback_range < impulse_range * 0.6

    # Use last pullback bar as rejection bar.
    rej = pullback.iloc[-1]
    rej_high = float(rej["high"])
    rej_low = float(rej["low"])
    rej_close = float(rej["close"])

    # LONG reversal after down impulse.
    if down_impulse and compression_ok:
        near_ema50 = abs(rej_close - ema50) <= 0.5 * atr14
        long_wick = (rej_low < ema50) and (rej_close > ema50) and ((rej_close - rej_low) > 0.6 * (rej_high - rej_low))
        if near_ema50 and long_wick and go_bar["close"] > rej_high:
            entry = float(go_bar["close"])
            sl = float(min(rej_low, ema200 - 0.5 * atr14))
            risk = abs(entry - sl)
            tp = entry + 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side="LONG",
                    setup="S3_TREND_REVERSAL",
                    confidence=0.6,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Trend reversal long: down impulse, compression, bullish rejection at EMA50",
                    meta={
                        "impulse_pct": change_pct,
                        "ema50": ema50,
                        "ema200": ema200,
                        "atr14": atr14,
                    },
                )
            )

    # SHORT reversal after up impulse.
    if up_impulse and compression_ok:
        near_ema50 = abs(rej_close - ema50) <= 0.5 * atr14
        short_wick = (rej_high > ema50) and (rej_close < ema50) and ((rej_high - rej_close) > 0.6 * (rej_high - rej_low))
        if near_ema50 and short_wick and go_bar["close"] < rej_low:
            entry = float(go_bar["close"])
            sl = float(max(rej_high, ema200 + 0.5 * atr14))
            risk = abs(sl - entry)
            tp = entry - 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side="SHORT",
                    setup="S3_TREND_REVERSAL",
                    confidence=0.6,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Trend reversal short: up impulse, compression, bearish rejection at EMA50",
                    meta={
                        "impulse_pct": change_pct,
                        "ema50": ema50,
                        "ema200": ema200,
                        "atr14": atr14,
                    },
                )
            )

    return intents

