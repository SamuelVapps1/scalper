from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from scalper.scalper_types import Intent


def _latest_row(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {}
    row = df.iloc[-1]
    return row.to_dict()


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    S1 Trend Pullback:
    - EMA50 vs EMA200 trend filter.
    - Pullback near EMA20/EMA50 within PULLBACK_TOL_ATR.
    - Trigger with reclaim/lose EMA20 + wick candle.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    tf = str(ctx.get("tf", "15"))
    settings = ctx.get("settings")
    tol_atr = float(getattr(getattr(settings, "strategy_v3", settings), "pullback_tol_atr", 0.10) or 0.10)

    if df.empty or len(df) < 30:
        return []

    d = _latest_row(df)
    close = float(d.get("close", 0) or 0)
    ema20 = float(d.get("ema20", 0) or 0)
    ema50 = float(d.get("ema50", 0) or 0)
    ema200 = float(d.get("ema200", 0) or 0)
    atr14 = float(d.get("atr14", 0) or 0)
    high = float(d.get("high", 0) or 0)
    low = float(d.get("low", 0) or 0)
    bar_ts = str(d.get("ts", ctx.get("bar_ts_used", "")) or "")

    if close <= 0 or ema50 <= 0 or ema200 <= 0 or atr14 <= 0:
        return []

    intents: List[Intent] = []

    # Determine trend by EMA50 vs EMA200.
    if ema50 > ema200 * 1.001:
        side = "LONG"
        pullback_ok = abs(close - ema20) <= tol_atr * atr14 or abs(close - ema50) <= tol_atr * atr14
        wick_ok = (high - max(close, ema20)) >= 0.2 * atr14
        if pullback_ok and wick_ok and close > ema20:
            entry = close
            sl = min(low, ema50 - tol_atr * atr14)
            risk = abs(entry - sl)
            tp = entry + 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side=side,
                    setup="S1_TREND_PULLBACK",
                    confidence=0.65,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Trend pullback: EMA50>EMA200, pullback near EMA20/50 with reclaim",
                    meta={
                        "ema20": ema20,
                        "ema50": ema50,
                        "ema200": ema200,
                        "atr14": atr14,
                    },
                )
            )
    elif ema50 < ema200 * 0.999:
        side = "SHORT"
        pullback_ok = abs(close - ema20) <= tol_atr * atr14 or abs(close - ema50) <= tol_atr * atr14
        wick_ok = (min(close, ema20) - low) >= 0.2 * atr14
        if pullback_ok and wick_ok and close < ema20:
            entry = close
            sl = max(high, ema50 + tol_atr * atr14)
            risk = abs(sl - entry)
            tp = entry - 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side=side,
                    setup="S1_TREND_PULLBACK",
                    confidence=0.65,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Trend pullback: EMA50<EMA200, pullback near EMA20/50 with loss",
                    meta={
                        "ema20": ema20,
                        "ema50": ema50,
                        "ema200": ema200,
                        "atr14": atr14,
                    },
                )
            )

    return intents

