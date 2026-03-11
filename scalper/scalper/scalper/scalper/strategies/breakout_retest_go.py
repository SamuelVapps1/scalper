from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from scalper.scalper_types import Intent


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    S2 Breakout Retest Go:
    - Range highs/lows over LOOKBACK_BARS.
    - Breakout close outside the range.
    - Retest within RETEST_TOL_ATR.
    - Go candle continuation.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    tf = str(ctx.get("tf", "15"))
    settings = ctx.get("settings")
    lookback = int(ctx.get("lookback_bars", ctx.get("RANGE_LOOKBACK_BARS", 80)) or 80)
    tol_atr = float(
        getattr(getattr(settings, "strategy_v3", settings), "pullback_tol_atr", 0.10) or 0.10
    )

    if df.empty or len(df) < lookback + 3:
        return []

    # Use last `lookback` bars to define range.
    window = df.iloc[-(lookback + 3) : -3]
    range_high = float(window["high"].max())
    range_low = float(window["low"].min())

    # The breakout bar and retest bar precede the current bar.
    breakout_bar = df.iloc[-3]
    retest_bar = df.iloc[-2]
    go_bar = df.iloc[-1]

    atr14 = float(go_bar.get("atr14", 0) or 0)
    if atr14 <= 0:
        return []

    intents: List[Intent] = []
    bar_ts = str(go_bar.get("ts", ctx.get("bar_ts_used", "")) or "")

    # LONG breakout: break above range high, retest near high, then continuation up.
    if breakout_bar["close"] > range_high and retest_bar["low"] <= range_high + tol_atr * atr14:
        if go_bar["close"] > breakout_bar["close"]:
            entry = float(go_bar["close"])
            sl = float(min(retest_bar["low"], range_high - tol_atr * atr14))
            risk = abs(entry - sl)
            tp = entry + 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side="LONG",
                    setup="S2_BREAKOUT_RETEST_GO",
                    confidence=0.7,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Breakout above range high, retest near high and continuation",
                    meta={
                        "range_high": range_high,
                        "range_low": range_low,
                        "atr14": float(atr14),
                    },
                )
            )

    # SHORT breakout: break below range low, retest near low, then continuation down.
    if breakout_bar["close"] < range_low and retest_bar["high"] >= range_low - tol_atr * atr14:
        if go_bar["close"] < breakout_bar["close"]:
            entry = float(go_bar["close"])
            sl = float(max(retest_bar["high"], range_low + tol_atr * atr14))
            risk = abs(sl - entry)
            tp = entry - 2.0 * risk
            sl_pct = risk / max(entry, 1e-10) * 100.0
            tp_pct = 2.0 * sl_pct
            intents.append(
                Intent(
                    symbol=symbol,
                    tf=tf,
                    side="SHORT",
                    setup="S2_BREAKOUT_RETEST_GO",
                    confidence=0.7,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    bar_ts_used=bar_ts,
                    reason="Breakout below range low, retest near low and continuation",
                    meta={
                        "range_high": range_high,
                        "range_low": range_low,
                        "atr14": float(atr14),
                    },
                )
            )

    return intents

