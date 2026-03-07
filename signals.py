from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))


def _macd(series: pd.Series) -> pd.DataFrame:
    ema12 = _ema(series, 12)
    ema26 = _ema(series, 26)
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame(
        {"macd_line": macd_line, "macd_signal": signal_line, "macd_hist": hist}
    )


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def generate_signals(symbol: str, candles: List[Dict[str, float]]) -> List[Dict[str, object]]:
    if len(candles) < 210:
        return []

    df = pd.DataFrame(candles)
    df["ema20"] = _ema(df["close"], 20)
    df["ema50"] = _ema(df["close"], 50)
    df["ema200"] = _ema(df["close"], 200)
    df["rsi14"] = _rsi(df["close"], 14)
    df["atr14"] = _atr(df, 14)

    macd_df = _macd(df["close"])
    df = pd.concat([df, macd_df], axis=1)

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    now_iso = datetime.now(timezone.utc).isoformat()

    signals: List[Dict[str, object]] = []

    # Setup 1: EMA trend continuation (bullish)
    if (
        latest["close"] > latest["ema20"] > latest["ema50"] > latest["ema200"]
        and 50 <= latest["rsi14"] <= 70
        and latest["macd_line"] > latest["macd_signal"]
        and latest["close"] > prev["close"]
    ):
        signals.append(
            {
                "timestamp_utc": now_iso,
                "symbol": symbol,
                "setup": "EMA_TREND_CONTINUATION_LONG",
                "direction": "LONG",
                "close": float(latest["close"]),
                "reason": "EMA20>EMA50>EMA200, RSI 50-70, MACD bullish.",
            }
        )

    # Setup 2: ATR breakout with momentum confirmation (bullish)
    recent_range = (latest["high"] - latest["low"]) / max(latest["close"], 1e-10)
    atr_ratio = latest["atr14"] / max(latest["close"], 1e-10)
    if (
        latest["close"] > latest["ema20"] > latest["ema50"]
        and latest["macd_hist"] > 0
        and latest["rsi14"] > 55
        and recent_range > atr_ratio
        and latest["close"] > prev["high"]
    ):
        signals.append(
            {
                "timestamp_utc": now_iso,
                "symbol": symbol,
                "setup": "ATR_BREAKOUT_MOMENTUM_LONG",
                "direction": "LONG",
                "close": float(latest["close"]),
                "reason": "Breakout above prev high with ATR and momentum confirmation.",
            }
        )

    return signals
