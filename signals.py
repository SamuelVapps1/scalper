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

<<<<<<< HEAD
    compact = evaluated.get("debug_why_none", {}) or {}
    if compact:
        lines.append(f"debug_why_none={compact}")

    if candles_5m:
        lines.append("")
        lines.append(f"Last 12 bars (5m), candles={len(candles_5m)}:")
        start_5 = max(0, len(candles_5m) - 12)
        for i in range(start_5, len(candles_5m)):
            c = candles_5m[i]
            marker = " <- EARLY[5m](-1)" if i == len(candles_5m) - 1 else ""
            lines.append(
                f"- {_candle_ts_utc(c)}{marker} "
                f"O={_fmt_num(float(c.get('open', 0.0)))} "
                f"H={_fmt_num(float(c.get('high', 0.0)))} "
                f"L={_fmt_num(float(c.get('low', 0.0)))} "
                f"C={_fmt_num(float(c.get('close', 0.0)))}"
            )
    return "\n".join(lines)


def evaluate_symbol_intents_with_plugins(
    symbol: str,
    *,
    candles_15m: List[Dict[str, float]],
    candles_5m: Optional[List[Dict[str, float]]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    signal_debug: bool = False,
    timeframe: str = "15",
    bar_ts_used: str = "",
) -> Dict[str, object]:
    """
    Thin compatibility wrapper for plugin architecture.
    Returns legacy evaluate_* dict shape for existing consumers.
    """
    from scalper.settings import get_settings
    from scalper.strategies import (
        evaluate_enabled_first,
        load_enabled_strategies,
        strategy_result_to_evaluated,
    )

    settings = get_settings()
    strategies = load_enabled_strategies(settings)
    ctx: Dict[str, Any] = {
        "candles_15m": candles_15m or [],
        "candles_5m": candles_5m or [],
        "mtf_snapshot": mtf_snapshot or {},
        "bias_info": bias_info or {},
        "bar_ts_used": str(bar_ts_used or ""),
        "signal_debug": bool(signal_debug),
        "timeframe": str(timeframe or "15"),
        "v3_params": {
            "DONCHIAN_N_15M": settings.strategy_v3.donchian_n_15m,
            "BODY_ATR_15M": settings.strategy_v3.body_atr_15m,
            "TREND_SEP_ATR_1H": settings.strategy_v3.trend_sep_atr_1h,
            "USE_5M_CONFIRM": settings.strategy_v3.use_5m_confirm,
        },
        "i15": max(0, len(candles_15m or []) - 1),
        "sl_atr_mult": float(_get_config_float("PULLBACK_SL_ATR_MULT", 0.60)),
        "tp_r": float(_get_config_float("PAPER_TP_ATR", 1.5)),
    }
    result = evaluate_enabled_first(symbol=symbol, context=ctx, strategies=strategies)
    return strategy_result_to_evaluated(result, context=ctx)
=======
    return signals
>>>>>>> 687e22dccb4ca354fd3fb211e4c4c4cb9c7b2313
