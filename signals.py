from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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


def _market_snapshot(symbol: str, candles: List[Dict[str, float]]) -> Dict[str, Any]:
    if not candles:
        return {"symbol": symbol, "ts": "", "bar_ts_used": ""}
    last = candles[-1]
    return {
        "symbol": symbol,
        "ts": str(last.get("timestamp_utc", "") or ""),
        "bar_ts_used": str(last.get("timestamp_utc", "") or ""),
        "close": float(last.get("close", 0.0) or 0.0),
    }


def _build_frame(candles: List[Dict[str, float]]) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    if df.empty:
        return df
    df["ema20"] = _ema(df["close"], 20)
    df["ema50"] = _ema(df["close"], 50)
    df["ema200"] = _ema(df["close"], 200)
    df["rsi14"] = _rsi(df["close"], 14)
    df["atr14"] = _atr(df, 14)

    macd_df = _macd(df["close"])
    df = pd.concat([df, macd_df], axis=1)
    return df


def evaluate_symbol_intents(
    symbol: str,
    candles: List[Dict[str, float]],
    signal_debug: bool = False,
    early_min_conf: float = 0.35,
    threshold_profile: str = "A",
) -> Dict[str, Any]:
    if len(candles) < 210:
        return {
            "final_intents": [],
            "candidates_before": [],
            "early_intents": [],
            "collisions": [],
            "rejections": [],
            "near_miss_candidates": [],
            "debug_why_none": {"reason": "not_enough_candles"},
            "market_snapshot": _market_snapshot(symbol, candles),
            "error": None,
        }

    df = _build_frame(candles)
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    now_iso = str(candles[-1].get("timestamp_utc") or datetime.now(timezone.utc).isoformat())

    candidates: List[Dict[str, Any]] = []
    near_miss: List[Dict[str, Any]] = []

    if (
        latest["close"] > latest["ema20"] > latest["ema50"] > latest["ema200"]
        and 50 <= latest["rsi14"] <= 70
        and latest["macd_line"] > latest["macd_signal"]
        and latest["close"] > prev["close"]
    ):
        candidates.append(
            {
                "ts": now_iso,
                "symbol": symbol,
                "strategy": "TREND_PULLBACK_EMA20",
                "side": "LONG",
                "confidence": 0.62,
                "reason": "Trend continuation: EMA stack + MACD + RSI",
            }
        )
    elif latest["close"] > latest["ema20"] > latest["ema50"]:
        near_miss.append({"symbol": symbol, "strategy": "TREND_PULLBACK_EMA20", "reason": "weak_momentum"})

    recent_range = (latest["high"] - latest["low"]) / max(latest["close"], 1e-10)
    atr_ratio = latest["atr14"] / max(latest["close"], 1e-10)
    if (
        latest["close"] > latest["ema20"] > latest["ema50"]
        and latest["macd_hist"] > 0
        and latest["rsi14"] > 55
        and recent_range > atr_ratio
        and latest["close"] > prev["high"]
    ):
        candidates.append(
            {
                "ts": now_iso,
                "symbol": symbol,
                "strategy": "RANGE_BREAKOUT_RETEST_GO",
                "side": "LONG",
                "confidence": 0.68,
                "reason": "Breakout above previous high with momentum",
            }
        )

    profile_threshold = {"A": 0.45, "B": 0.55, "C": 0.65}.get(str(threshold_profile or "A").upper(), 0.45)
    final_intents = [c for c in candidates if float(c.get("confidence", 0.0)) >= max(profile_threshold, early_min_conf)]
    debug = {"threshold_profile": str(threshold_profile).upper(), "candidates": len(candidates)}
    if not final_intents:
        debug["reason"] = "below_threshold_or_no_setup"
    if signal_debug:
        debug["latest_close"] = float(latest["close"])
        debug["latest_rsi"] = float(latest["rsi14"])

    return {
        "final_intents": final_intents,
        "candidates_before": candidates,
        "early_intents": [],
        "collisions": [],
        "rejections": [],
        "near_miss_candidates": near_miss,
        "debug_why_none": debug,
        "market_snapshot": _market_snapshot(symbol, candles),
        "error": None,
    }


def evaluate_early_intents_from_5m(
    symbol: str,
    candles_5m: List[Dict[str, float]],
    context_15m: Optional[Dict[str, Any]] = None,
    early_min_conf: float = 0.35,
    require_15m_context: bool = True,
) -> List[Dict[str, Any]]:
    if len(candles_5m) < 30:
        return []
    if require_15m_context and not (context_15m or {}).get("candidates_before"):
        return []

    df = _build_frame(candles_5m[-120:])
    if len(df) < 25:
        return []
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    side = None
    conf = 0.0
    if latest["close"] > latest["ema20"] and latest["macd_hist"] > 0 and latest["close"] > prev["high"]:
        side = "LONG"
        conf = 0.42
    elif latest["close"] < latest["ema20"] and latest["macd_hist"] < 0 and latest["close"] < prev["low"]:
        side = "SHORT"
        conf = 0.42
    if side is None or conf < early_min_conf:
        return []

    ts_5m = str(candles_5m[-1].get("timestamp_utc", datetime.now(timezone.utc).isoformat()) or "")
    bar_ts_15m = str(((context_15m or {}).get("market_snapshot") or {}).get("bar_ts_used", "") or "")
    return [
        {
            "symbol": symbol,
            "side": side,
            "strategy": "TREND_PULLBACK_EMA20",
            "confidence": conf,
            "ts": ts_5m,
            "bar_ts_5m": ts_5m,
            "bar_ts_15m": bar_ts_15m,
        }
    ]


def build_reconcile_report(
    symbol: str,
    candles: List[Dict[str, float]],
    candles_5m: Optional[List[Dict[str, float]]] = None,
    threshold_profile: str = "A",
) -> str:
    evaluated = evaluate_symbol_intents(
        symbol=symbol,
        candles=candles,
        signal_debug=True,
        threshold_profile=threshold_profile,
    )
    lines = [
        f"RECON symbol={symbol}",
        f"candles_15m={len(candles)} candles_5m={len(candles_5m or [])}",
        f"threshold_profile={str(threshold_profile).upper()}",
        f"candidates={len(evaluated.get('candidates_before', []) or [])}",
        f"final_intents={len(evaluated.get('final_intents', []) or [])}",
        f"debug={evaluated.get('debug_why_none', {})}",
    ]
    return "\n".join(lines)


def generate_signals(symbol: str, candles: List[Dict[str, float]]) -> List[Dict[str, object]]:
    evaluated = evaluate_symbol_intents(symbol=symbol, candles=candles)
    out: List[Dict[str, object]] = []
    for intent in evaluated.get("final_intents", []) or []:
        out.append(
            {
                "timestamp_utc": str(intent.get("ts", datetime.now(timezone.utc).isoformat())),
                "symbol": symbol,
                "setup": str(intent.get("strategy", "")),
                "direction": str(intent.get("side", "")),
                "close": float((evaluated.get("market_snapshot") or {}).get("close", 0.0)),
                "reason": str(intent.get("reason", "")),
            }
        )
    return out
