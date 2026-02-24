"""
Multi-Timeframe (MTF) candle + indicator pipeline: 4H/1H/15m/5m.
EMA20/50/200 and ATR14. In-memory cache with TTL. DRY RUN only.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from indicators import atr_wilder, ema

_CACHE: Dict[Tuple[str, int], Tuple[List[Dict[str, float]], float]] = {}
_logger = logging.getLogger(__name__)


def _tf_to_interval(tf_min: int) -> str:
    """Convert TF minutes to Bybit interval string."""
    return str(tf_min)


def _normalize_candle(item: Any) -> Optional[Dict[str, float]]:
    """Ensure candle has timestamp_utc, open, high, low, close (floats)."""
    try:
        if isinstance(item, dict):
            ts_raw = item.get("timestamp") or item.get("timestamp_utc")
            if ts_raw is None:
                return None
            if isinstance(ts_raw, (int, float)):
                ts_ms = int(ts_raw)
                ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
            else:
                ts_utc = str(ts_raw)
            return {
                "timestamp_utc": ts_utc,
                "open": float(item.get("open", 0) or 0),
                "high": float(item.get("high", 0) or 0),
                "low": float(item.get("low", 0) or 0),
                "close": float(item.get("close", 0) or 0),
            }
        if isinstance(item, (list, tuple)) and len(item) >= 5:
            ts_ms = int(item[0])
            ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
            return {
                "timestamp_utc": ts_utc,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
            }
    except (TypeError, ValueError, IndexError):
        pass
    return None


def normalize_candles(raw: List[Any]) -> List[Dict[str, float]]:
    """Normalize candles: timestamp_utc, open, high, low, close (floats), ascending time."""
    out: List[Dict[str, float]] = []
    for item in raw:
        c = _normalize_candle(item)
        if c:
            out.append(c)
    if not out:
        return []
    out.sort(key=lambda x: x["timestamp_utc"])
    return out


def _fetch_candles_cached(
    symbol: str,
    tf_min: int,
    limit: int,
    ttl_seconds: int,
) -> Optional[List[Dict[str, float]]]:
    """Fetch candles with in-memory cache. Returns None on failure."""
    key = (symbol, tf_min)
    now = time.time()
    if key in _CACHE:
        cached, expiry = _CACHE[key]
        if now < expiry:
            return cached
        del _CACHE[key]

    try:
        from bybit import fetch_klines

        interval = _tf_to_interval(tf_min)
        raw = fetch_klines(symbol=symbol, interval=interval, limit=limit)
        candles = normalize_candles(raw)
        if candles:
            _CACHE[key] = (candles, now + ttl_seconds)
        return candles
    except Exception as exc:
        _logger.debug("MTF fetch failed symbol=%s tf=%s: %s", symbol, tf_min, exc)
        return None


def _compute_snapshot_for_tf(
    candles: List[Dict[str, float]],
    tf_min: int,
) -> Dict[str, Any]:
    """Compute ema20/50/200, atr14, close, high, low, ts for one TF."""
    result: Dict[str, Any] = {
        "ema20": 0.0,
        "ema50": 0.0,
        "ema200": 0.0,
        "atr14": 0.0,
        "close": 0.0,
        "high": 0.0,
        "low": 0.0,
        "ts": "",
    }
    if not candles:
        return result

    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]

    last = candles[-1]
    result["close"] = float(last["close"])
    result["high"] = float(last["high"])
    result["low"] = float(last["low"])
    result["ts"] = str(last.get("timestamp_utc", ""))

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema200_list = ema(closes, 200)
    atr_list = atr_wilder(highs, lows, closes, 14)

    result["ema20"] = ema20_list[-1] if ema20_list else 0.0
    result["ema50"] = ema50_list[-1] if ema50_list else 0.0
    result["ema200"] = ema200_list[-1] if ema200_list else 0.0
    result["atr14"] = atr_list[-1] if atr_list and atr_list[-1] is not None else 0.0

    return result


def get_mtf_snapshot(
    symbol: str,
    tfs: List[int],
    lookbacks: Dict[int, int],
    cache_ttl_seconds: int,
) -> Optional[Dict[int, Dict[str, Any]]]:
    """
    Fetch candles for all TFs, compute indicators, return snapshot per TF.
    If any TF fetch fails, return None (fail-safe: caller may skip symbol).
    """
    snapshot: Dict[int, Dict[str, Any]] = {}
    for tf_min in tfs:
        limit = lookbacks.get(tf_min, 300)
        candles = _fetch_candles_cached(symbol, tf_min, limit, cache_ttl_seconds)
        if not candles:
            return None
        snapshot[tf_min] = _compute_snapshot_for_tf(candles, tf_min)
    return snapshot


def _tf_label(tf_min: int) -> str:
    if tf_min >= 60:
        return f"{tf_min // 60}H" if tf_min % 60 == 0 else f"{tf_min}m"
    return f"{tf_min}m"


def log_mtf_ready(symbol: str, snapshot: Dict[int, Dict[str, Any]], logger: Optional[logging.Logger] = None) -> None:
    """Debug log: one line per symbol, MTF_READY with ema200/atr/close per TF."""
    log = logger or _logger
    if not log.isEnabledFor(logging.DEBUG):
        return
    parts: List[str] = []
    for tf_min in sorted(snapshot.keys(), reverse=True):
        s = snapshot[tf_min]
        label = _tf_label(tf_min)
        parts.append(f"{label} ema200={s['ema200']:.4f} atr={s['atr14']:.6f} close={s['close']:.4f}")
    msg = "MTF_READY symbol=%s %s" % (symbol, " | ".join(parts))
    log.debug(msg)


def build_mtf_snapshot(symbol: str) -> Dict[int, Dict[str, Any]]:
    """
    Fetch candles for TFs 240/60/15/5 (from env), compute EMA20/50/200 and ATR14.
    Returns snapshot dict; on any fetch failure returns empty dict (guaranteed to run).
    """
    try:
        import config as _config

        tfs = [
            getattr(_config, "TF_BIAS", 240),
            getattr(_config, "TF_SETUP", 60),
            getattr(_config, "TF_TRIGGER", 15),
            getattr(_config, "TF_TIMING", 5),
        ]
        lookbacks = {
            getattr(_config, "TF_BIAS", 240): getattr(_config, "LOOKBACK_4H", 300),
            getattr(_config, "TF_SETUP", 60): getattr(_config, "LOOKBACK_1H", 300),
            getattr(_config, "TF_TRIGGER", 15): getattr(_config, "LOOKBACK_15M", 500),
            getattr(_config, "TF_TIMING", 5): getattr(_config, "LOOKBACK_5M", 800),
        }
        cache_ttl = getattr(_config, "CANDLES_CACHE_TTL_SECONDS", 20)
        snap = get_mtf_snapshot(symbol, tfs, lookbacks, cache_ttl)
        return snap if snap else {}
    except Exception as exc:
        _logger.debug("MTF build_mtf_snapshot failed symbol=%s: %s", symbol, exc)
        return {}
