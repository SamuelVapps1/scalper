"""
Multi-Timeframe (MTF) candle + indicator pipeline: 4H/1H/15m/5m.
EMA20/50/200 and ATR14. In-memory cache with TTL. DRY RUN only.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from indicators import atr_wilder, ema, rsi_wilder

# Cache key: (symbol, interval_min, limit) -> (candles, expiry_ts)
_CACHE: Dict[Tuple[str, int, int], Tuple[List[Dict[str, float]], float]] = {}
_LAST_MTF_FAILURE_REASON: Optional[str] = None
_logger = logging.getLogger(__name__)


def _tf_to_interval(tf_min: int) -> str:
    """Convert TF minutes to Bybit interval string (60, 240, not 60m/240m)."""
    from bybit import _to_bybit_interval
    return _to_bybit_interval(tf_min)


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
    """Fetch candles with in-memory cache. Cache key: (symbol, interval, limit). Returns None on failure."""
    key = (symbol, tf_min, limit)
    now = time.time()
    if key in _CACHE:
        cached, expiry = _CACHE[key]
        if now < expiry:
            try:
                from bybit import record_cache_hit
                record_cache_hit()
            except Exception:
                pass
            return cached
        del _CACHE[key]

    global _LAST_MTF_FAILURE_REASON
    try:
        from bybit import fetch_klines
        from bybit import RateLimitedError as BybitRateLimitedError

        interval = _tf_to_interval(tf_min)
        raw = fetch_klines(symbol=symbol, interval=interval, limit=limit)
        candles = normalize_candles(raw)
        if candles:
            _CACHE[key] = (candles, now + ttl_seconds)
        return candles
    except BybitRateLimitedError:
        _LAST_MTF_FAILURE_REASON = "RATE_LIMITED"
        _logger.debug("MTF fetch rate-limited symbol=%s tf=%s", symbol, tf_min)
        return None
    except Exception as exc:
        _LAST_MTF_FAILURE_REASON = None
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
        "ema200_slope_10": None,
        "atr14": 0.0,
        "rsi14": None,
        "open": 0.0,
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
    result["open"] = float(last.get("open", 0) or 0)
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
    if len(ema200_list) >= 11:
        result["ema200_slope_10"] = ema200_list[-1] - ema200_list[-11]
    else:
        result["ema200_slope_10"] = None
    result["atr14"] = atr_list[-1] if atr_list and atr_list[-1] is not None else 0.0
    if tf_min == 15:
        rsi_list = rsi_wilder(closes, 14)
        result["rsi14"] = rsi_list[-1] if rsi_list and rsi_list[-1] is not None else None

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


def compute_4h_bias(
    symbol: str,
    snapshot_4h: Dict[str, Any],
    failure_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compute STRICT 4H bias from snapshot.
    Returns: {symbol, bias, slope10, dist_pct, close4h, ema2004h, ts, reason}
    bias: "LONG" | "SHORT" | "NONE"
    If failure_reason is set (e.g. RATE_LIMITED), returns NONE with that reason.
    """
    if failure_reason:
        return {
            "symbol": symbol,
            "bias": "NONE",
            "slope10": None,
            "dist_pct": 0.0,
            "close4h": 0.0,
            "ema2004h": 0.0,
            "ts": "",
            "reason": failure_reason,
        }
    ts = str(snapshot_4h.get("ts", "") or "")
    close4h = float(snapshot_4h.get("close", 0.0) or 0.0)
    ema2004h = float(snapshot_4h.get("ema200", 0.0) or 0.0)
    slope_raw = snapshot_4h.get("ema200_slope_10")

    if slope_raw is None:
        return {
            "symbol": symbol,
            "bias": "NONE",
            "slope10": None,
            "dist_pct": 0.0,
            "close4h": close4h,
            "ema2004h": ema2004h,
            "ts": ts,
            "reason": "INSUFFICIENT_4H_DATA",
        }

    slope10 = float(slope_raw)
    dist_pct = ((close4h - ema2004h) / max(ema2004h, 1e-10)) * 100.0 if ema2004h else 0.0

    if close4h > ema2004h and slope10 > 0:
        bias = "LONG"
        reason = ""
    elif close4h < ema2004h and slope10 < 0:
        bias = "SHORT"
        reason = ""
    else:
        bias = "NONE"
        reason = "BIAS_NOT_CLEAR"

    return {
        "symbol": symbol,
        "bias": bias,
        "slope10": slope10,
        "dist_pct": dist_pct,
        "close4h": close4h,
        "ema2004h": ema2004h,
        "ts": ts,
        "reason": reason,
    }


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


def build_mtf_snapshot(symbol: str) -> Tuple[Dict[int, Dict[str, Any]], Optional[str]]:
    """
    Fetch candles for TFs 240/60/15/5 (from env), compute EMA20/50/200 and ATR14.
    Returns (snapshot dict, failure_reason or None). On fetch failure returns ({}, reason).
    """
    global _LAST_MTF_FAILURE_REASON
    _LAST_MTF_FAILURE_REASON = None
    try:
        import config as _config

        tfs = [
            getattr(_config, "TF_BIAS", 240),
            getattr(_config, "TF_SETUP", 60),
            getattr(_config, "TF_TRIGGER", 15),
            getattr(_config, "TF_TIMING", 5),
        ]
        lookbacks = {
            getattr(_config, "TF_BIAS", 240): getattr(_config, "LOOKBACK_4H", 250),
            getattr(_config, "TF_SETUP", 60): getattr(_config, "LOOKBACK_1H", 250),
            getattr(_config, "TF_TRIGGER", 15): getattr(_config, "LOOKBACK_15M", 400),
            getattr(_config, "TF_TIMING", 5): getattr(_config, "LOOKBACK_5M", 400),
        }
        cache_ttl = getattr(_config, "CANDLES_CACHE_TTL_SECONDS", 120)
        snap = get_mtf_snapshot(symbol, tfs, lookbacks, cache_ttl)
        if snap:
            return (snap, None)
        reason = _LAST_MTF_FAILURE_REASON or "FETCH_FAILED"
        return ({}, reason)
    except Exception as exc:
        _logger.debug("MTF build_mtf_snapshot failed symbol=%s: %s", symbol, exc)
        return ({}, "FETCH_FAILED")
