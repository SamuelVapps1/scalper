"""
Local CSV cache for Bybit candles. Used by replay to avoid repeated API calls.
Root: data/candles/{symbol}/{tf}.csv
Columns: ts, open, high, low, close, volume

get_candles(symbol, tf, start_ms, end_ms, use_cache=True) -> list[candle]
Range-based loader with pagination. Candle = {ts, open, high, low, close, volume}.
"""
from __future__ import annotations

import csv
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

CSV_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _candle_to_row(c: Dict[str, Any]) -> Dict[str, Any]:
    ts = c.get("timestamp") or c.get("timestamp_utc")
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            ts_ms = int(dt.timestamp() * 1000)
        except ValueError:
            return {}
    else:
        ts_ms = int(ts) if ts else 0
    return {
        "ts": ts_ms,
        "open": float(c.get("open", 0) or 0),
        "high": float(c.get("high", 0) or 0),
        "low": float(c.get("low", 0) or 0),
        "close": float(c.get("close", 0) or 0),
        "volume": float(c.get("volume", 0) or 0),
    }


def _row_to_candle(row: Dict[str, Any]) -> Dict[str, Any]:
    ts_ms = int(row.get("ts", 0) or 0)
    ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
    return {
        "timestamp": ts_ms,
        "timestamp_utc": ts_utc,
        "open": float(row.get("open", 0) or 0),
        "high": float(row.get("high", 0) or 0),
        "low": float(row.get("low", 0) or 0),
        "close": float(row.get("close", 0) or 0),
        "volume": float(row.get("volume", 0) or 0),
    }


def _cache_path(symbol: str, tf_min: int) -> Path:
    root = Path.cwd() / "data" / "candles"
    return root / symbol.upper() / f"{tf_min}.csv"


def _load_cache_file(path: Path) -> List[Dict[str, Any]]:
    """Load candles from CSV. Returns [] if file missing or empty."""
    if not path.exists():
        return []
    candles: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("ts"):
                    candles.append(_row_to_candle(row))
    except (OSError, csv.Error) as e:
        _log.debug("Cache read failed %s: %s", path, e)
        return []
    candles.sort(key=lambda c: c["timestamp"])
    return candles


def _save_cache_file(path: Path, candles: List[Dict[str, Any]], cache_days: int, end_ms: int) -> None:
    """Save candles to CSV. Trims to last cache_days from end_ms. Never write empty cache."""
    if not candles or len(candles) < 5:
        return
    min_ts = end_ms - cache_days * 24 * 3600 * 1000
    trimmed = [c for c in candles if c["timestamp"] >= min_ts]
    trimmed.sort(key=lambda c: c["timestamp"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for c in trimmed:
            w.writerow(_candle_to_row(c))


def fetch_klines_cached(
    symbol: str,
    tf_min: int,
    start_ms: int,
    end_ms: int,
    fetch_fn: Callable[[str, int, int, int], List[Dict[str, Any]]],
    *,
    use_cache: bool = True,
    cache_days: int = 365,
    cache_hits: Optional[Dict[str, int]] = None,
    cache_misses: Optional[Dict[str, int]] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Fetch candles with optional CSV cache.
    Returns (candles, from_cache).
    cache_hits/cache_misses: mutable dicts to increment; key = "symbol_tf" e.g. "BTCUSDT_15".
    """
    key = f"{symbol}_{tf_min}"
    path = _cache_path(symbol, tf_min)

    if use_cache and path.exists():
        existing = _load_cache_file(path)
        if existing and len(existing) >= 5:
            min_ts = existing[0]["timestamp"]
            max_ts = existing[-1]["timestamp"]
            bar_ms = tf_min * 60 * 1000
            last_bar_needed = (end_ms // bar_ms) * bar_ms
            hit = min_ts <= start_ms and max_ts >= last_bar_needed
            if hit:
                out = [c for c in existing if start_ms <= c["timestamp"] <= end_ms]
                if cache_hits is not None:
                    cache_hits[key] = cache_hits.get(key, 0) + 1
                return (out, True)

    raw = fetch_fn(symbol, tf_min, start_ms, end_ms)
    existing = _load_cache_file(path) if use_cache and path.exists() else []
    by_ts: Dict[int, Dict[str, Any]] = {c["timestamp"]: c for c in existing}
    for c in raw:
        by_ts[c["timestamp"]] = c
    merged = sorted(by_ts.values(), key=lambda x: x["timestamp"])
    if use_cache and merged and len(merged) >= 5:
        _save_cache_file(path, merged, cache_days, end_ms)
    out = [c for c in merged if start_ms <= c["timestamp"] <= end_ms]
    if cache_misses is not None:
        cache_misses[key] = cache_misses.get(key, 0) + 1
    return (out, False)


def _tf_to_min(tf: str | int) -> int:
    """Convert tf to minutes (e.g. '15m' -> 15, 60 -> 60)."""
    if isinstance(tf, int):
        return tf
    s = str(tf).strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) if s[:-1].isdigit() else 15
    if s.endswith("h"):
        return int(s[:-1]) * 60 if s[:-1].isdigit() else 60
    return int(s) if s.isdigit() else 15


def _to_api_interval(tf_min: int) -> str:
    """Unify API interval mapping (e.g. 5m/15m/60m/240m -> 5/15/60/240)."""
    from bybit import _to_bybit_interval

    return _to_bybit_interval(tf_min)


def _align_range(start_ms: int, end_ms: int, tf_min: int) -> Tuple[int, int]:
    bar_ms = tf_min * 60 * 1000
    return ((start_ms // bar_ms) * bar_ms, (end_ms // bar_ms) * bar_ms)


def _merge_dedupe_sort(existing: List[Dict[str, Any]], fetched: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ts: Dict[int, Dict[str, Any]] = {int(c["timestamp"]): c for c in existing}
    for c in fetched:
        by_ts[int(c["timestamp"])] = c
    return sorted(by_ts.values(), key=lambda x: x["timestamp"])


def _fetch_segment_from_api(
    symbol: str,
    tf_min: int,
    seg_start_ms: int,
    seg_end_ms: int,
    *,
    limit: int = 1000,
    pace_ms: int = 300,
) -> List[Dict[str, Any]]:
    """
    Fetch all candles in [seg_start_ms, seg_end_ms] from Bybit with pagination.
    Returns oldest-first, deduplicated by ts.
    """
    from bybit import fetch_klines

    all_candles: List[Dict[str, Any]] = []
    current_end = seg_end_ms
    while current_end >= seg_start_ms:
        time.sleep(pace_ms / 1000.0)
        raw = fetch_klines(
            symbol=symbol,
            interval=_to_api_interval(tf_min),
            limit=limit,
            start_ms=seg_start_ms,
            end_ms=current_end,
        )
        if not raw:
            break
        for c in raw:
            ts = int(c.get("timestamp", 0) or 0)
            if seg_start_ms <= ts <= seg_end_ms:
                all_candles.append(c)
        if len(raw) < limit:
            break
        oldest_ts = min(int(c.get("timestamp", 0) or 0) for c in raw)
        current_end = oldest_ts - 1
    by_ts: Dict[int, Dict[str, Any]] = {c["timestamp"]: c for c in all_candles}
    return sorted(by_ts.values(), key=lambda x: x["timestamp"])


def get_candles(
    symbol: str,
    tf: str | int,
    start_ms: int,
    end_ms: int,
    *,
    use_cache: bool = True,
    pace_ms: int = 300,
    cache_days: int = 365,
    cache_only: bool = False,
    cache_hits: Optional[Dict[str, int]] = None,
    cache_misses: Optional[Dict[str, int]] = None,
    _timing_out: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Range-based candle loader with pagination.
    Returns list of candles {ts, open, high, low, close, volume} (ts = timestamp ms).
    Cache: data/candles/{symbol}/{tf}.csv
    """
    tf_min = _tf_to_min(tf)
    path = _cache_path(symbol, tf_min)
    key = f"{symbol}_{tf_min}"
    bar_ms = tf_min * 60 * 1000
    aligned_start, aligned_end = _align_range(start_ms, end_ms, tf_min)

    cached: List[Dict[str, Any]] = []
    if use_cache and path.exists():
        cached = _load_cache_file(path)
    min_ts = cached[0]["timestamp"] if cached else None
    max_ts = cached[-1]["timestamp"] if cached else None

    if cache_only:
        if min_ts is None or max_ts is None:
            raise RuntimeError(f"cache_only: no cache for {symbol} tf={tf_min} (path={path})")
        cache_min, cache_max = int(min_ts), int(max_ts)
        tf_ms = bar_ms
        max_gap_bars = int(os.getenv("CACHE_ONLY_GAP_BARS_MAX", "12"))
        max_trunc_pct = float(os.getenv("CACHE_ONLY_MAX_TRUNC_PCT", "0.05"))
        allow_trunc = _env_bool("CACHE_ONLY_ALLOW_TRUNCATION", True)

        req_bars = int((aligned_end - aligned_start) // tf_ms) + 1
        missing_start = 0
        if cache_min > aligned_start:
            missing_start = int(math.ceil((cache_min - aligned_start) / tf_ms))
        missing_end = 0
        if cache_max < aligned_end:
            missing_end = int(math.ceil((aligned_end - cache_max) / tf_ms))
        total_missing = missing_start + missing_end
        missing_pct = (total_missing / req_bars) if req_bars > 0 else 1.0

        if (missing_start > max_gap_bars or missing_end > max_gap_bars) and (
            (not allow_trunc) or missing_pct > max_trunc_pct
        ):
            raise RuntimeError(
                f"cache_only: cache for {symbol} tf={tf} missing too much data "
                f"(start_gap_bars={missing_start}, end_gap_bars={missing_end}, missing_pct={missing_pct:.3%}). "
                f"Cache has [{cache_min}..{cache_max}], requested [{aligned_start}..{aligned_end}]"
            )

        old_start, old_end = aligned_start, aligned_end
        if cache_min > aligned_start:
            aligned_start = cache_min
        if cache_max < aligned_end:
            aligned_end = cache_max
        if old_start != aligned_start or old_end != aligned_end:
            _log.info(
                "CACHE_ONLY_TRUNCATED symbol=%s tf=%s missing_start_bars=%d missing_end_bars=%d missing_pct=%.3f%% "
                "new_range=[%d..%d] old_range=[%d..%d]",
                symbol,
                tf,
                missing_start,
                missing_end,
                missing_pct * 100.0,
                aligned_start,
                aligned_end,
                old_start,
                old_end,
            )

        out = [_ensure_ts_key(c) for c in cached if aligned_start <= c["timestamp"] <= aligned_end]
        if cache_hits is not None:
            cache_hits[key] = cache_hits.get(key, 0) + 1
        if _timing_out is not None:
            _timing_out["source"] = "cache"
        _log.info("CANDLES tf=%s range=%d..%d bars=%d source=cache", tf_min, aligned_start, aligned_end, len(out))
        _sanity_check(tf_min, aligned_start, aligned_end, len(out), "cache")
        return out

    # ensure_range engine: backfill + forward-fill with pagination.
    segments: List[Tuple[int, int, str]] = []
    if min_ts is None or max_ts is None:
        segments.append((aligned_start, aligned_end, "FULL_RANGE"))
    else:
        if min_ts > aligned_start:
            segments.append((aligned_start, min(min_ts - bar_ms, aligned_end), "BACKFILL_START"))
        if max_ts < aligned_end:
            segments.append((max(max_ts + bar_ms, aligned_start), aligned_end, "FILL_END"))

    fetched: List[Dict[str, Any]] = []
    for seg_start, seg_end, seg_kind in segments:
        if seg_start > seg_end:
            continue
        _log.info(
            "%s symbol=%s tf=%s request=%d..%d cache=[%s..%s]",
            seg_kind,
            symbol,
            tf_min,
            seg_start,
            seg_end,
            str(min_ts),
            str(max_ts),
        )
        seg = _fetch_segment_from_api(symbol, tf_min, seg_start, seg_end, limit=1000, pace_ms=pace_ms)
        if not seg:
            raise RuntimeError(
                f"ensure_range missing segment with empty API response: "
                f"symbol={symbol} tf={tf_min} requested=[{seg_start}..{seg_end}] "
                f"existing_cache=[{min_ts}..{max_ts}] kind={seg_kind}"
            )
        _log.info(
            "%s symbol=%s tf=%s fetched_bars=%d request=%d..%d",
            seg_kind,
            symbol,
            tf_min,
            len(seg),
            seg_start,
            seg_end,
        )
        fetched.extend(seg)

    if fetched:
        merged = _merge_dedupe_sort(cached, fetched)
        if use_cache and len(merged) >= 5:
            _save_cache_file(path, merged, cache_days, aligned_end)
    else:
        merged = list(cached)

    out = [_ensure_ts_key(c) for c in merged if aligned_start <= c["timestamp"] <= aligned_end]

    covered_from_cache = bool(cached) and not segments
    if covered_from_cache:
        source = "cache"
        if cache_hits is not None:
            cache_hits[key] = cache_hits.get(key, 0) + 1
    elif cached and fetched:
        source = "mixed"
    else:
        source = "api"
    if fetched and cache_misses is not None:
        cache_misses[key] = cache_misses.get(key, 0) + 1
    if _timing_out is not None:
        _timing_out["source"] = source
    _log.info("CANDLES tf=%s range=%d..%d bars=%d source=%s", tf_min, aligned_start, aligned_end, len(out), source)
    _sanity_check(tf_min, aligned_start, aligned_end, len(out), source)
    return out


def _ensure_ts_key(c: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure candle has 'ts' key (alias for timestamp)."""
    out = dict(c)
    if "ts" not in out and "timestamp" in out:
        out["ts"] = out["timestamp"]
    return out


def _sanity_check(tf_min: int, start_ms: int, end_ms: int, returned: int, source: str) -> None:
    """Log WARNING if returned bars < 70% of expected."""
    bar_ms = tf_min * 60 * 1000
    range_ms = max(1, end_ms - start_ms)
    expected = int(range_ms / bar_ms)
    if expected > 0 and returned < 0.7 * expected:
        _log.warning(
            "CANDLES sanity: tf=%s expected~%d returned=%d (%.1f%%) source=%s",
            tf_min, expected, returned, 100.0 * returned / expected if expected else 0, source,
        )
