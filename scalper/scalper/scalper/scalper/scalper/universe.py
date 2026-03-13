from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import config
from bybit import get_linear_tickers, fetch_klines

_UNIVERSE_CACHE_FILE = "universe_cache.json"
_UNIVERSE_CACHE_MEM: Dict[str, Any] = {
    "symbols": [],
    "mode": "",
    "fetched_at": None,
}


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _load_universe_cache_disk() -> Tuple[List[str], Any]:
    try:
        with open(_UNIVERSE_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        symbols = [str(s).upper() for s in data.get("symbols", []) if str(s).strip()]
        ts_raw = str(data.get("fetched_at", "") or "").strip()
        ts = None
        if ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw)
            except Exception:
                ts = None
        return symbols, ts
    except Exception:
        return [], None


def _save_universe_cache_disk(symbols: List[str], fetched_at: datetime) -> None:
    try:
        payload = {
            "symbols": list(symbols or []),
            "fetched_at": fetched_at.isoformat(),
        }
        with open(_UNIVERSE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True)
    except Exception as exc:
        logging.warning("UNIVERSE cache write failed: %s", exc)


def _build_static_universe() -> List[str]:
    symbols = list(getattr(config, "WATCHLIST", []) or [])
    symbols = [str(s).upper() for s in symbols if str(s).strip()]
    logging.info(
        "UNIVERSE mode=static size=%d symbols=%s",
        len(symbols),
        ",".join(symbols),
    )
    return symbols


def _filter_tickers(rows: List[Dict[str, Any]]) -> Tuple[List[Tuple[str, float]], Dict[str, int]]:
    """Apply basic USDT/turnover/ATR% filters to raw tickers."""
    min_turnover = float(
        getattr(config, "MIN_24H_TURNOVER", getattr(config, "MIN_TURNOVER", 0.0)) or 0.0
    )
    min_atr_pct = float(
        getattr(config, "MIN_ATR_PCT", getattr(config, "MIN_ATR_PCT_15M", 0.0)) or 0.0
    )

    counts: Dict[str, int] = {
        "total": 0,
        "non_usdt": 0,
        "inactive": 0,
        "bad_price": 0,
        "low_turnover": 0,
        "low_atr_pct": 0,
    }
    candidates: List[Tuple[str, float]] = []

    for row in rows:
        counts["total"] += 1
        symbol = str(row.get("symbol", "") or "").upper()
        if not symbol or not symbol.endswith("USDT"):
            counts["non_usdt"] += 1
            continue
        quote = str(row.get("quoteCoin", "USDT") or "").upper()
        if quote != "USDT":
            counts["non_usdt"] += 1
            continue
        status = str(row.get("status", row.get("symbolStatus", "Trading"))).lower()
        if status not in {"trading", "trade", "active"}:
            counts["inactive"] += 1
            continue

        last_price = _to_float(row.get("lastPrice"))
        if last_price <= 0:
            counts["bad_price"] += 1
            continue

        turnover = _to_float(row.get("turnover24h"))
        if turnover <= 0:
            turnover = _to_float(row.get("volume24h"))
        if min_turnover > 0 and turnover < min_turnover:
            counts["low_turnover"] += 1
            continue

        high = _to_float(row.get("highPrice24h"))
        low = _to_float(row.get("lowPrice24h"))
        vol_pct = ((high - low) / last_price * 100.0) if high > low and last_price > 0 else 0.0
        if min_atr_pct > 0 and vol_pct < (min_atr_pct * 100.0):
            counts["low_atr_pct"] += 1
            continue

        candidates.append((symbol, turnover))

    logging.info(
        "UNIVERSE tickers total=%d non_usdt=%d inactive=%d bad_price=%d low_turnover=%d low_atr_pct=%d candidates=%d",
        counts["total"],
        counts["non_usdt"],
        counts["inactive"],
        counts["bad_price"],
        counts["low_turnover"],
        counts["low_atr_pct"],
        len(candidates),
    )
    return candidates, counts


def _validate_with_klines(candidates: List[Tuple[str, float]], universe_size: int) -> Tuple[List[str], Dict[str, int]]:
    """Validate symbols by fetching a small kline sample on INTERVAL; skip empty/error symbols."""
    interval = str(getattr(config, "INTERVAL", "15") or "15")
    max_candidates = min(max(universe_size * 2, universe_size), 500)
    selected: List[str] = []
    counts: Dict[str, int] = {
        "validated": 0,
        "empty_kline": 0,
        "kline_error": 0,
    }
    for symbol, _ in candidates[:max_candidates]:
        if len(selected) >= universe_size:
            break
        try:
            candles = fetch_klines(symbol=symbol, interval=interval, limit=5)
        except Exception as exc:
            counts["kline_error"] += 1
            logging.warning("UNIVERSE kline_error symbol=%s error=%s", symbol, exc)
            continue
        if not candles:
            counts["empty_kline"] += 1
            logging.info("UNIVERSE skip symbol=%s reason=empty_kline", symbol)
            continue
        selected.append(symbol)
        counts["validated"] += 1
    logging.info(
        "UNIVERSE kline_validation validated=%d empty_kline=%d kline_error=%d",
        counts["validated"],
        counts["empty_kline"],
        counts["kline_error"],
    )
    return selected, counts


def _build_dynamic_universe() -> List[str]:
    rows = get_linear_tickers()
    candidates, _ = _filter_tickers(rows)
    candidates.sort(key=lambda x: x[1], reverse=True)
    universe_size = int(getattr(config, "UNIVERSE_SIZE", getattr(config, "UNIVERSE_TOP_N", 200)) or 200)
    validated, _ = _validate_with_klines(candidates, universe_size)
    logging.info(
        "UNIVERSE mode=dynamic final_size=%d universe_size=%d symbols=%s",
        len(validated),
        universe_size,
        ",".join(validated),
    )
    return validated


def get_universe() -> List[str]:
    mode = str(getattr(config, "UNIVERSE_MODE", "static") or "static").strip().lower()
    refresh_hours = int(getattr(config, "UNIVERSE_REFRESH_HOURS", 6) or 6)
    now = datetime.now(timezone.utc)

    if mode != "dynamic":
        return _build_static_universe()

    # Try in-memory cache first
    cached_symbols = _UNIVERSE_CACHE_MEM.get("symbols") or []
    cached_at = _UNIVERSE_CACHE_MEM.get("fetched_at")
    if (
        cached_symbols
        and isinstance(cached_at, datetime)
        and refresh_hours > 0
        and (now - cached_at).total_seconds() < refresh_hours * 3600
    ):
        logging.info(
            "UNIVERSE mode=dynamic (mem cached) size=%d symbols=%s",
            len(cached_symbols),
            ",".join(cached_symbols),
        )
        return list(cached_symbols)

    # Load disk cache if memory cache is empty or stale
    disk_symbols, disk_ts = _load_universe_cache_disk()
    if (
        disk_symbols
        and not cached_symbols
        and disk_ts is not None
        and refresh_hours > 0
        and (now - disk_ts).total_seconds() < refresh_hours * 3600
    ):
        logging.info(
            "UNIVERSE mode=dynamic (disk cached) size=%d symbols=%s",
            len(disk_symbols),
            ",".join(disk_symbols),
        )
        _UNIVERSE_CACHE_MEM["symbols"] = list(disk_symbols)
        _UNIVERSE_CACHE_MEM["fetched_at"] = disk_ts
        _UNIVERSE_CACHE_MEM["mode"] = "dynamic"
        return list(disk_symbols)

    try:
        symbols = _build_dynamic_universe()
        _UNIVERSE_CACHE_MEM["symbols"] = list(symbols)
        _UNIVERSE_CACHE_MEM["mode"] = "dynamic"
        _UNIVERSE_CACHE_MEM["fetched_at"] = now
        _save_universe_cache_disk(symbols, now)
        return symbols
    except Exception as exc:
        logging.exception("UNIVERSE dynamic fetch failed: %s", exc)
        if cached_symbols:
            logging.info(
                "UNIVERSE fallback to mem cached dynamic size=%d",
                len(cached_symbols),
            )
            return list(cached_symbols)
        if disk_symbols:
            logging.info(
                "UNIVERSE fallback to disk cached dynamic size=%d",
                len(disk_symbols),
            )
            _UNIVERSE_CACHE_MEM["symbols"] = list(disk_symbols)
            _UNIVERSE_CACHE_MEM["mode"] = "dynamic"
            _UNIVERSE_CACHE_MEM["fetched_at"] = disk_ts
            return list(disk_symbols)
        logging.info("UNIVERSE falling back to static WATCHLIST")
        return _build_static_universe()


def _load_rotation_state() -> int:
    path = str(getattr(config, "ROTATION_STATE_FILE", "state.json") or "state.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        offset = int(data.get("offset", 0) or 0)
        return max(0, offset)
    except Exception:
        return 0


def _save_rotation_state(offset: int) -> None:
    path = str(getattr(config, "ROTATION_STATE_FILE", "state.json") or "state.json")
    try:
        payload = {"offset": int(offset)}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True)
    except Exception as exc:
        logging.warning("UNIVERSE rotation state write failed: %s", exc)


def get_rotating_batch() -> Tuple[List[str], int]:
    """Return (batch_symbols, offset_before) using persistent rotation state."""
    universe = get_universe()
    if not universe:
        return [], 0
    n = len(universe)
    batch_size = int(getattr(config, "BATCH_SIZE", 20) or getattr(config, "UNIVERSE_TOP_N", 20) or 20)
    effective = min(batch_size, n)
    offset = _load_rotation_state() % n
    batch = [universe[(offset + i) % n] for i in range(effective)]
    next_offset = (offset + effective) % n
    _save_rotation_state(next_offset)
    logging.info(
        "UNIVERSE_ROTATION universe_size=%d batch_size=%d effective=%d offset=%d selected=%s",
        n,
        batch_size,
        effective,
        offset,
        ",".join(batch),
    )
    return batch, offset

