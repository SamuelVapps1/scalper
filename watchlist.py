"""
Dynamic Top10 Watchlist selection using Bybit public tickers (DRY RUN only).
No secrets; falls back to static WATCHLIST if Bybit public call fails.
"""
from __future__ import annotations
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

_CACHE: Dict[str, Any] = {
    "symbols": [],
    "mode": "static",
    "fetched_at_utc": None,
}


def _to_float(raw: Any) -> float:
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _log_and_persist_watchlist(
    log: logging.Logger,
    source: str,
    symbols: List[str],
    *,
    candidates_count: Optional[int] = None,
    cached_until_ts: Optional[int] = None,
) -> None:
    """Log one line and persist to storage kv. No secrets."""
    syms = list(symbols or [])
    cand = candidates_count if candidates_count is not None else len(syms)
    log.info("WATCHLIST source=%s candidates=%d selected=%s", source, cand, ",".join(syms))
    from storage import set_watchlist_transparency
    set_watchlist_transparency(source, syms, candidates_count=candidates_count, cached_until_ts=cached_until_ts)


def get_watchlist(
    config: Any,
    bybit_client: Optional[Any] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[str], str]:
    """
    Resolve watchlist based on WATCHLIST_MODE.
    Returns (symbols, mode) where mode is one of: static, dynamic, static_fallback, dynamic_cached_fallback.
    """
    log = logger or logging.getLogger(__name__)
    mode = str(getattr(config, "WATCHLIST_MODE", "static")).strip().lower()
    static_watchlist = list(getattr(config, "WATCHLIST", []) or [])

    if mode == "static":
        _log_and_persist_watchlist(log, "static", static_watchlist, candidates_count=len(static_watchlist))
        return static_watchlist, "static"

    if mode != "dynamic":
        # Legacy "topn" or unknown: treat as static
        if mode == "topn":
            # topn uses existing bybit.get_top_linear_usdt_symbols (no vol filter)
            from bybit import get_top_linear_usdt_symbols
            try:
                top_n = int(getattr(config, "WATCHLIST_TOP_N", 10))
                refresh_min = int(getattr(config, "WATCHLIST_REFRESH_MIN", getattr(config, "WATCHLIST_REFRESH_MINUTES", 60)))
                now = datetime.now(timezone.utc)
                cached = _CACHE.get("symbols") or []
                cached_at = _CACHE.get("fetched_at_utc")
                cache_mode = _CACHE.get("mode", "")
                if (
                    cached
                    and isinstance(cached_at, datetime)
                    and cache_mode == "topn"
                    and now - cached_at < timedelta(minutes=max(1, refresh_min))
                ):
                    _log_and_persist_watchlist(log, "cached", cached)
                    return list(cached), "topn"
                resolved = get_top_linear_usdt_symbols(
                    top_n,
                    min_price=float(getattr(config, "WATCHLIST_MIN_PRICE", 0.01)),
                    min_turnover_24h=float(getattr(config, "WATCHLIST_MIN_TURNOVER_24H", 0.0)),
                    exclude_prefixes=list(getattr(config, "WATCHLIST_EXCLUDE_PREFIXES", [])),
                    exclude_symbols=list(getattr(config, "WATCHLIST_EXCLUDE_SYMBOLS", [])),
                    exclude_regex=str(getattr(config, "WATCHLIST_EXCLUDE_REGEX", "") or ""),
                    max_spread_bps=float(getattr(config, "WATCHLIST_MAX_SPREAD_BPS", 0.0)),
                )
                if resolved:
                    _CACHE["symbols"] = list(resolved)
                    _CACHE["mode"] = "topn"
                    _CACHE["fetched_at_utc"] = now
                    cached_until = int(now.timestamp()) + max(1, refresh_min) * 60
                    _log_and_persist_watchlist(log, "dynamic", resolved, candidates_count=len(resolved), cached_until_ts=cached_until)
                    return list(resolved), "topn"
            except Exception as exc:
                log.warning("Fallback to static WATCHLIST (topn failed): %s", exc)
        _log_and_persist_watchlist(log, "fallback_static", static_watchlist, candidates_count=len(static_watchlist))
        return static_watchlist, "static"

    # Dynamic mode: turnover + volatility filter
    top_n = int(getattr(config, "WATCHLIST_TOP_N", 10))
    refresh_min = int(getattr(config, "WATCHLIST_REFRESH_MIN", getattr(config, "WATCHLIST_REFRESH_MINUTES", 60)))
    min_turnover = float(getattr(config, "MIN_TURNOVER_USDT", getattr(config, "WATCHLIST_MIN_TURNOVER_24H", 50_000_000)))
    min_vol_pct = float(getattr(config, "MIN_VOL_PCT", 0.8))
    max_vol_pct = float(getattr(config, "MAX_VOL_PCT", 8.0))
    min_price = float(getattr(config, "WATCHLIST_MIN_PRICE", 0.01))
    exclude_prefixes = tuple(str(p).upper() for p in (getattr(config, "WATCHLIST_EXCLUDE_PREFIXES", []) or []))
    exclude_symbols = {str(s).upper() for s in (getattr(config, "WATCHLIST_EXCLUDE_SYMBOLS", []) or [])}
    exclude_regex = str(getattr(config, "WATCHLIST_EXCLUDE_REGEX", "") or "").strip()
    deny_pattern = re.compile(exclude_regex, re.IGNORECASE) if exclude_regex else None

    now = datetime.now(timezone.utc)
    cached = _CACHE.get("symbols") or []
    cached_at = _CACHE.get("fetched_at_utc")
    cache_mode = _CACHE.get("mode", "")
    if cached and isinstance(cached_at, datetime) and cache_mode == "dynamic":
        if now - cached_at < timedelta(minutes=max(1, refresh_min)):
            _log_and_persist_watchlist(log, "cached", cached)
            return list(cached), "dynamic"

    try:
        from bybit import get_linear_tickers

        rows = get_linear_tickers()
    except Exception as exc:
        log.warning("Fallback to static WATCHLIST (Bybit fetch failed): %s", exc)
        if static_watchlist:
            _log_and_persist_watchlist(log, "fallback_static", static_watchlist, candidates_count=len(static_watchlist))
            return static_watchlist, "static_fallback"
        if cached:
            log.warning("Using cached dynamic watchlist as fallback.")
            _log_and_persist_watchlist(log, "cached", cached)
            return list(cached), "dynamic_cached_fallback"
        _log_and_persist_watchlist(log, "fallback_static", [], candidates_count=0)
        return [], "static_fallback"

    candidates: List[Tuple[str, float, float]] = []
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or not symbol.endswith("USDT"):
            continue
        quote = str(row.get("quoteCoin", "USDT")).upper()
        if quote != "USDT":
            continue
        status = str(row.get("status", row.get("symbolStatus", "Trading"))).lower()
        if status not in {"trading", "trade", "active"}:
            continue

        turnover = _to_float(row.get("turnover24h", 0.0))
        if turnover < min_turnover:
            continue

        last_price = _to_float(row.get("lastPrice", 0.0))
        high = _to_float(row.get("highPrice24h", 0.0))
        low = _to_float(row.get("lowPrice24h", 0.0))
        if last_price <= 0:
            continue
        if min_price > 0 and last_price < min_price:
            continue
        volatility_pct = ((high - low) / last_price) * 100.0 if high > low else 0.0
        if volatility_pct < min_vol_pct or volatility_pct > max_vol_pct:
            continue

        if exclude_prefixes and symbol.startswith(exclude_prefixes):
            continue
        if exclude_symbols and symbol in exclude_symbols:
            continue
        if deny_pattern and deny_pattern.search(symbol):
            continue

        candidates.append((symbol, turnover, volatility_pct))

    rank_mode = str(getattr(config, "WATCHLIST_RANK", "turnover")).strip().lower()
    pool_n = int(getattr(config, "WATCHLIST_POOL_N", 30))
    if rank_mode not in {"turnover", "turnover_vol", "momentum_1h"}:
        rank_mode = "turnover"

    if rank_mode == "turnover":
        candidates.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, _, _ in candidates[:top_n]]
    elif rank_mode == "turnover_vol":
        candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
        result = [s for s, _, _ in candidates[:top_n]]
    elif rank_mode == "momentum_1h":
        candidates.sort(key=lambda x: x[1], reverse=True)
        pool = candidates[:pool_n]
        scored: List[Tuple[str, float, float, float]] = []
        from bybit import fetch_klines
        from bybit import RateLimitedError as BybitRateLimitedError
        downgraded = False
        for symbol, turnover, vol_pct in pool:
            abs_return = 0.0
            try:
                klines = fetch_klines(symbol=symbol, interval="60", limit=2)
                if klines:
                    c = klines[-1]
                    o = float(c.get("open", 0) or 0)
                    cl = float(c.get("close", 0) or 0)
                    if o > 0:
                        abs_return = abs((cl - o) / o) * 100.0
            except BybitRateLimitedError:
                if not downgraded:
                    log.info("WATCHLIST momentum_1h rate-limited; downgrading to turnover_vol")
                    downgraded = True
                rank_mode = "turnover_vol"
                break
            except Exception:
                pass
            scored.append((symbol, turnover, vol_pct, abs_return))
        if rank_mode == "momentum_1h" and scored:
            scored.sort(key=lambda x: (x[3], x[1]), reverse=True)
            result = [s for s, _, _, _ in scored[:top_n]]
        elif rank_mode == "turnover_vol":
            candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
            result = [s for s, _, _ in candidates[:top_n]]
        else:
            candidates.sort(key=lambda x: x[1], reverse=True)
            result = [s for s, _, _ in candidates[:top_n]]
    else:
        candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
        result = [s for s, _, _ in candidates[:top_n]]

    if not result:
        log.warning("Dynamic watchlist produced no symbols; falling back to static.")
        if static_watchlist:
            _log_and_persist_watchlist(log, "fallback_static", static_watchlist, candidates_count=len(static_watchlist))
            return static_watchlist, "static_fallback"
        if cached:
            _log_and_persist_watchlist(log, "cached", cached)
            return list(cached), "dynamic_cached_fallback"
        _log_and_persist_watchlist(log, "fallback_static", [], candidates_count=0)
        return [], "static_fallback"

    _CACHE["symbols"] = list(result)
    _CACHE["mode"] = "dynamic"
    _CACHE["fetched_at_utc"] = now
    cached_until = int(now.timestamp()) + max(1, refresh_min) * 60
    _log_and_persist_watchlist(log, "dynamic", result, candidates_count=len(candidates), cached_until_ts=cached_until)
    return list(result), "dynamic"
