from __future__ import annotations

import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import BYBIT_BASE_URL, REQUEST_SLEEP_MS

_SESSION = requests.Session()
_LAST_REQUEST_TS = 0.0
_LAST_TOPN_EXCLUDED: List[str] = []
_LAST_TOPN_EXCLUDED_COUNTS: Dict[str, int] = {"prefix": 0, "symbol": 0, "regex": 0}
_RATE_LIMIT_PATTERN = re.compile(r"too many visits|rate limit|too many requests", re.IGNORECASE)


def _pace_before_request() -> None:
    global _LAST_REQUEST_TS
    sleep_ms = max(0, int(REQUEST_SLEEP_MS or 0))
    if sleep_ms <= 0:
        _LAST_REQUEST_TS = time.time()
        return
    min_gap = sleep_ms / 1000.0
    now = time.time()
    elapsed = now - _LAST_REQUEST_TS
    if elapsed < min_gap:
        time.sleep(min_gap - elapsed)
    _LAST_REQUEST_TS = time.time()


def _is_rate_limited(status_code: int, payload: Optional[Dict[str, Any]], err_text: str) -> bool:
    if status_code == 429:
        return True
    if payload:
        code = int(payload.get("retCode", 0) or 0)
        msg = str(payload.get("retMsg", "") or "")
        if code == 10006 or _RATE_LIMIT_PATTERN.search(msg):
            return True
    return bool(_RATE_LIMIT_PATTERN.search(err_text or ""))


def _request_json_with_retry(
    *,
    path: str,
    params: Dict[str, Any],
    timeout: int = 20,
    max_attempts: int = 5,
) -> Dict[str, Any]:
    url = f"{BYBIT_BASE_URL.rstrip('/')}{path}"
    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        _pace_before_request()
        try:
            response = _SESSION.get(url, params=params, timeout=timeout)
            status = int(response.status_code)
            payload: Optional[Dict[str, Any]] = None
            err_text = ""
            try:
                payload = response.json()
            except Exception:
                payload = None
                err_text = response.text or ""
            if response.ok and payload is not None and int(payload.get("retCode", 0) or 0) == 0:
                return payload
            if not _is_rate_limited(status, payload, err_text):
                if payload is not None:
                    raise RuntimeError(f"Bybit API error: {payload.get('retMsg', 'unknown error')}")
                response.raise_for_status()
            backoff = (2**attempt) * 0.5 + random.uniform(0.0, 0.25)
            time.sleep(min(backoff, 8.0))
        except requests.RequestException as exc:
            last_exc = exc
            backoff = (2**attempt) * 0.5 + random.uniform(0.0, 0.25)
            time.sleep(min(backoff, 8.0))
    if last_exc is not None:
        raise RuntimeError(f"Bybit request failed after retries: {last_exc}") from last_exc
    raise RuntimeError("Bybit request failed after retries")


def _do_json_request(url: str, params: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """Backward-compatible helper used by tests (and get_top_linear_usdt_symbols)."""
    if url.startswith("http://") or url.startswith("https://"):
        path = url.replace(BYBIT_BASE_URL.rstrip("/"), "", 1)
    else:
        path = url
    return _request_json_with_retry(path=path, params=params, timeout=timeout)


def _to_bybit_interval(tf) -> str:
    s = str(tf).strip().lower()

    alias_map = {
        "1": "1",
        "3": "3",
        "5": "5",
        "15": "15",
        "30": "30",
        "60": "60",
        "120": "120",
        "240": "240",
        "360": "360",
        "720": "720",
        "d": "D",
        "1d": "D",
        "w": "W",
        "1w": "W",
        "m": "M",
    }

    if s.endswith("m") and s[:-1].isdigit():
        s = s[:-1]

    if s in alias_map:
        return alias_map[s]

    return str(int(s))


def fetch_klines(symbol: str, interval: str, limit: int = 1000, start_ms=None, end_ms=None):
    params: Dict[str, Any] = {
        "category": "linear",
        "symbol": str(symbol).upper(),
        "interval": str(interval),
        "limit": str(max(1, min(1000, int(limit)))),
    }

    if start_ms is not None:
        params["start"] = int(start_ms)

    if end_ms is not None:
        params["end"] = int(end_ms)

    payload = _request_json_with_retry(
        path="/v5/market/kline",
        params=params,
        timeout=15,
    )
    rows = payload.get("result", {}).get("list", []) or []
    ordered = list(reversed(rows))

    candles: List[Dict[str, float]] = []
    for item in ordered:
        ts_ms = int(item[0])
        candles.append(
            {
                "timestamp": ts_ms,
                "timestamp_utc": datetime.fromtimestamp(
                    ts_ms / 1000.0, tz=timezone.utc
                ).isoformat(),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )
    return candles


def _spread_bps(row: Dict[str, Any]) -> float:
    try:
        ask = float(row.get("ask1Price") or row.get("askPrice") or 0.0)
        bid = float(row.get("bid1Price") or row.get("bidPrice") or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if ask <= 0 or bid <= 0:
        return 0.0
    mid = (ask + bid) / 2.0
    return ((ask - bid) / mid) * 10000.0 if mid > 0 else 0.0


def get_top_linear_usdt_symbols(
    n: int,
    *,
    min_price: float = 0.0,
    min_turnover_24h: float = 0.0,
    exclude_prefixes: Optional[List[str]] = None,
    exclude_symbols: Optional[List[str]] = None,
    exclude_regex: str = "",
    max_spread_bps: float = 0.0,
) -> List[str]:
    payload = _do_json_request(
        f"{BYBIT_BASE_URL}/v5/market/tickers",
        {"category": "linear"},
        timeout=20,
    )
    rows = payload.get("result", {}).get("list", []) or []
    deny_prefixes = tuple(str(x).upper() for x in (exclude_prefixes or []))
    deny_symbols = {str(x).upper() for x in (exclude_symbols or [])}
    deny_re = re.compile(exclude_regex, re.IGNORECASE) if str(exclude_regex).strip() else None
    global _LAST_TOPN_EXCLUDED, _LAST_TOPN_EXCLUDED_COUNTS
    excluded_examples: List[str] = []
    counts = {"prefix": 0, "symbol": 0, "regex": 0}

    candidates: List[Tuple[str, float]] = []
    for row in rows:
        symbol = str(row.get("symbol", "") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        quote_coin = str(row.get("quoteCoin", "USDT") or "USDT").upper()
        if quote_coin != "USDT":
            continue
        status = str(row.get("status", row.get("symbolStatus", "Trading")) or "").lower()
        if status not in {"trading", "trade", "active"}:
            continue
        try:
            last_price = float(row.get("lastPrice") or 0.0)
            turnover = float(row.get("turnover24h") or 0.0)
            volume = float(row.get("volume24h") or 0.0)
        except (TypeError, ValueError):
            continue
        if min_price > 0 and last_price < min_price:
            continue
        if min_turnover_24h > 0 and turnover < min_turnover_24h:
            continue
        if max_spread_bps > 0 and _spread_bps(row) > max_spread_bps:
            continue
        if deny_prefixes and symbol.startswith(deny_prefixes):
            counts["prefix"] += 1
            if len(excluded_examples) < 5:
                excluded_examples.append(symbol)
            continue
        if deny_symbols and symbol in deny_symbols:
            counts["symbol"] += 1
            if len(excluded_examples) < 5:
                excluded_examples.append(symbol)
            continue
        if deny_re and deny_re.search(symbol):
            counts["regex"] += 1
            if len(excluded_examples) < 5:
                excluded_examples.append(symbol)
            continue
        score = turnover if turnover > 0 else volume
        if score > 0:
            candidates.append((symbol, score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    _LAST_TOPN_EXCLUDED = excluded_examples
    _LAST_TOPN_EXCLUDED_COUNTS = counts
    top_n = max(1, int(n))
    return [s for s, _ in candidates[:top_n]]


def get_last_topn_excluded_examples() -> List[str]:
    return list(_LAST_TOPN_EXCLUDED)


def get_last_topn_excluded_counts() -> Dict[str, int]:
    return dict(_LAST_TOPN_EXCLUDED_COUNTS)
