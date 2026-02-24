import re
from datetime import datetime, timezone
from typing import Dict, List

import requests

from config import BYBIT_BASE_URL

_LAST_TOPN_STATS: Dict[str, int] = {"candidates": 0, "filtered_out": 0, "final": 0}
_LAST_TOPN_EXCLUDED: List[str] = []


def get_linear_tickers() -> List[Dict]:
    """
    Fetch public tickers for category=linear (USDT linear perps).
    Returns raw ticker list from Bybit v5 /market/tickers.
    """
    url = f"{BYBIT_BASE_URL}/v5/market/tickers"
    params = {"category": "linear"}
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {payload.get('retMsg', 'unknown error')}")
    return list(payload.get("result", {}).get("list", []) or [])


def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, float]]:
    """
    Fetch public market candles from Bybit v5 /market/kline.
    This module only uses public market data and contains no trading endpoints.
    """
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()

    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {payload.get('retMsg', 'unknown error')}")

    rows = payload.get("result", {}).get("list", [])
    if not rows:
        return []

    # Bybit returns newest-first. Reverse so indicators are calculated oldest -> newest.
    ordered = list(reversed(rows))

    candles: List[Dict[str, float]] = []
    for item in ordered:
        ts_ms = int(item[0])
        ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
        candles.append(
            {
                "timestamp": ts_ms,
                "timestamp_utc": ts_utc,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )
    return candles


def get_top_linear_usdt_symbols(
    n: int,
    *,
    min_price: float = 0.01,
    min_turnover_24h: float = 0.0,
    exclude_prefixes: List[str] | None = None,
    exclude_symbols: List[str] | None = None,
    exclude_regex: str = "",
    max_spread_bps: float = 0.0,
) -> List[str]:
    """
    Fetch top active USDT linear perp symbols by 24h turnover (fallback: volume).
    Uses public Bybit v5 /market/tickers endpoint.
    """
    top_n = max(1, int(n))
    url = f"{BYBIT_BASE_URL}/v5/market/tickers"
    params = {"category": "linear"}
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()

    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {payload.get('retMsg', 'unknown error')}")

    rows = payload.get("result", {}).get("list", []) or []
    scored: List[tuple] = []
    blocked: List[tuple] = []
    blocked_examples: List[str] = []
    deny_prefixes = tuple(str(p).upper() for p in (exclude_prefixes or []))
    deny_symbols = {str(s).upper() for s in (exclude_symbols or [])}
    deny_pattern = None
    if str(exclude_regex or "").strip():
        try:
            deny_pattern = re.compile(str(exclude_regex).strip(), re.IGNORECASE)
        except re.error:
            deny_pattern = None

    def _to_float(raw) -> float:
        try:
            return float(raw or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _spread_bps_from_row(row_data: Dict[str, object]) -> float:
        ask = _to_float(
            row_data.get("ask1Price")
            or row_data.get("askPrice")
            or row_data.get("bestAskPrice")
        )
        bid = _to_float(
            row_data.get("bid1Price")
            or row_data.get("bidPrice")
            or row_data.get("bestBidPrice")
        )
        mid = (ask + bid) / 2.0
        if ask <= 0 or bid <= 0 or mid <= 0:
            return 0.0
        return ((ask - bid) / mid) * 10000.0

    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or not symbol.endswith("USDT"):
            continue
        quote_coin = str(row.get("quoteCoin", "USDT")).upper()
        if quote_coin != "USDT":
            continue
        status = str(row.get("status", row.get("symbolStatus", "Trading"))).lower()
        if status not in {"trading", "trade", "active"}:
            continue

        last_price = _to_float(row.get("lastPrice", 0.0))
        turnover = _to_float(row.get("turnover24h", 0.0))
        volume = _to_float(row.get("volume24h", 0.0))
        score = turnover if turnover > 0 else volume
        if score <= 0:
            continue
        turnover_for_filter = turnover if turnover > 0 else volume
        passes_filters = True
        if min_price > 0 and last_price < min_price:
            passes_filters = False
        if min_turnover_24h > 0 and turnover_for_filter < min_turnover_24h:
            passes_filters = False
        if deny_prefixes and symbol.startswith(deny_prefixes):
            passes_filters = False
        if deny_symbols and symbol in deny_symbols:
            passes_filters = False
        if deny_pattern is not None and deny_pattern.search(symbol):
            passes_filters = False
        if max_spread_bps > 0:
            spread_bps = _spread_bps_from_row(row)
            if spread_bps > max_spread_bps:
                passes_filters = False
        target = scored if passes_filters else blocked
        target.append((symbol, score))
        if (not passes_filters) and len(blocked_examples) < 5:
            blocked_examples.append(symbol)

    scored.sort(key=lambda x: x[1], reverse=True)
    blocked.sort(key=lambda x: x[1], reverse=True)
    global _LAST_TOPN_EXCLUDED
    _LAST_TOPN_EXCLUDED = list(blocked_examples)
    preferred = [symbol for symbol, _ in scored]
    if len(preferred) >= top_n:
        result = preferred[:top_n]
        _LAST_TOPN_STATS.update(
            {"candidates": len(preferred) + len(blocked), "filtered_out": len(blocked), "final": len(result)}
        )
        return result
    backfill = [symbol for symbol, _ in blocked]
    result = (preferred + backfill)[:top_n]
    _LAST_TOPN_STATS.update(
        {"candidates": len(preferred) + len(blocked), "filtered_out": len(blocked), "final": len(result)}
    )
    return result


def get_last_topn_stats() -> Dict[str, int]:
    return dict(_LAST_TOPN_STATS)


def get_last_topn_excluded_examples() -> List[str]:
    return list(_LAST_TOPN_EXCLUDED)