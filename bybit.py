from typing import Dict, List

import requests

from config import BYBIT_BASE_URL


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
        candles.append(
            {
                "timestamp": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )
    return candles
