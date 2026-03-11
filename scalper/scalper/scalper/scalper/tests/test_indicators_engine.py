from __future__ import annotations

from typing import Any, Dict, List

from indicators_engine import precompute_tf_indicators


def _candles(n: int) -> List[Dict[str, Any]]:
    base = 1_700_000_000_000
    out: List[Dict[str, Any]] = []
    for i in range(n):
        price = 100.0 + i * 0.3
        out.append(
            {
                "timestamp": base + i * 60_000,
                "open": price,
                "high": price + 1.0,
                "low": price - 1.0,
                "close": price + 0.2,
                "volume": 5.0 + i,
            }
        )
    return out


def test_precompute_tf_indicators_output_lengths() -> None:
    candles = _candles(60)
    out = precompute_tf_indicators(candles)

    expected_len = len(candles)
    for key in ("ts", "open", "high", "low", "close", "ema20", "ema50", "ema200", "ema200_prev10", "atr14"):
        assert len(out[key]) == expected_len
    assert all(v is None for v in out["ema200_prev10"][:10])

