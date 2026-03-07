from __future__ import annotations

from typing import Any, Dict, List

import candle_cache


def _mk_candles(start_ms: int, bars: int, tf_min: int) -> List[Dict[str, Any]]:
    bar_ms = tf_min * 60 * 1000
    out: List[Dict[str, Any]] = []
    for i in range(bars):
        ts = start_ms + i * bar_ms
        price = 100.0 + i
        out.append(
            {
                "timestamp": ts,
                "open": price,
                "high": price + 1.0,
                "low": price - 1.0,
                "close": price + 0.5,
                "volume": 10.0 + i,
            }
        )
    return out


def test_get_candles_covered_range_uses_cache_without_api(monkeypatch, tmp_path) -> None:
    tf_min = 15
    bar_ms = tf_min * 60 * 1000
    base = (1_700_000_000_000 // bar_ms) * bar_ms
    cached = _mk_candles(base, bars=40, tf_min=tf_min)

    fake_cache = tmp_path / "15.csv"
    fake_cache.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(candle_cache, "_cache_path", lambda _symbol, _tf: fake_cache)
    monkeypatch.setattr(candle_cache, "_load_cache_file", lambda _path: list(cached))

    calls = {"count": 0}

    def _never_fetch(*_args, **_kwargs):
        calls["count"] += 1
        return []

    monkeypatch.setattr(candle_cache, "_fetch_segment_from_api", _never_fetch)

    # Unaligned boundaries should still be fully covered after alignment.
    start_ms = base + 10 * bar_ms + 123
    end_ms = base + 20 * bar_ms + 456
    out = candle_cache.get_candles("BTCUSDT", tf_min, start_ms, end_ms, use_cache=True, cache_only=False)

    assert calls["count"] == 0
    assert len(out) == 11
    assert all("ts" in c for c in out)
    assert out[0]["timestamp"] == base + 10 * bar_ms
    assert out[-1]["timestamp"] == base + 20 * bar_ms

