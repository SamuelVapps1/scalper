"""
Smoke test for deterministic replay harness.
Uses embedded fixture candles. Asserts completion and required summary keys.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))


def _make_smoke_candles(count: int = 250) -> list[dict]:
    """Generate minimal OHLC candles for smoke test. 250 bars >= min_needed (200)."""
    base_ts = 1704067200000  # 2024-01-01 00:00 UTC
    bar_ms = 15 * 60 * 1000
    base_price = 42000.0
    candles = []
    for i in range(count):
        ts = base_ts + i * bar_ms
        ts_utc = __import__("datetime").datetime.fromtimestamp(
            ts / 1000.0, tz=__import__("datetime").timezone.utc
        ).isoformat()
        drift = (i % 20 - 10) * 10.0
        o = base_price + drift
        h = o + 50
        l = o - 50
        c = o + (i % 3 - 1) * 20
        candles.append({
            "timestamp": ts,
            "timestamp_utc": ts_utc,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": 1000,
        })
    return candles


def test_replay_smoke_completes_and_outputs_summary(tmp_path, monkeypatch):
    """Replay completes and produces summary with required keys."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "runs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "cache").mkdir(parents=True, exist_ok=True)

    from scalper.replay_harness import run_replay

    fixtures = _make_smoke_candles(250)

    def fetch_fn(sym: str, tf: int, start_ms: int, end_ms: int):
        return [c for c in fixtures if start_ms <= c["timestamp"] <= end_ms]

    summary = run_replay(
        symbols=["BTCUSDT"],
        start_str="2024-01-01",
        end_str="2024-01-07",
        interval=15,
        seed=123,
        out_tag="smoke",
        use_cache=False,
        fetch_fn=fetch_fn,
        emit_events=False,
    )

    assert "pnl" in summary
    assert "winrate" in summary
    assert "maxDD" in summary
    assert "PF" in summary
    assert "avgR" in summary
    assert "trades_per_day" in summary
    assert "trades" in summary
    assert "seed" in summary
    assert summary["seed"] == 123

    assert (tmp_path / "runs" / "equity_curve_smoke.csv").exists()
    assert (tmp_path / "runs" / "trades_smoke.csv").exists()
    assert (tmp_path / "runs" / "summary_smoke.json").exists()


def test_replay_deterministic(tmp_path, monkeypatch):
    """Same inputs + seed => same summary."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "runs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "cache").mkdir(parents=True, exist_ok=True)

    from scalper.replay_harness import run_replay

    fixtures = _make_smoke_candles(250)

    def fetch_fn(sym: str, tf: int, start_ms: int, end_ms: int):
        return [c for c in fixtures if start_ms <= c["timestamp"] <= end_ms]

    summary1 = run_replay(
        symbols=["BTCUSDT"],
        start_str="2024-01-01",
        end_str="2024-01-07",
        interval=15,
        seed=456,
        out_tag="det1",
        use_cache=False,
        fetch_fn=fetch_fn,
        emit_events=False,
    )

    summary2 = run_replay(
        symbols=["BTCUSDT"],
        start_str="2024-01-01",
        end_str="2024-01-07",
        interval=15,
        seed=456,
        out_tag="det2",
        use_cache=False,
        fetch_fn=fetch_fn,
        emit_events=False,
    )

    assert summary1["pnl"] == summary2["pnl"]
    assert summary1["trades"] == summary2["trades"]
    assert summary1["winrate"] == summary2["winrate"]
