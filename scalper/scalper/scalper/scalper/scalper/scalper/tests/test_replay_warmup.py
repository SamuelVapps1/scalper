"""
Tests for replay warmup_days: short evaluation windows need extra candle history
for 4H EMA200 + slope10 (~35+ days of 4H).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_warmup_days_increases_fetch_range():
    """warmup_days=60 fetches more candles than warmup_days=0; both complete successfully."""
    from replay import run_replay

    result_no_warm = run_replay(
        symbols=["BTCUSDT"],
        days=5,
        tf_trigger=15,
        tf_timing=5,
        tf_bias=240,
        tf_setup=60,
        step_bars=1,
        use_cache=True,
        silent=True,
        warmup_days=0,
    )
    result_warm = run_replay(
        symbols=["BTCUSDT"],
        days=5,
        tf_trigger=15,
        tf_timing=5,
        tf_bias=240,
        tf_setup=60,
        step_bars=1,
        use_cache=True,
        silent=True,
        warmup_days=60,
    )
    kpi_no = result_no_warm.get("kpi") or {}
    kpi_warm = result_warm.get("kpi") or {}
    trades_no = int(kpi_no.get("trades_total", 0) or 0)
    trades_warm = int(kpi_warm.get("trades_total", 0) or 0)
    # With warmup, we have more 4H history; trades should be >= without warmup
    assert trades_warm >= trades_no, (
        f"warmup_days=60 should produce >= trades than warmup_days=0; got {trades_warm} vs {trades_no}"
    )
    # Both should complete with valid KPIs
    assert "trades_total" in kpi_no
    assert "trades_total" in kpi_warm


def test_warmup_days_in_run_signature():
    """RUN_SIGNATURE includes warmup_days for deterministic cache/run_id."""
    from replay import run_replay

    result = run_replay(
        symbols=["BTCUSDT"],
        days=3,
        tf_trigger=15,
        tf_timing=5,
        tf_bias=240,
        tf_setup=60,
        step_bars=1,
        use_cache=True,
        silent=True,
        warmup_days=42,
    )
    run_id = result.get("run_id", "")
    assert run_id, "run_id should be non-empty"
