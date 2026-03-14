"""Tests for paper exit-layer controls: time-stop, BE move, partial TP."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper import PaperPosition, update_and_maybe_close


def _candle(high: float, low: float, close: float, ts: str = "2024-01-01T12:00:00+00:00") -> dict:
    return {"high": high, "low": low, "close": close, "timestamp_utc": ts}


def test_time_stop_triggers():
    """TIME_STOP: when bars_held >= time_stop_bars and max_favorable_R < time_stop_min_r => close."""
    # LONG: entry 100, sl 98 => risk=2. Price stays flat, never reaches 1R (102).
    pos = PaperPosition(
        intent_id="t1",
        symbol="X",
        side="LONG",
        strategy="V3",
        entry_price=100.0,
        notional_usdt=100.0,
        qty_est=1.0,
        atr_at_entry=2.0,
        sl_price=98.0,
        tp_price=106.0,
        entry_ts="2024-01-01T10:00:00+00:00",
        last_ts="2024-01-01T10:00:00+00:00",
        bars_held=5,  # already 5 bars
        max_favorable_price=100.5,  # favorable_r = 0.25
    )
    # Bar 6: close 100.2, favorable_r still < 0.5. time_stop_bars=6, time_stop_min_r=0.5
    c = _candle(100.5, 99.5, 100.2)
    exit_params = {"time_stop_bars": 6, "time_stop_min_r": 0.5}
    updated, closed, pnl, reason, partial = update_and_maybe_close(
        pos, c, fees_bps=6.0, timeout_bars=100,
        replay_strict_exit=False,
        exit_params=exit_params,
    )
    assert closed
    assert reason == "TIME_STOP"
    # gross = (100.2 - 100) * 1 = 0.2, fees = notional * 6e-4 * 2
    assert pnl > 0


def test_be_move_triggers():
    """BE move: when favorable_r >= be_at_r => sl moves to entry, be_moved=True."""
    pos = PaperPosition(
        intent_id="t2",
        symbol="X",
        side="LONG",
        strategy="V3",
        entry_price=100.0,
        notional_usdt=100.0,
        qty_est=1.0,
        atr_at_entry=2.0,
        sl_price=98.0,
        tp_price=106.0,
        entry_ts="2024-01-01T10:00:00+00:00",
        last_ts="2024-01-01T10:00:00+00:00",
        bars_held=1,
        max_favorable_price=0.0,
        be_moved=False,
    )
    # Bar with high 103 => favorable_r = 1.5 >= 1.0
    c = _candle(103.0, 101.0, 102.0)
    exit_params = {"be_at_r": 1.0}
    updated, closed, pnl, reason, partial = update_and_maybe_close(
        pos, c, fees_bps=6.0, timeout_bars=100,
        replay_strict_exit=False,
        exit_params=exit_params,
    )
    assert not closed
    assert updated.be_moved
    assert updated.sl_price == 100.0  # moved to entry


def test_partial_tp_triggers_once_only():
    """Partial TP: triggers once when favorable_r >= partial_tp_at_r; partial_taken blocks repeat."""
    pos = PaperPosition(
        intent_id="t3",
        symbol="X",
        side="LONG",
        strategy="V3",
        entry_price=100.0,
        notional_usdt=100.0,
        qty_est=1.0,
        atr_at_entry=2.0,
        sl_price=98.0,
        tp_price=106.0,
        entry_ts="2024-01-01T10:00:00+00:00",
        last_ts="2024-01-01T10:00:00+00:00",
        bars_held=0,
        max_favorable_price=0.0,
        partial_taken=False,
    )
    exit_params = {"partial_tp_at_r": 1.0, "partial_tp_pct": 0.5}
    # Bar 1: high 103 => favorable_r 1.5 >= 1.0 => partial TP
    c1 = _candle(103.0, 101.0, 102.5)
    updated, closed, pnl, reason, partial = update_and_maybe_close(
        pos, c1, fees_bps=6.0, timeout_bars=100,
        replay_strict_exit=False,
        exit_params=exit_params,
    )
    assert not closed
    assert partial is not None
    assert partial.get("close_reason") == "PARTIAL_TP"
    assert updated.partial_taken
    assert updated.qty_est < pos.qty_est  # reduced
    # Bar 2: still favorable, but partial_taken => no second partial
    c2 = _candle(104.0, 102.0, 103.5)
    updated2, closed2, pnl2, reason2, partial2 = update_and_maybe_close(
        updated, c2, fees_bps=6.0, timeout_bars=100,
        replay_strict_exit=False,
        exit_params=exit_params,
    )
    assert partial2 is None  # no second partial
    assert not closed2
