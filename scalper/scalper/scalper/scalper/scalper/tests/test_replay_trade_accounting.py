"""Tests for replay/experiment trade accounting: entries_count, closes_count, partial_closes_count."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scalper.storage import compute_paper_kpis


def test_partial_then_close_accounting():
    """One position: open -> partial TP -> full close => entries=1, partial_closes=1, closes=1."""
    # Same position: partial TP (partial=1) then full close (partial=0)
    partial_trade = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "setup": "V3_TREND_BREAKOUT",
        "entry_ts": "2024-01-15T10:00:00+00:00",
        "close_ts": "2024-01-15T11:00:00+00:00",
        "pnl_usdt": 2.5,
        "close_reason": "PARTIAL_TP",
        "entry_price": 100.0,
        "sl_price": 98.0,
        "tp_price": 101.0,
        "partial": 1,
    }
    full_close = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "setup": "V3_TREND_BREAKOUT",
        "entry_ts": "2024-01-15T10:00:00+00:00",
        "close_ts": "2024-01-15T14:00:00+00:00",
        "pnl_usdt": -0.5,
        "close_reason": "SL",
        "entry_price": 100.0,
        "sl_price": 98.0,
        "partial": 0,
    }
    closed_trades = [partial_trade, full_close]
    result = compute_paper_kpis(closed_trades, paper_equity_usdt=200.0)
    kpi = result.get("kpi") or {}

    assert kpi["entries_count"] == 1, "One position opened"
    assert kpi["partial_closes_count"] == 1, "One partial TP event"
    assert kpi["closes_count"] == 1, "One full close"
    assert kpi["total_fills_count"] == 2, "Two fills (partial + full)"
    # winrate_roundtrip: position PnL = 2.5 + (-0.5) = 2.0 > 0 => win
    assert kpi["winrate_roundtrip"] == 1.0, "Roundtrip PnL positive"
    assert kpi["round_trip_wins"] == 1
    assert kpi["round_trip_losses"] == 0
