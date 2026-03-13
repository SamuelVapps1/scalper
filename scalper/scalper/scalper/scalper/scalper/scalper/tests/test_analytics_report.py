from __future__ import annotations

from scalper.analytics_report import AnalyticsSummary, compute_analytics


def test_compute_analytics_on_synthetic_history() -> None:
    intents = [
        {"risk_verdict": "ALLOW"},
        {"risk_verdict": "BLOCK", "block_reason": "ATR_UNAVAILABLE"},
        {"risk_verdict": "BLOCK", "block_reason": "cooldown_active"},
        {"risk_verdict": "BLOCK", "block_reason": "INVALID_TRADE_PLAN"},
    ]
    records = [
        {
            "symbol": "BTCUSDT",
            "strategy": "TEST_A",
            "pnl_usdt": 10.0,
            "close_reason": "TP",
            "pnl_r": 1.5,
        },
        {
            "symbol": "BTCUSDT",
            "strategy": "TEST_A",
            "pnl_usdt": -5.0,
            "close_reason": "SL",
            "pnl_r": -1.0,
        },
        {
            "symbol": "ETHUSDT",
            "strategy": "TEST_B",
            "pnl_usdt": 0.0,
            "close_reason": "TIMEOUT",
            "pnl_r": 0.0,
        },
    ]
    summary = compute_analytics(intents, records)
    assert summary.total_intents == 4
    assert summary.total_allow == 1
    assert summary.total_block == 3
    assert summary.paper_trades == 3
    assert summary.tp_count == 1
    assert summary.sl_count == 1
    assert summary.timeout_count == 1
    assert summary.winrate > 0
    # ATR failure / cooldown / degraded-plan rates derived from block reasons.
    assert summary.atr_failure_rate > 0
    assert summary.cooldown_block_rate > 0
