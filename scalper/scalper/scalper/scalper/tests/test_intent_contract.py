"""
Test TradeIntent contract and RiskEngine validation.
- Valid intent passes schema validation.
- Invalid intent is blocked with explicit reason.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_valid_intent_passes_validation():
    from scalper.models import validate_trade_intent, TradeIntent, intent_from_dict

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "V3_TREND_BREAKOUT",
        "timeframe": "15",
        "bar_ts": "2024-01-01T12:00:00+00:00",
    }
    ok, reason = validate_trade_intent(intent)
    assert ok is True
    assert reason == ""

    ti = intent_from_dict(intent)
    assert ti.symbol == "BTCUSDT"
    assert ti.side == "LONG"
    assert ti.strategy_id == "V3_TREND_BREAKOUT"
    assert ti.timeframe == "15"
    assert ti.bar_ts == "2024-01-01T12:00:00+00:00"


def test_invalid_intent_blocked_with_reason():
    from scalper.models import validate_trade_intent

    ok, reason = validate_trade_intent(None)
    assert ok is False
    assert reason == "INTENT_NONE"

    ok, reason = validate_trade_intent({})
    assert ok is False
    assert "INTENT_MISSING" in reason or "SYMBOL" in reason

    ok, reason = validate_trade_intent({"symbol": "BTCUSDT"})
    assert ok is False
    assert "SIDE" in reason or "STRATEGY" in reason or "TIMEFRAME" in reason or "BAR_TS" in reason

    ok, reason = validate_trade_intent({
        "symbol": "BTCUSDT",
        "side": "INVALID",
        "strategy": "X",
        "timeframe": "15",
        "bar_ts": "2024-01-01T12:00:00",
    })
    assert ok is False
    assert "SIDE" in reason

    ok, reason = validate_trade_intent({
        "symbol": "",
        "side": "LONG",
        "strategy": "X",
        "timeframe": "15",
        "bar_ts": "2024-01-01T12:00:00",
    })
    assert ok is False
    assert "SYMBOL" in reason


def test_risk_engine_blocks_invalid_intent():
    from scalper.models import validate_trade_intent
    from scalper.risk_engine_core import RiskEngine
    from types import SimpleNamespace

    store = SimpleNamespace(
        load_paper_state=lambda: {"open_positions": [], "day_utc": "2024-01-01", "trade_count_today": 0},
        save_paper_state=lambda _: None,
        store_risk_event=lambda _: None,
        store_trade_intent=lambda _: None,
    )
    settings = SimpleNamespace(
        paper_equity_usdt=200.0,
        fail_closed_on_snapshot_missing=False,
        max_dd_pct=12.0,
        max_concurrent_positions=2,
        max_symbol_notional_pct=30.0,
        cluster_btc_eth_limit=1,
        max_trades_day=12,
        min_seconds_between_trades=0,
        min_seconds_between_symbol_trades=0,
    )

    engine = RiskEngine({}, settings, store)

    verdict = engine.evaluate(
        {"symbol": ""},
        snapshot={"equity": 200, "open_positions": [], "open_positions_count": 0},
    )
    assert verdict.allowed is False
    assert "INTENT" in verdict.reason
