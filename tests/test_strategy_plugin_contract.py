"""
Test strategy plugin contract.
- Loads enabled strategies and validates they return StrategyResult with correct shape.
- Validates intent shape when ok=True.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_load_enabled_strategies():
    from scalper.strategies import load_enabled_strategies
    from scalper.settings import get_settings

    settings = get_settings()
    strategies = load_enabled_strategies(settings)
    assert isinstance(strategies, list)


def test_strategies_return_strategy_result():
    from scalper.strategies import load_enabled_strategies, evaluate_enabled_first
    from scalper.models import StrategyResult
    from scalper.settings import get_settings

    settings = get_settings()
    strategies = load_enabled_strategies(settings)
    ctx = {
        "candles_15m": [],
        "candles_5m": [],
        "mtf_snapshot": {},
        "bias_info": {},
        "bar_ts_used": "2024-01-01T12:00:00+00:00",
        "timeframe": "15",
    }

    for s in strategies:
        res = s.evaluate("BTCUSDT", ctx)
        assert isinstance(res, StrategyResult)
        assert hasattr(res, "ok")
        assert hasattr(res, "reason")
        assert isinstance(res.ok, bool)
        assert isinstance(res.reason, str)


def test_strategy_result_to_evaluated_returns_dict_with_final_intents():
    from scalper.models import StrategyResult
    from scalper.strategies import strategy_result_to_evaluated

    res = StrategyResult(ok=False, reason="no_setup")
    ev = strategy_result_to_evaluated(res)
    assert "final_intents" in ev
    assert "market_snapshot" in ev
    assert "skip_reason" in ev
    assert isinstance(ev["final_intents"], list)


def test_strategy_result_with_intent_builds_trade_intent():
    from scalper.models import StrategyResult, TradeIntent, validate_trade_intent
    from scalper.strategies import strategy_result_to_evaluated

    evaluated = {
        "final_intents": [{
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "V3_TREND_BREAKOUT",
            "timeframe": "15",
            "bar_ts": "2024-01-01T12:00:00+00:00",
        }],
        "market_snapshot": {},
        "skip_reason": None,
    }
    res = StrategyResult(ok=True, debug={"evaluated": evaluated})
    ctx = {"timeframe": "15", "bar_ts_used": "2024-01-01T12:00:00+00:00"}

    ev = strategy_result_to_evaluated(res, context=ctx)
    assert res.intent is not None
    assert isinstance(res.intent, TradeIntent)
    ok, reason = validate_trade_intent(res.intent)
    assert ok is True
