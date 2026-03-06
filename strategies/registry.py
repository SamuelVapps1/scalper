from __future__ import annotations

from typing import Any, Dict, List, Optional

from scalper.models import StrategyResult, TradeIntent, intent_from_dict
from scalper.strategies.base import Strategy, StrategyContext
from scalper.strategies.v1_strategy import V1Strategy
from scalper.strategies.v2_strategy import V2TrendPullbackStrategy
from scalper.strategies.v3_strategy import V3TrendBreakoutStrategy


def available_strategies() -> List[Strategy]:
    # Keep live/replay precedence: V3 first, then V2, then V1.
    return [
        V3TrendBreakoutStrategy(),
        V2TrendPullbackStrategy(),
        V1Strategy(),
    ]


def load_enabled_strategies(settings: Any) -> List[Strategy]:
    return [s for s in available_strategies() if s.enabled(settings)]


def strategy_result_to_evaluated(result: StrategyResult, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    evaluated = dict((result.debug or {}).get("evaluated") or {})
    if evaluated:
        intents = list(evaluated.get("final_intents", []) or [])
        if intents and result.ok and result.intent is None:
            first = intents[0]
            if isinstance(first, dict):
                try:
                    merged = dict(first)
                    if context:
                        merged.setdefault("timeframe", str(context.get("timeframe", "15") or "15"))
                        merged.setdefault("bar_ts", str(context.get("bar_ts_used", "") or ""))
                        merged.setdefault("candle_ts", merged.get("bar_ts"))
                        merged.setdefault("ts", merged.get("bar_ts"))
                    result.intent = intent_from_dict(merged)
                except Exception:
                    pass
        return evaluated
    if result.ok:
        return {"final_intents": [], "market_snapshot": {}, "skip_reason": None}
    return {"final_intents": [], "market_snapshot": {}, "skip_reason": str(result.reason or "")}


def evaluate_enabled_first(
    symbol: str,
    context: StrategyContext,
    strategies: List[Strategy],
) -> StrategyResult:
    last = StrategyResult(ok=False, reason="no_active_setup")
    for strategy in strategies:
        res = strategy.evaluate(symbol, context)
        if res.ok:
            return res
        if res.reason:
            last = res
    return last

