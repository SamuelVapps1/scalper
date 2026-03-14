from __future__ import annotations

from typing import Any

from scalper.models import StrategyResult
from scalper.strategies.base import StrategyContext


class V2TrendPullbackStrategy:
    name = "v2_trend_pullback"

    def enabled(self, settings: Any) -> bool:
        return bool(settings.strategy_v3.v2_trend_pullback)

    def evaluate(self, symbol: str, context: StrategyContext) -> StrategyResult:
        from signals import evaluate_symbol_intents_v2_trend_pullback

        evaluated = evaluate_symbol_intents_v2_trend_pullback(
            symbol=symbol,
            candles_15m=context.get("candles_15m") or [],
            mtf_snapshot=context.get("mtf_snapshot") or {},
            bias_info=context.get("bias_info") or {},
            bar_ts_used=str(context.get("bar_ts_used", "") or ""),
        )
        intents = list(evaluated.get("final_intents", []) or [])
        first = intents[0] if intents else {}
        return StrategyResult(
            ok=bool(intents),
            side=first.get("side"),
            reason=str(evaluated.get("skip_reason") or ""),
            debug={"evaluated": evaluated},
        )

