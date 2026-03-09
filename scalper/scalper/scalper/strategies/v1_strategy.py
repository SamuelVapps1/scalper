from __future__ import annotations

from typing import Any

from scalper.models import StrategyResult
from scalper.strategies.base import StrategyContext


class V1Strategy:
    name = "v1"

    def enabled(self, settings: Any) -> bool:
        return bool(settings.strategy_v3.strategy_v1)

    def evaluate(self, symbol: str, context: StrategyContext) -> StrategyResult:
        from signals import evaluate_symbol_intents_v1

        evaluated = evaluate_symbol_intents_v1(
            symbol=symbol,
            candles_15m=context.get("candles_15m") or [],
            mtf_snapshot=context.get("mtf_snapshot") or {},
            bias_info=context.get("bias_info") or {},
            signal_debug=bool(context.get("signal_debug", False)),
            timeframe=str(context.get("timeframe", "15")),
        )
        intents = list(evaluated.get("final_intents", []) or [])
        first = intents[0] if intents else {}
        return StrategyResult(
            ok=bool(intents),
            side=first.get("side"),
            reason=str(evaluated.get("skip_reason") or ""),
            debug={"evaluated": evaluated},
        )

