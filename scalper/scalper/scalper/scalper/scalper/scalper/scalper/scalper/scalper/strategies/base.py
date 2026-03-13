from __future__ import annotations

from typing import Any, Dict, Protocol

from scalper.models import StrategyResult


StrategyContext = Dict[str, Any]


class Strategy(Protocol):
    name: str

    def enabled(self, settings: Any) -> bool:
        ...

    def evaluate(self, symbol: str, context: StrategyContext) -> StrategyResult:
        ...

