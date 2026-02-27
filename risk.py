from __future__ import annotations

from typing import Any, Dict, Tuple


def evaluate_intent(intent: Dict[str, Any], state: Dict[str, Any], now_ts: int) -> Tuple[str, str]:
    """
    DRY-RUN risk gate for trade intents.
    Compatibility shim over RiskEngine.
    Returns: ("ALLOW"|"BLOCK", reason)
    """
    from scalper.risk_engine_core import RiskEngine
    from scalper.settings import get_settings
    import storage as state_store

    _ = now_ts
    engine = RiskEngine(state, get_settings().risk, state_store)
    verdict = engine.assess(intent)
    return ("ALLOW", verdict.reason) if verdict.allowed else ("BLOCK", verdict.reason)
