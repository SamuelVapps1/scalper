from __future__ import annotations

from typing import Any, Dict, List, Tuple

from scalper.settings import get_settings
from scalper.strategies import (
    evaluate_enabled_first,
    load_enabled_strategies,
    strategy_result_to_evaluated,
)


class StrategyEngine:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._enabled_strategies = load_enabled_strategies(self._settings)

    @property
    def has_enabled(self) -> bool:
        return bool(self._enabled_strategies)

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        candles_15m: List[Dict[str, Any]],
        candles_5m: List[Dict[str, Any]],
        mtf_snapshot: Dict[int, Dict[str, Any]],
        bias_info: Dict[str, Any],
        signal_debug: bool,
        interval: str,
        bar_ts_used: str,
        v3_params: Dict[str, Any],
        sl_atr_mult: float,
        tp_r: float,
    ) -> Tuple[Dict[str, Any], Any]:
        plugin_context = {
            "candles_15m": candles_15m or [],
            "candles_5m": candles_5m or [],
            "mtf_snapshot": mtf_snapshot or {},
            "bias_info": bias_info or {},
            "bar_ts_used": str(bar_ts_used or ""),
            "signal_debug": bool(signal_debug),
            "timeframe": str(interval),
            "v3_params": v3_params or {},
            "i15": (len(candles_15m) - 1 if candles_15m else 0),
            "sl_atr_mult": float(sl_atr_mult),
            "tp_r": float(tp_r),
        }
        plugin_result = evaluate_enabled_first(
            symbol=symbol,
            context=plugin_context,
            strategies=self._enabled_strategies,
        )
        return strategy_result_to_evaluated(plugin_result, context=plugin_context), plugin_result

