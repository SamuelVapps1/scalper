from __future__ import annotations

from typing import Any, Dict

from scalper.models import StrategyResult
from scalper.strategies.base import StrategyContext


class V3TrendBreakoutStrategy:
    name = "v3_trend_breakout"

    def enabled(self, settings: Any) -> bool:
        return bool(settings.strategy_v3.v3_trend_breakout)

    def evaluate(self, symbol: str, context: StrategyContext) -> StrategyResult:
        from strategies.strategy_v3_tcb import v3_tcb_evaluate

        candles_15m = context.get("candles_15m") or []
        if not candles_15m:
            return StrategyResult(ok=False, reason="v3_not_enough_15m_bars")

        i15 = int(context.get("i15", len(candles_15m) - 1))
        result = v3_tcb_evaluate(
            symbol=symbol,
            snapshot_symbol=context.get("mtf_snapshot") or {},
            candles_15m=candles_15m,
            candles_5m=context.get("candles_5m"),
            i15=i15,
            params=context.get("v3_params") or {},
            map15_to_5=context.get("map15_to_5"),
            close5=context.get("close5"),
        )

        if not result.ok:
            return StrategyResult(ok=False, side=result.side, reason=result.reason, debug=dict(result.debug or {}))

        cur = candles_15m[i15] if 0 <= i15 < len(candles_15m) else {}
        low_15m = float(cur.get("low", 0) or 0)
        high_15m = float(cur.get("high", 0) or 0)
        atr15m = float((result.debug or {}).get("atr15m", 0) or 0)
        sl_atr_mult = float(context.get("sl_atr_mult", 0.60))
        tp_r = float(context.get("tp_r", 1.5))
        side = str(result.side or "")
        sl_price = low_15m - sl_atr_mult * atr15m if side == "LONG" else high_15m + sl_atr_mult * atr15m
        intent: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "strategy": "V3_TREND_BREAKOUT",
            "close": (result.debug or {}).get("close_15m"),
            "level_ref": result.breakout_level,
            "entry_type": "market_sim",
            "meta": {
                "sl_hint": sl_price,
                "tp_r_mult": tp_r,
                "atr14": atr15m,
            },
        }
        evaluated = {
            "final_intents": [intent],
            "market_snapshot": {"atr14": atr15m},
            "skip_reason": None,
        }
        return StrategyResult(
            ok=True,
            side=side,
            reason="",
            breakout_level=result.breakout_level,
            debug={"evaluated": evaluated, **dict(result.debug or {})},
        )

