from __future__ import annotations

from typing import Any, Dict

from scalper.models import StrategyResult
from scalper.strategies.base import StrategyContext


def _parse_v3_conservative_params(s: str) -> Dict[str, Any]:
    """Parse 'KEY=VAL;KEY2=VAL2' into dict. Same as experiment _parse_variant."""
    out: Dict[str, Any] = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if v.lower() in ("1", "true", "yes"):
            out[k] = True
        elif v.lower() in ("0", "false", "no"):
            out[k] = False
        elif v.isdigit():
            out[k] = int(v)
        else:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out


class V3TrendBreakoutStrategy:
    name = "v3_trend_breakout"

    def enabled(self, settings: Any) -> bool:
        return bool(settings.strategy_v3.v3_trend_breakout)

    def evaluate(self, symbol: str, context: StrategyContext) -> StrategyResult:
        from scalper.settings import get_settings
        from strategies.strategy_v3_tcb import v3_tcb_evaluate

        candles_15m = context.get("candles_15m") or []
        if not candles_15m:
            return StrategyResult(ok=False, reason="v3_not_enough_15m_bars")

        i15 = int(context.get("i15", len(candles_15m) - 1))
        raw_params = dict(context.get("v3_params") or {})

        # RAW: default params
        result_raw = v3_tcb_evaluate(
            symbol=symbol,
            snapshot_symbol=context.get("mtf_snapshot") or {},
            candles_15m=candles_15m,
            candles_5m=context.get("candles_5m"),
            i15=i15,
            params=raw_params,
            map15_to_5=context.get("map15_to_5"),
            close5=context.get("close5"),
        )
        raw_ok = bool(result_raw and result_raw.ok)

        # CONSERVATIVE (HQ): v3_params + override from env
        hq_ok = False
        result_hq = None
        conservative_params_str = str(get_settings().strategy_v3.v3_conservative_params or "").strip()
        if conservative_params_str:
            hq_params = dict(raw_params)
            hq_params.update(_parse_v3_conservative_params(conservative_params_str))
            result_hq = v3_tcb_evaluate(
                symbol=symbol,
                snapshot_symbol=context.get("mtf_snapshot") or {},
                candles_15m=candles_15m,
                candles_5m=context.get("candles_5m"),
                i15=i15,
                params=hq_params,
                map15_to_5=context.get("map15_to_5"),
                close5=context.get("close5"),
            )
            hq_ok = bool(result_hq and result_hq.ok)

        # Choose result and profile (when no conservative params, only RAW is possible)
        if not conservative_params_str:
            if not raw_ok:
                return StrategyResult(
                    ok=False,
                    side=result_raw.side,
                    reason=str(result_raw.reason or "v3_fail"),
                    debug=dict(result_raw.debug or {}),
                )
            result = result_raw
            profile = "RAW"
            conf_raw = 0.70
            conf_hq = None
        elif raw_ok and hq_ok:
            result = result_hq
            profile = "HQ"
            conf_raw = 0.70
            conf_hq = 0.70
        elif hq_ok:
            result = result_hq
            profile = "HQ"
            conf_raw = None
            conf_hq = 0.70
        elif raw_ok:
            result = result_raw
            profile = "RAW"
            conf_raw = 0.70
            conf_hq = None
        else:
            res = result_hq if result_hq else result_raw
            return StrategyResult(
                ok=False,
                side=getattr(res, "side", None),
                reason=str(getattr(res, "reason", "v3_fail") or "v3_fail"),
                debug=dict(getattr(res, "debug", None) or {}),
            )

        cur = candles_15m[i15] if 0 <= i15 < len(candles_15m) else {}
        low_15m = float(cur.get("low", 0) or 0)
        high_15m = float(cur.get("high", 0) or 0)
        atr15m = float((result.debug or {}).get("atr15m", 0) or 0)
        sl_atr_mult = float(context.get("sl_atr_mult", 0.60))
        tp_r = float(context.get("tp_r", 1.5))
        side = str(result.side or "")
        sl_price = low_15m - sl_atr_mult * atr15m if side == "LONG" else high_15m + sl_atr_mult * atr15m
        meta: Dict[str, Any] = {
            "sl_hint": sl_price,
            "tp_r_mult": tp_r,
            "atr14": atr15m,
            "profile": profile,
        }
        if conf_raw is not None:
            meta["conf_raw"] = conf_raw
        if conf_hq is not None:
            meta["conf_hq"] = conf_hq
        intent: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "strategy": "V3_TREND_BREAKOUT",
            "close": (result.debug or {}).get("close_15m"),
            "level_ref": result.breakout_level,
            "entry_type": "market_sim",
            "meta": meta,
            "profile": profile,
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

