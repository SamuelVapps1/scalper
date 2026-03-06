from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from indicators_engine import precompute_tf_indicators
from scalper.models import StrategyResult, TradeIntent, intent_from_dict
from scalper.strategies.base import Strategy, StrategyContext
from scalper.strategies.breakout_retest_go import generate_intents as s2_breakout_generate
from scalper.strategies.liquidity_sweep_reversal import generate_intents as s5_lsr_generate
from scalper.strategies.rev_swept_rsi import generate_intents as rev_swept_rsi_generate
from scalper.strategies.trend_pullback import generate_intents as s1_trend_pullback_generate
from scalper.strategies.trend_reversal import generate_intents as s3_trend_reversal_generate
from scalper.strategies.v1_strategy import V1Strategy
from scalper.strategies.v2_strategy import V2TrendPullbackStrategy
from scalper.strategies.v3_strategy import V3TrendBreakoutStrategy
from scalper.strategies.vol_expansion import generate_intents as s4_vol_expansion_generate
from scalper.types import Intent


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


# --- Hybrid registry (DataFrame-based strategies) -----------------------------------------------

HYBRID_STRATEGIES: Dict[str, Tuple[str, Any]] = {
    "trend_pullback": ("S1_TREND_PULLBACK", s1_trend_pullback_generate),
    "breakout_retest_go": ("S2_BREAKOUT_RETEST_GO", s2_breakout_generate),
    "trend_reversal": ("S3_TREND_REVERSAL", s3_trend_reversal_generate),
    "vol_expansion": ("S4_VOL_EXPANSION", s4_vol_expansion_generate),
    "liquidity_sweep_reversal": ("Liquidity sweep reversal", s5_lsr_generate),
    "rev_swept_rsi": ("REV_SWEPT_RSI", rev_swept_rsi_generate),
}


def _parse_strategies_enabled(raw: str) -> List[str]:
    raw = str(raw or "").strip()
    if not raw:
        return []
    if raw.lower() in ("all", "*"):
        return list(HYBRID_STRATEGIES.keys())
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return [p for p in parts if p in HYBRID_STRATEGIES]


def _apply_confidence_adjustment(intent: Intent, bias_info: Dict[str, Any]) -> float:
    """
    Lightweight confidence adjustment:
    - +0.03 when aligned with 4H bias (LONG/LONG or SHORT/SHORT)
    - -0.05 when against a directional bias
    """
    base = float(intent.confidence or 0.0)
    bias = str((bias_info or {}).get("bias", "NONE") or "NONE").upper()
    side = str(intent.side or "").upper()
    adj = 0.0
    if bias in ("LONG", "SHORT"):
        if bias == side:
            adj = 0.03
        else:
            adj = -0.05
    return max(0.0, min(1.0, base + adj))


def run_hybrid_strategies_for_symbol(
    *,
    symbol: str,
    candles_15m: List[Dict[str, Any]],
    candles_5m: List[Dict[str, Any]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    settings: Any,
    interval: str,
    bar_ts_used: str,
    strategies_enabled_raw: str,
    top_intents_per_scan: int,
) -> Dict[str, Any]:
    """
    High-level helper for scanner: run enabled hybrid strategies for one symbol and
    return an evaluated dict compatible with the existing scanner pipeline.
    """
    enabled = _parse_strategies_enabled(strategies_enabled_raw or "")
    if not enabled:
        return {"final_intents": [], "market_snapshot": {}, "skip_reason": "hybrid_disabled"}

    if not candles_15m:
        return {"final_intents": [], "market_snapshot": {}, "skip_reason": "no_candles"}

    # Build candles+indicators DataFrame.
    indic = precompute_tf_indicators(candles_15m)
    df = pd.DataFrame(indic)
    if df.empty:
        return {"final_intents": [], "market_snapshot": {}, "skip_reason": "empty_df"}
    indic_5m = precompute_tf_indicators(candles_5m or []) if candles_5m else {}
    df_5m = pd.DataFrame(indic_5m) if indic_5m else pd.DataFrame()

    ctx: Dict[str, Any] = {
        "symbol": symbol,
        "tf": str(interval),
        "settings": settings,
        "mtf_snapshot": mtf_snapshot or {},
        "bias_info": bias_info or {},
        "bar_ts_used": bar_ts_used,
        "candles_5m": candles_5m or [],
    }

    all_intents: List[Intent] = []
    for key in enabled:
        setup_name, fn = HYBRID_STRATEGIES[key]
        if key == "rev_swept_rsi" and not bool(getattr(getattr(settings, "strategy_v3", settings), "rev_enabled", True)):
            logging.debug("HYBRID_STRATEGY_SKIP %s %s disabled_by_REV_ENABLED", symbol, setup_name)
            continue
        try:
            source_df = df_5m if key in {"rev_swept_rsi"} else df
            strategy_ctx = dict(ctx)
            if key in {"rev_swept_rsi"}:
                strategy_ctx["tf"] = "5"
                if indic_5m.get("ts"):
                    strategy_ctx["bar_ts_used"] = str(indic_5m.get("ts")[-1])
            intents = fn(source_df, strategy_ctx)
        except Exception as exc:  # pragma: no cover - defensive
            logging.debug("HYBRID_STRATEGY_ERROR %s %s: %s", symbol, setup_name, exc)
            continue
        for intent in intents:
            # Ensure naming consistency.
            if not intent.setup:
                intent.setup = setup_name
            before = float(intent.confidence or 0.0)
            intent.confidence = _apply_confidence_adjustment(intent, bias_info)
            all_intents.append(intent)
            logging.debug(
                "HYBRID_STRATEGY_EMIT %s %s side=%s conf=%.3f->%.3f reason=%s",
                symbol,
                intent.setup,
                intent.side,
                before,
                intent.confidence,
                intent.reason,
            )

    if not all_intents:
        return {"final_intents": [], "market_snapshot": {}, "skip_reason": "no_hybrid_intents"}

    # Sort by confidence desc and take top N.
    all_intents.sort(key=lambda it: float(it.confidence or 0.0), reverse=True)
    if top_intents_per_scan and top_intents_per_scan > 0:
        selected = all_intents[: int(top_intents_per_scan)]
    else:
        selected = all_intents

    final_intents = [it.to_signal_dict() for it in selected]
    candidates_before = [it.to_signal_dict() for it in all_intents]

    # Basic market snapshot, compatible with dashboard / volatility filters.
    snap_15 = mtf_snapshot.get(15, {}) if isinstance(mtf_snapshot, dict) else {}
    last_close = float(snap_15.get("close", indic["close"][-1] if indic["close"] else 0.0) or 0.0)
    atr14 = float(snap_15.get("atr14", indic["atr14"][-1] if indic["atr14"] else 0.0) or 0.0)
    atr_pct = (atr14 / max(last_close, 1e-10)) * 100.0 if last_close > 0 else 0.0

    market_snapshot = {
        "symbol": symbol,
        "last_close": last_close,
        "close": last_close,
        "atr14": atr14,
        "atr14_pct": atr_pct,
        "ema200": float(snap_15.get("ema200", indic["ema200"][-1] if indic["ema200"] else 0.0) or 0.0),
        "bar_ts_used": bar_ts_used,
    }

    return {
        "final_intents": final_intents,
        "candidates_before": candidates_before,
        "market_snapshot": market_snapshot,
        "skip_reason": None,
    }


