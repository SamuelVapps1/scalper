from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .trade_plan import TradePlan


@dataclass(frozen=True)
class RankedPlan:
    plan: TradePlan
    score: float
    rank_index: int
    features: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        d = self.plan.as_dict()
        d.update(
            {
                "score": self.score,
                "rank_index": self.rank_index,
                "score_features": dict(self.features),
            }
        )
        return d


def _compute_atr_pct(plan: TradePlan) -> float:
    if plan.entry <= 0 or plan.atr14_used <= 0:
        return 0.0
    return (plan.atr14_used / max(plan.entry, 1e-10)) * 100.0


def score_trade_plan(
    plan: TradePlan,
    *,
    bias: str = "",
    min_rr: float = 1.0,
    max_rr: float = 4.0,
    min_atr_pct: float = 0.1,
    max_atr_pct: float = 10.0,
) -> Tuple[float, Dict[str, Any]]:
    """
    Compute a scalar score for a trade plan based on:
      - risk/reward quality
      - confidence
      - trend alignment
      - volatility quality (ATR%)
      - structure quality (R1/S1)
      - degradation flags
    Pure function; does not mutate the plan.
    """
    features: Dict[str, Any] = {}
    score = 0.0

    # Risk/reward quality.
    rr = max(0.0, float(plan.rr))
    rr_clamped = max(0.0, min(rr, max_rr))
    rr_quality = max(0.0, rr_clamped - min_rr)
    score += rr_quality * 1.5
    features["rr"] = rr
    features["rr_quality"] = rr_quality

    # Confidence.
    conf = max(0.0, float(plan.confidence))
    score += conf * 1.0
    features["confidence"] = conf

    # Trend alignment (bias vs side).
    bias_norm = str(bias or "").upper()
    side_norm = str(plan.side or "").upper()
    trend_align = 0.0
    if bias_norm in {"LONG", "SHORT"} and side_norm in {"LONG", "SHORT"}:
        if bias_norm == side_norm:
            trend_align = 1.0
        else:
            trend_align = -0.5
    score += trend_align * 0.75
    features["trend_align"] = trend_align
    features["bias"] = bias_norm

    # Volatility / ATR%.
    atr_pct = _compute_atr_pct(plan)
    vol_quality = 0.0
    if min_atr_pct <= atr_pct <= max_atr_pct:
        vol_quality = 1.0
    elif atr_pct > 0:
        vol_quality = -0.5
    score += vol_quality * 0.5
    features["atr_pct"] = atr_pct
    features["vol_quality"] = vol_quality

    # Structure quality.
    has_res = plan.resistance_1 is not None
    has_sup = plan.support_1 is not None
    structure_quality = 0.0
    if has_res and has_sup:
        structure_quality = 1.0
    elif has_res or has_sup:
        structure_quality = 0.5
    score += structure_quality * 0.75
    features["structure_quality"] = structure_quality

    # Degradation penalty.
    degraded_penalty = -1.0 if plan.degraded else 0.0
    score += degraded_penalty
    features["degraded"] = plan.degraded
    features["degraded_penalty"] = degraded_penalty

    return float(score), features


def rank_plans(
    plans: List[TradePlan],
    *,
    bias_by_symbol: Dict[str, str] | None = None,
    min_rr: float = 1.0,
    max_rr: float = 4.0,
    min_atr_pct: float = 0.1,
    max_atr_pct: float = 10.0,
) -> List[RankedPlan]:
    """
    Rank a list of TradePlans, highest score first.
    Does not filter; filtering is handled separately so ranking can be feature-flagged.
    """
    bias_by_symbol = bias_by_symbol or {}

    scored: List[Tuple[TradePlan, float, Dict[str, Any]]] = []
    for plan in plans:
        bias = bias_by_symbol.get(plan.symbol, "")
        score, feats = score_trade_plan(
            plan,
            bias=bias,
            min_rr=min_rr,
            max_rr=max_rr,
            min_atr_pct=min_atr_pct,
            max_atr_pct=max_atr_pct,
        )
        scored.append((plan, score, feats))

    scored.sort(key=lambda tpl: tpl[1], reverse=True)
    ranked: List[RankedPlan] = []
    for idx, (plan, score, feats) in enumerate(scored):
        ranked.append(RankedPlan(plan=plan, score=score, rank_index=idx, features=feats))
    return ranked


def select_top_plans(
    ranked: List[RankedPlan],
    *,
    max_candidates_per_scan: int = 0,
    max_allow_per_scan: int = 0,
) -> Tuple[List[RankedPlan], List[RankedPlan]]:
    """
    Optionally truncate ranked plans for analytics or hard filtering.
    With max_* = 0, returns ranked unchanged.
    """
    if max_candidates_per_scan and max_candidates_per_scan > 0:
        ranked = ranked[: max_candidates_per_scan]
    if max_allow_per_scan and max_allow_per_scan > 0:
        accepted = ranked[: max_allow_per_scan]
        rejected = ranked[max_allow_per_scan:]
    else:
        accepted = ranked
        rejected: List[RankedPlan] = []
    return accepted, rejected

