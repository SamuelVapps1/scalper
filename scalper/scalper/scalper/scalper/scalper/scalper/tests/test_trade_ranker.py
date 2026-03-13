from __future__ import annotations

from types import SimpleNamespace

from scalper.trade_plan import TradePlan
from scalper.trade_ranker import rank_plans, score_trade_plan, select_top_plans


def _plan(
    *,
    rr: float,
    confidence: float,
    side: str = "LONG",
    degraded: bool = False,
    atr14_used: float = 2.0,
    entry: float = 100.0,
    resistance_1: float | None = 110.0,
    support_1: float | None = 95.0,
) -> TradePlan:
    return TradePlan(
        ok=True,
        reason="",
        symbol="BTCUSDT",
        side=side,
        strategy="TEST",
        timeframe="15",
        confidence=confidence,
        entry=entry,
        stop=95.0,
        tp=110.0,
        rr=rr,
        risk_pct=1.0,
        stop_distance_pct=5.0,
        leverage_recommended=5.0,
        leverage_cap_applied=False,
        position_value_usdt=100.0,
        qty_est=1.0,
        notional_est=100.0,
        resistance_1=resistance_1,
        support_1=support_1,
        atr14_used=atr14_used,
        atr_source="test",
        degraded=degraded,
        execution_ready=True,
        bar_ts_used="2026-01-01T12:00:00+00:00",
        notes=[],
    )


def test_ranking_sort_order() -> None:
    good = _plan(rr=3.0, confidence=0.8)
    ok = _plan(rr=2.0, confidence=0.6)
    weak = _plan(rr=1.1, confidence=0.4, degraded=True)

    ranked = rank_plans([weak, ok, good], bias_by_symbol={"BTCUSDT": "LONG"})
    assert ranked[0].plan is good
    assert ranked[-1].plan is weak


def test_top_n_truncation() -> None:
    plans = [
        _plan(rr=2.0, confidence=0.5),
        _plan(rr=3.0, confidence=0.7),
        _plan(rr=1.5, confidence=0.4),
    ]
    ranked = rank_plans(plans, bias_by_symbol={"BTCUSDT": "LONG"})
    top, rest = select_top_plans(ranked, max_candidates_per_scan=3, max_allow_per_scan=1)
    assert len(top) == 1
    assert len(rest) == 2
    assert top[0].score >= rest[0].score


def test_poor_rr_penalized_in_score() -> None:
    good = _plan(rr=3.0, confidence=0.6)
    poor = _plan(rr=0.8, confidence=0.6)
    good_score, _ = score_trade_plan(good, min_rr=1.0, max_rr=4.0)
    poor_score, _ = score_trade_plan(poor, min_rr=1.0, max_rr=4.0)
    assert good_score > poor_score


def test_chop_low_volatility_penalized() -> None:
    normal = _plan(rr=2.0, confidence=0.6, atr14_used=2.0, entry=100.0)
    # Very low ATR% relative to price -> should be penalized.
    chop = _plan(rr=2.0, confidence=0.6, atr14_used=0.01, entry=100.0)
    normal_score, _ = score_trade_plan(normal, min_atr_pct=0.1, max_atr_pct=10.0)
    chop_score, _ = score_trade_plan(chop, min_atr_pct=0.1, max_atr_pct=10.0)
    assert normal_score > chop_score

