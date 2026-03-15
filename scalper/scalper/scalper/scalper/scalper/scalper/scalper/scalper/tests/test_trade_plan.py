from __future__ import annotations

from types import SimpleNamespace

from scalper.trade_plan import TradePlan, build_trade_plan


def _risk_settings(max_leverage: float = 10.0) -> SimpleNamespace:
    return SimpleNamespace(
        risk_per_trade_pct=1.0,
        max_leverage=max_leverage,
    )


def _basic_signal(side: str = "LONG") -> dict:
    return {
        "symbol": "BTCUSDT",
        "side": side,
        "strategy": "TREND_PULLBACK_EMA20",
        "timeframe": "15",
        "confidence": 0.7,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
    }


def _basic_preview(side: str = "LONG") -> dict:
    if side == "LONG":
        return {
            "ok": True,
            "reason": "",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "TREND_PULLBACK_EMA20",
            "entry": 100.0,
            "sl": 95.0,
            "tp": 110.0,
            "atr_used": 2.0,
            "atr_source": "test",
            "rr_ratio": 2.0,
            "bar_ts_used": "2026-01-01T12:00:00+00:00",
            "degraded_preview": False,
        }
    return {
        "ok": True,
        "reason": "",
        "symbol": "BTCUSDT",
        "side": "SHORT",
        "strategy": "TREND_PULLBACK_EMA20",
        "entry": 100.0,
        "sl": 105.0,
        "tp": 90.0,
        "atr_used": 2.0,
        "atr_source": "test",
        "rr_ratio": 2.0,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
        "degraded_preview": False,
    }


def _swing_candles_long() -> list[dict]:
    # Construct candles with a clear swing high above entry and higher lows.
    return [
        {"high": 98.0, "low": 94.0},
        {"high": 99.0, "low": 95.0},
        {"high": 105.0, "low": 97.0},  # swing high / resistance
        {"high": 101.0, "low": 96.0},
        {"high": 102.0, "low": 97.5},
    ]


def _swing_candles_short() -> list[dict]:
    # Construct candles with a clear swing low below entry and lower highs.
    return [
        {"high": 102.0, "low": 98.0},
        {"high": 101.0, "low": 97.0},
        {"high": 99.0, "low": 92.0},  # swing low / support
        {"high": 100.0, "low": 94.0},
        {"high": 99.5, "low": 95.0},
    ]


def test_leverage_recommendation_from_stop_distance() -> None:
    signal = _basic_signal("LONG")
    preview = _basic_preview("LONG")
    candles = _swing_candles_long()
    settings = _risk_settings(max_leverage=20.0)

    plan = build_trade_plan(
        signal=signal,
        preview=preview,
        candles=candles,
        risk_settings=settings,
        equity_usdt=1000.0,
    )
    assert isinstance(plan, TradePlan)
    assert plan.ok is True
    # Stop distance is 5% -> base leverage ~20x, capped to 20 by settings.
    assert 19.0 <= plan.leverage_recommended <= 20.0
    assert plan.leverage_cap_applied is True


def test_tp_under_first_resistance_for_long() -> None:
    signal = _basic_signal("LONG")
    preview = _basic_preview("LONG")
    candles = _swing_candles_long()
    settings = _risk_settings()

    plan = build_trade_plan(
        signal=signal,
        preview=preview,
        candles=candles,
        risk_settings=settings,
        equity_usdt=1000.0,
    )
    assert plan.ok is True
    assert plan.resistance_1 is not None
    # TP should be below resistance and above entry.
    assert plan.entry < plan.tp <= plan.resistance_1


def test_tp_above_first_support_for_short() -> None:
    signal = _basic_signal("SHORT")
    preview = _basic_preview("SHORT")
    candles = _swing_candles_short()
    settings = _risk_settings()

    plan = build_trade_plan(
        signal=signal,
        preview=preview,
        candles=candles,
        risk_settings=settings,
        equity_usdt=1000.0,
    )
    assert plan.ok is True
    assert plan.support_1 is not None
    # TP should be above support and below entry.
    assert plan.support_1 <= plan.tp < plan.entry


def test_fallback_atr_tp_if_resistance_unavailable_marks_degraded() -> None:
    signal = _basic_signal("LONG")
    preview = _basic_preview("LONG")
    # Empty candles => no swing structure.
    candles: list[dict] = []
    settings = _risk_settings()

    plan = build_trade_plan(
        signal=signal,
        preview=preview,
        candles=candles,
        risk_settings=settings,
        equity_usdt=1000.0,
    )
    assert plan.ok is True
    assert plan.degraded is True
    assert "structure_degraded_fallback_to_atr_tp" in plan.notes
    # TP falls back to preview/ATR-based target.
    assert plan.tp == preview["tp"]


def test_invalid_atr_or_entry_rejected() -> None:
    signal = _basic_signal("LONG")
    bad_preview = {
        "ok": True,
        "reason": "",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "entry": 0.0,
        "sl": 0.0,
        "tp": 0.0,
        "atr_used": 0.0,
        "atr_source": "test",
        "rr_ratio": 0.0,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
        "degraded_preview": False,
    }
    candles = _swing_candles_long()
    settings = _risk_settings()

    plan = build_trade_plan(
        signal=signal,
        preview=bad_preview,
        candles=candles,
        risk_settings=settings,
        equity_usdt=1000.0,
    )
    assert plan.ok is False
    assert plan.degraded is True
    assert plan.reason == "INVALID_LEVELS_OR_ATR"

