from __future__ import annotations

from types import SimpleNamespace

from scalper.paper_broker import PaperBroker
from scalper.scanner import _apply_preview_gate
from scalper.telegram_format import format_intent_allow
from scalper.trade_preview import build_trade_preview


def _risk_settings() -> SimpleNamespace:
    return SimpleNamespace(
        paper_sl_atr=1.0,
        paper_tp_atr=1.5,
        risk_per_trade_pct=0.15,
        paper_start_equity_usdt=1000.0,
        tf_trigger=15,
        preview_min_rr=1.1,
        preview_min_atr_pct=0.01,
        preview_max_atr_pct=20.0,
        preview_max_retest_drift_pct=1.0,
        paper_slippage_pct=0.01,
        paper_fee_pct=0.055,
    )


def test_preview_builder_returns_valid_levels_normal_case() -> None:
    preview = build_trade_preview(
        signal={
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "TREND_PULLBACK_EMA20",
            "entry_type": "market_sim",
            "meta": {"atr14": 120.0},
        },
        market_snapshot={"close": 50000.0, "atr14": 120.0, "bar_ts_used": "2026-01-01T12:00:00+00:00"},
        candles=[{"close": 50000.0, "high": 50120.0, "low": 49890.0, "timestamp_utc": "2026-01-01T12:00:00+00:00"}],
        mtf_snapshot={15: {"atr14": 120.0}},
        risk_settings=_risk_settings(),
        equity_usdt=1000.0,
        for_execution=True,
    )
    assert preview["ok"] is True
    assert preview["entry"] > 0
    assert preview["sl"] > 0
    assert preview["tp"] > 0
    assert preview["sl_pct"] > 0
    assert preview["tp_pct"] > 0


def test_preview_builder_rejects_invalid_atr_case() -> None:
    preview = build_trade_preview(
        signal={
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "TREND_PULLBACK_EMA20",
            "entry_type": "market_sim",
            "meta": {},
        },
        market_snapshot={"close": 50000.0, "atr14": 0.0, "bar_ts_used": "2026-01-01T12:00:00+00:00"},
        candles=[],
        mtf_snapshot={15: {"atr14": 0.0}},
        risk_settings=_risk_settings(),
        equity_usdt=1000.0,
        for_execution=True,
    )
    assert preview["ok"] is False
    assert preview["reason"] in {"ATR_UNAVAILABLE", "ATR_DEGRADED"}


def test_allow_formatter_never_prints_na_for_valid_allow() -> None:
    msg = format_intent_allow(
        {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "TREND_PULLBACK_EMA20",
            "confidence": 0.77,
        },
        {"reason": "allowed"},
        {
            "tf": "15",
            "entry": 50000.0,
            "sl": 49880.0,
            "tp": 50180.0,
            "sl_pct": 0.24,
            "tp_pct": 0.36,
            "qty": 0.012,
            "notional": 600.0,
            "preview_status": "ok",
            "execution_status": "not_opened",
            "bar_ts_used": "2026-01-01T12:00:00+00:00",
            "telegram_format": "compact",
        },
    )
    assert "entry=n/a" not in msg
    assert "sl=n/a" not in msg
    assert "tp=n/a" not in msg
    assert "preview_status=ok" in msg


def test_scanner_downgrades_invalid_preview_to_block() -> None:
    allowed, reason = _apply_preview_gate(True, "allowed", {"ok": False, "reason": "LEVELS_UNAVAILABLE"})
    assert allowed is False
    assert reason == "LEVELS_UNAVAILABLE"


def test_paper_open_and_allow_use_same_preview_levels() -> None:
    settings = _risk_settings()
    preview = build_trade_preview(
        signal={
            "symbol": "ETHUSDT",
            "side": "SHORT",
            "strategy": "RANGE_BREAKOUT_RETEST_GO",
            "entry_type": "market_sim",
            "meta": {"atr14": 12.0, "sl_hint": 3512.0},
        },
        market_snapshot={"close": 3500.0, "atr14": 12.0, "bar_ts_used": "2026-01-01T12:00:00+00:00"},
        candles=[{"close": 3500.0, "high": 3510.0, "low": 3490.0, "timestamp_utc": "2026-01-01T12:00:00+00:00"}],
        mtf_snapshot={15: {"atr14": 12.0}},
        risk_settings=settings,
        equity_usdt=1000.0,
        for_execution=True,
    )
    assert preview["ok"] is True

    store = SimpleNamespace(
        load_paper_state=lambda: {"daily_pnl_realized": 0.0, "equity_peak": 1000.0},
        upsert_paper_position=lambda _pos: None,
        insert_paper_trade=lambda _trade: None,
        delete_paper_position=lambda _intent_id: None,
    )
    broker = PaperBroker(store=store, risk_settings=settings)
    pos_dict, err = broker.open_from_preview(
        preview=preview,
        intent_id="intent-1",
        ts="2026-01-01T12:00:00+00:00",
        strategy="RANGE_BREAKOUT_RETEST_GO",
    )
    assert err is None
    assert pos_dict is not None
    assert float(pos_dict["sl_price"]) == float(preview["sl"])
    assert float(pos_dict["tp_price"]) == float(preview["tp"])
