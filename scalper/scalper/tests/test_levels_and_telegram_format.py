from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_levels_compute_tp_sl_long_short() -> None:
    from scalper.levels import compute_tp_sl

    long_levels = compute_tp_sl(
        entry=100.0,
        atr=2.0,
        side="LONG",
        sl_atr_mult=1.0,
        tp_atr_mult=1.5,
    )
    assert long_levels["sl"] == 98.0
    assert long_levels["tp"] == 103.0
    assert long_levels["sl_pct"] == 2.0
    assert long_levels["tp_pct"] == 3.0

    short_levels = compute_tp_sl(
        entry=100.0,
        atr=2.0,
        side="SHORT",
        sl_atr_mult=1.0,
        tp_atr_mult=1.5,
    )
    assert short_levels["sl"] == 102.0
    assert short_levels["tp"] == 97.0
    assert short_levels["sl_pct"] == 2.0
    assert short_levels["tp_pct"] == 3.0


def test_telegram_allow_includes_levels() -> None:
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "confidence": 0.71,
        "reason": "confirmed",
        "intent_id": "id-1",
    }
    risk = {"reason": "allowed"}
    ctx = {
        "tf": "15",
        "entry": 50000.0,
        "sl": 49500.0,
        "tp": 50750.0,
        "sl_pct": 1.0,
        "tp_pct": 1.5,
        "qty": 0.01,
        "notional": 500.0,
        "levels_reason": "computed_from_atr14",
        "telegram_format": "compact",
    }
    msg = format_intent_allow(intent, risk, ctx)
    assert "entry=50000.0000" in msg
    assert "sl=49500.0000" in msg
    assert "tp=50750.0000" in msg
    assert "qty=0.010000" in msg
    assert "notional=500.00" in msg


def test_telegram_early_preview_optional() -> None:
    from telegram_format import format_early_alert

    early = {
        "symbol": "ETHUSDT",
        "side": "SHORT",
        "strategy": "RANGE_BREAKOUT_RETEST_GO",
        "confidence": 0.42,
        "bar_ts_15m": "2026-01-01T12:00:00+00:00",
        "bar_ts_5m": "2026-01-01T12:10:00+00:00",
    }

    msg_with_preview = format_early_alert(
        early,
        {
            "tf": "5",
            "preview_entry": 3000.0,
            "preview_sl": 3030.0,
            "preview_tp": 2955.0,
            "preview_reason": "ATR_PREVIEW",
            "telegram_format": "compact",
        },
    )
    assert "PREVIEW entry=3000.0000 sl=3030.0000 tp=2955.0000" in msg_with_preview

    msg_without_preview = format_early_alert(
        early,
        {
            "tf": "5",
            "preview_reason": "no atr -> no preview",
            "telegram_format": "compact",
        },
    )
    assert "preview=OFF" in msg_without_preview

