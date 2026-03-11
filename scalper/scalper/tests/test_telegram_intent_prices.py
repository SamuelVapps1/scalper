"""Tests for entry/sl/tp in Telegram ALLOW intent alerts."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_format_intent_allow_long_includes_entry_sl_tp():
    """LONG intent with populated market_ctx prints entry, sl, tp (not n/a)."""
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "confidence": 0.72,
        "reason": "V2 trend pullback to EMA20",
    }
    risk = {"reason": "allowed"}
    ctx = {
        "tf": "15",
        "entry": 65780.0,
        "sl": 65466.33,
        "tp": 66250.49,
        "sl_pct": 0.48,
        "tp_pct": 0.72,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
    }
    msg = format_intent_allow(intent, risk, ctx)
    assert "entry=65780" in msg
    assert "sl=65466" in msg
    assert "tp=66250" in msg
    assert "entry=n/a" not in msg
    assert "sl=n/a" not in msg
    assert "tp=n/a" not in msg


def test_format_intent_allow_short_includes_entry_sl_tp():
    """SHORT intent with populated market_ctx prints entry, sl, tp (not n/a)."""
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "ETHUSDT",
        "side": "SHORT",
        "strategy": "V3_TREND_BREAKOUT",
        "confidence": 0.68,
        "reason": "V3 breakout",
    }
    risk = {"reason": "allowed"}
    ctx = {
        "tf": "15",
        "entry": 3450.25,
        "sl": 3480.50,
        "tp": 3395.00,
        "sl_pct": 0.88,
        "tp_pct": 1.60,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
    }
    msg = format_intent_allow(intent, risk, ctx)
    assert "entry=3450" in msg
    assert "sl=3480" in msg
    assert "tp=3395" in msg
    assert "entry=n/a" not in msg
    assert "sl=n/a" not in msg
    assert "tp=n/a" not in msg


def test_format_intent_allow_blocks_when_levels_missing():
    """ALLOW formatter downgrades to BLOCK when sl/tp are unavailable."""
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "confidence": 0.70,
    }
    risk = {"reason": "allowed"}
    ctx = {
        "tf": "15",
        "bar_close": 65800.5,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
    }
    msg = format_intent_allow(intent, risk, ctx)
    assert msg.startswith("BLOCK[15m]")
    assert "risk_reason=allowed" in msg


def test_format_intent_allow_includes_recommended_sl_when_meta_present():
    """format_intent_allow includes rec_sl when recommended_sl_price in intent.meta."""
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "confidence": 0.70,
        "meta": {"recommended_sl_price": 65000.0},
    }
    risk = {"reason": "allowed"}
    ctx = {
        "tf": "15",
        "entry": 65780.0,
        "sl": 65466.33,
        "tp": 66250.49,
        "sl_pct": 0.48,
        "tp_pct": 0.72,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
        "telegram_format": "verbose",
    }
    msg = format_intent_allow(intent, risk, ctx)
    assert "rec_sl=65000" in msg


def test_compute_entry_sl_tp_for_display_returns_values():
    """compute_entry_sl_tp_for_display returns entry/sl/tp when candle and ATR available."""
    from paper_engine import compute_entry_sl_tp_for_display

    trade_intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "direction": "LONG",
        "strategy": "TREND_PULLBACK_EMA20",
        "meta": {},
    }
    candles = [{"close": 65780.0, "low": 65600, "high": 65900, "timestamp_utc": "2026-01-01T12:00:00Z"}]
    snapshot = {"atr14": 350.0}
    result = compute_entry_sl_tp_for_display(
        trade_intent, candles, snapshot, sl_atr_mult=1.0, tp_atr_mult=1.5
    )
    assert result is not None
    assert result["entry"] > 0
    assert result["sl"] > 0
    assert result["tp"] > 0
    assert result["sl_pct"] > 0
    assert result["tp_pct"] > 0
    assert result["sl"] < result["entry"]  # LONG: sl below entry
    assert result["tp"] > result["entry"]  # LONG: tp above entry
