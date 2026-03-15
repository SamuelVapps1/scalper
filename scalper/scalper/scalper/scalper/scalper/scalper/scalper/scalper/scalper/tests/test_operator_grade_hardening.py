from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_format_signal_alert_contains_enriched_fields() -> None:
    from telegram_format import format_signal_alert

    msg = format_signal_alert(
        {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "strategy": "RANGE_BREAKOUT_RETEST_GO",
            "reason": "Range breakout -> retest -> go",
            "confidence": 0.74,
            "confidence_source": "confidence",
            "entry": 65780.0,
            "sl": 65466.33,
            "tp": 66250.49,
            "sl_pct": 0.48,
            "tp_pct": 0.72,
        },
        {"tf": "15", "telegram_format": "compact"},
    )
    assert "entry=" in msg
    assert "sl=" in msg
    assert "tp=" in msg
    assert "conf=" in msg
    assert "mode=DRY_RUN" in msg


def test_settings_empty_watchlist_min_price_is_safe(monkeypatch) -> None:
    from scalper.settings import get_settings

    monkeypatch.setenv("WATCHLIST_MIN_PRICE", "")
    monkeypatch.setenv("TELEGRAM_POLICY", "both")
    get_settings.cache_clear()
    settings = get_settings()
    assert settings.risk.watchlist_min_price == 0.0
    assert settings.telegram.policy == "both"


def test_policy_filtering_for_signals_mode() -> None:
    from scalper.notifier import _policy_allows_kind

    assert _policy_allows_kind("signals", "signal") is True
    assert _policy_allows_kind("signals", "intent") is False
    assert _policy_allows_kind("events", "intent") is True
