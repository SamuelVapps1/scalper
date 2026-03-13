"""Tests for market watchlist 200/20 rotation and config/alias stability."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_market_rotation_200_20(monkeypatch):
    """WATCHLIST_MODE=market uses universe 200, batch 20, round-robin from Bybit tickers."""
    # Mock Bybit to return 250 symbols; we take top 200 then rotate to 20
    fake_symbols = [f"SYM{i}USDT" for i in range(250)]

    def fake_get_top(_n, **kwargs):
        return fake_symbols[:_n]

    monkeypatch.setattr("bybit.get_top_linear_usdt_symbols", fake_get_top)

    with tempfile.TemporaryDirectory() as tmp:
        state_file = Path(tmp) / "state.json"
        cfg = SimpleNamespace(
            WATCHLIST_MODE="market",
            WATCHLIST=["BTCUSDT", "ETHUSDT"],
            WATCHLIST_UNIVERSE_N=200,
            WATCHLIST_BATCH_N=20,
            WATCHLIST_REFRESH_SECONDS=900,
            WATCHLIST_ROTATE_MODE="roundrobin",
            WATCHLIST_ROTATE_SEED=0,
            WATCHLIST_MIN_TURNOVER_24H=0.0,
            WATCHLIST_MIN_PRICE=0.0,
            WATCHLIST_MAX_SPREAD_BPS=0.0,
            WATCHLIST_EXCLUDE_PREFIXES=[],
            WATCHLIST_EXCLUDE_SYMBOLS=[],
            WATCHLIST_EXCLUDE_REGEX="",
            ROTATION_STATE_FILE=str(state_file),
        )

        from watchlist import WatchlistManager

        manager = WatchlistManager(cfg)
        symbols, source = manager.get_watchlist()

        assert source == "market"
        assert len(symbols) == 20
        assert symbols == fake_symbols[:20]

        # Next call should rotate (round-robin)
        symbols2, _ = manager.get_watchlist()
        assert len(symbols2) == 20
        assert symbols2 == fake_symbols[20:40]


def test_alias_env_vars_map_correctly(monkeypatch):
    """Legacy env vars MIN_24H_TURNOVER, UNIVERSE_SIZE, BATCH_SIZE, WATCHLIST_REFRESH_MINUTES map to new fields when primary vars are unset."""
    import config  # noqa: F401 - ensures scalper.settings is loaded/registered
    import scalper.settings as settings_mod

    if hasattr(settings_mod.get_settings, "cache_clear"):
        settings_mod.get_settings.cache_clear()

    # Prevent .env from overwriting our env so alias mapping is visible
    monkeypatch.setattr(settings_mod, "load_dotenv", lambda *a, **k: False)

    # Unset primary vars so aliases apply
    for key in ("WATCHLIST_MIN_TURNOVER_24H", "WATCHLIST_UNIVERSE_N", "WATCHLIST_BATCH_N", "WATCHLIST_REFRESH_SECONDS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MIN_24H_TURNOVER", "5000000")
    monkeypatch.setenv("UNIVERSE_SIZE", "100")
    monkeypatch.setenv("BATCH_SIZE", "10")
    monkeypatch.setenv("WATCHLIST_REFRESH_MINUTES", "15")

    try:
        s = settings_mod.get_settings()
        r = s.risk
        assert r.watchlist_min_turnover_24h == 5000000.0, "MIN_24H_TURNOVER should map to watchlist_min_turnover_24h"
        assert r.watchlist_universe_n == 100, "UNIVERSE_SIZE should map to watchlist_universe_n"
        assert r.watchlist_batch_n == 10, "BATCH_SIZE should map to watchlist_batch_n"
        assert r.watchlist_refresh_seconds == 15 * 60, "WATCHLIST_REFRESH_MINUTES should map to watchlist_refresh_seconds"
    finally:
        for key in ("MIN_24H_TURNOVER", "UNIVERSE_SIZE", "BATCH_SIZE", "WATCHLIST_REFRESH_MINUTES"):
            monkeypatch.delenv(key, raising=False)
        if hasattr(settings_mod.get_settings, "cache_clear"):
            settings_mod.get_settings.cache_clear()


def test_static_mode_no_crash_without_market_vars():
    """Static mode with WATCHLIST set does not AttributeError on config."""
    from types import SimpleNamespace

    cfg = SimpleNamespace(
        WATCHLIST_MODE="static",
        WATCHLIST="BTCUSDT,ETHUSDT",
        WATCHLIST_UNIVERSE_N=200,
        WATCHLIST_BATCH_N=20,
        WATCHLIST_REFRESH_SECONDS=900,
        WATCHLIST_ROTATE_MODE="roundrobin",
        WATCHLIST_ROTATE_SEED=0,
        ROTATION_STATE_FILE="state.json",
        WATCHLIST_MIN_TURNOVER_24H=0.0,
        WATCHLIST_MIN_PRICE=0.0,
        WATCHLIST_MAX_SPREAD_BPS=0.0,
        WATCHLIST_EXCLUDE_PREFIXES=[],
        WATCHLIST_EXCLUDE_SYMBOLS=[],
        WATCHLIST_EXCLUDE_REGEX="",
    )

    from watchlist import get_watchlist

    symbols, source = get_watchlist(cfg)
    assert source == "static"
    assert symbols == ["BTCUSDT", "ETHUSDT"]
