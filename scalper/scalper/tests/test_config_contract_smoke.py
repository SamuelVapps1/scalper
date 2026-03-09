"""Smoke tests for scanner-facing config compatibility."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_config_has_scanner_required_watchlist_and_debug_fields():
    import config

    required = (
        "WATCHLIST_MODE",
        "WATCHLIST_UNIVERSE_N",
        "WATCHLIST_BATCH_N",
        "WATCHLIST_REFRESH_SECONDS",
        "WATCHLIST_ROTATE_MODE",
        "WATCHLIST_ROTATE_SEED",
        "WATCHLIST_MIN_TURNOVER_24H",
        "MIN_VOL_PCT",
        "MAX_VOL_PCT",
        "WATCHLIST_MIN_PRICE",
        "WATCHLIST_EXCLUDE_PREFIXES",
        "WATCHLIST_EXCLUDE_SYMBOLS",
        "WATCHLIST_EXCLUDE_REGEX",
        "WATCHLIST_MAX_SPREAD_BPS",
        "SIGNAL_DEBUG",
    )
    for name in required:
        assert hasattr(config, name), f"missing config attribute: {name}"
