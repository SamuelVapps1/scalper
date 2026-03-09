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
        "WATCHLIST",
        "WATCHLIST_UNIVERSE_N",
        "WATCHLIST_BATCH_N",
        "WATCHLIST_REFRESH_SECONDS",
        "WATCHLIST_ROTATE_MODE",
        "WATCHLIST_ROTATE_SEED",
        "ROTATION_STATE_FILE",
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


def test_config_exports_exist():
    """Config module must export all scanner-facing names without AttributeError."""
    import config

    # Static/watchlist
    _ = config.WATCHLIST_MODE
    _ = config.WATCHLIST
    _ = config.WATCHLIST_UNIVERSE_N
    _ = config.WATCHLIST_BATCH_N
    _ = config.WATCHLIST_REFRESH_SECONDS
    _ = config.ROTATION_STATE_FILE
    _ = config.WATCHLIST_MIN_TURNOVER_24H
    _ = config.WATCHLIST_MIN_PRICE
    _ = config.WATCHLIST_MAX_SPREAD_BPS
    _ = config.WATCHLIST_EXCLUDE_PREFIXES
    _ = config.WATCHLIST_EXCLUDE_SYMBOLS
    _ = config.WATCHLIST_EXCLUDE_REGEX
    # Scan
    _ = config.INTERVAL
    _ = config.SCAN_SECONDS
    _ = config.LOOKBACK
