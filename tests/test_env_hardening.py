"""
Regression: Settings must not crash on common .env formatting (empty, comments, null/none).
Uses the settings module in the same package as this test (parent of tests/).
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

# Package root = directory containing settings.py and tests/
_PKG_ROOT = Path(__file__).resolve().parent.parent
_SETTINGS_PY = _PKG_ROOT / "settings.py"
_env_test_settings_mod = None


def _get_settings():
    """Load get_settings from settings.py in package root (avoids wrong scalper package)."""
    global _env_test_settings_mod
    if _env_test_settings_mod is None:
        spec = importlib.util.spec_from_file_location("_env_test_settings", _SETTINGS_PY)
        _env_test_settings_mod = importlib.util.module_from_spec(spec)
        sys.modules["_env_test_settings"] = _env_test_settings_mod
        spec.loader.exec_module(_env_test_settings_mod)
    return _env_test_settings_mod.get_settings


def _clear_settings_cache():
    try:
        get_settings = _get_settings()
        get_settings.cache_clear()
    except Exception:
        pass


def _set_env(env_dict: dict):
    for k, v in env_dict.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = str(v)


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    """Restore env after each test (avoid leaking WATCHLIST_UNIVERSE_N etc.)."""
    before = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(before)
    _clear_settings_cache()


def test_watchlist_universe_n_empty_uses_default():
    """WATCHLIST_UNIVERSE_N= (empty) must load without error and yield default 200."""
    _clear_settings_cache()
    _set_env({"WATCHLIST_UNIVERSE_N": ""})
    s = _get_settings()()
    assert s.risk.watchlist_universe_n == 200


def test_watchlist_universe_n_inline_comment_parsed():
    """WATCHLIST_UNIVERSE_N='200 # pool size' must yield 200."""
    _clear_settings_cache()
    _set_env({"WATCHLIST_UNIVERSE_N": "200 # pool size"})
    s = _get_settings()()
    assert s.risk.watchlist_universe_n == 200


def test_watchlist_universe_n_null_uses_default():
    """WATCHLIST_UNIVERSE_N=null or none must use default 200."""
    _clear_settings_cache()
    for val in ("null", "none", "None", "  null  "):
        _set_env({"WATCHLIST_UNIVERSE_N": val})
        _clear_settings_cache()
        s = _get_settings()()
        assert s.risk.watchlist_universe_n == 200, f"Expected 200 for value {val!r}"


def test_alias_universe_size_and_batch_size():
    """UNIVERSE_SIZE=200 and BATCH_SIZE=20 (no canonical vars) -> universe_n=200, batch_n=20."""
    _clear_settings_cache()
    _set_env({
        "UNIVERSE_SIZE": "200",
        "BATCH_SIZE": "20",
        "WATCHLIST_UNIVERSE_N": None,
        "WATCHLIST_BATCH_N": None,
    })
    s = _get_settings()()
    assert s.risk.watchlist_universe_n == 200
    assert s.risk.watchlist_batch_n == 20


def test_watchlist_raw_fallback_when_watchlist_empty():
    """When WATCHLIST is empty, WATCHLIST_RAW is used for watchlist_raw."""
    _clear_settings_cache()
    _set_env({"WATCHLIST": "", "WATCHLIST_RAW": "BTCUSDT,ETHUSDT"})
    s = _get_settings()()
    # watchlist is parsed from watchlist_raw (WATCHLIST or WATCHLIST_RAW)
    assert "BTCUSDT" in s.risk.watchlist or "ETHUSDT" in s.risk.watchlist
