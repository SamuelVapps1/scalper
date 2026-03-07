"""Compatibility wrapper for the project-root watchlist module."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Optional

_ROOT_WATCHLIST = None


def _root_watchlist():
    global _ROOT_WATCHLIST
    if _ROOT_WATCHLIST is None:
        root_watchlist_path = Path(__file__).resolve().parent.parent / "watchlist.py"
        spec = importlib.util.spec_from_file_location("_root_watchlist", root_watchlist_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project watchlist module")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _ROOT_WATCHLIST = mod
    return _ROOT_WATCHLIST


def get_watchlist(config: Any, bybit_client: Optional[Any] = None, logger: Optional[Any] = None):
    return _root_watchlist().get_watchlist(config, bybit_client=bybit_client, logger=logger)


if hasattr(_root_watchlist(), "WatchlistManager"):
    WatchlistManager = _root_watchlist().WatchlistManager
else:
    class WatchlistManager:
        """Minimal backward-compatible manager wrapper."""

        def __init__(self, config: Any):
            self.config = config

        def get_watchlist(self, bybit_client: Optional[Any] = None, logger: Optional[Any] = None):
            return get_watchlist(self.config, bybit_client=bybit_client, logger=logger)


__all__ = ["WatchlistManager", "get_watchlist"]
