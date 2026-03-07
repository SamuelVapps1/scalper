"""Compatibility wrapper for project-root bybit module."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT_BYBIT = None


def _root_bybit():
    global _ROOT_BYBIT
    if _ROOT_BYBIT is None:
        root_bybit_path = Path(__file__).resolve().parent.parent / "bybit.py"
        spec = importlib.util.spec_from_file_location("_root_bybit", root_bybit_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project bybit module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ROOT_BYBIT = module
    return _ROOT_BYBIT


def __getattr__(name: str):
    return getattr(_root_bybit(), name)
