"""Compatibility wrapper for project-root signals module."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT_SIGNALS = None


def _root_signals():
    global _ROOT_SIGNALS
    if _ROOT_SIGNALS is None:
        root_signals_path = Path(__file__).resolve().parent.parent / "signals.py"
        spec = importlib.util.spec_from_file_location("_root_signals", root_signals_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project signals module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ROOT_SIGNALS = module
    return _ROOT_SIGNALS


def __getattr__(name: str):
    return getattr(_root_signals(), name)
