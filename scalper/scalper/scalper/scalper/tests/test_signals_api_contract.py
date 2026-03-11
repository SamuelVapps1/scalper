"""Smoke tests for stable signals API contract used by scanner."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_scalper_signals_exports_early_eval_function():
    import scalper.signals as signals

    assert hasattr(signals, "evaluate_early_intents_from_5m")
    assert callable(signals.evaluate_early_intents_from_5m)
