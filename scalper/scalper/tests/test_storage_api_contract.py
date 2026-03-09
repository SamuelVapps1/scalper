"""Smoke tests for stable paper-state API contract."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_scalper_storage_exports_paper_state_api():
    import scalper.storage as storage

    assert hasattr(storage, "load_paper_state")
    assert hasattr(storage, "save_paper_state")
    assert callable(storage.load_paper_state)
    assert callable(storage.save_paper_state)


def test_scalper_storage_load_returns_dict_and_save_accepts_dict():
    import scalper.storage as storage

    state = storage.load_paper_state()
    assert isinstance(state, dict)

    # Save current state back to validate call compatibility with existing callers.
    storage.save_paper_state(state)
