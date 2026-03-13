"""Smoke test: scalper.settings imports without error."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_settings_import():
    """Import scalper.settings must not raise."""
    import scalper.settings  # noqa: F401
    assert scalper.settings is not None
