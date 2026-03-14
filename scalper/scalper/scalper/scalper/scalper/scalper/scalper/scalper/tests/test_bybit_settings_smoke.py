"""Smoke test: BybitSettings instantiates with correct default types."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_bybit_settings_defaults():
    """Clear Bybit-related env vars, instantiate BybitSettings(), assert default types."""
    for key in ("BYBIT_BASE_URL", "REQUEST_SLEEP_MS", "EXECUTION_MODE", "EXPLICIT_CONFIRM_EXECUTION"):
        os.environ.pop(key, None)

    from scalper.settings import BybitSettings

    s = BybitSettings()
    assert isinstance(s.base_url, str)
    assert isinstance(s.execution_mode, str)
    assert isinstance(s.explicit_confirm_execution, bool)
    assert isinstance(s.request_sleep_ms, int)
    assert s.base_url == "https://api.bybit.com"
    assert s.execution_mode == "disabled"
    assert s.explicit_confirm_execution is False
    assert s.request_sleep_ms == 250
