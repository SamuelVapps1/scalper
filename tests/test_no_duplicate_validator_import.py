"""Regression: config must not cause duplicate Pydantic validator (single canonical scalper.settings load)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_no_duplicate_validator_import():
    """Import config and reload config must not raise pydantic ConfigError (duplicate validator).
    Runs in subprocess with cwd=repo root so scalper.settings is loaded exactly once via normal import.
    """
    code = """
import scalper.settings
import config
import importlib
importlib.reload(config)
print("ok")
"""
    r = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    assert "ok" in r.stdout
    assert "ConfigError" not in r.stderr
    assert "duplicate validator" not in r.stderr
