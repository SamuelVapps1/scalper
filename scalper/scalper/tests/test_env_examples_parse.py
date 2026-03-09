"""
Parse all canonical .env.example files without ValidationError.
Loads settings from package root (same settings.py as env hardening tests).
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

_PKG_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = _PKG_ROOT.parent
_SETTINGS_PY = _PKG_ROOT / "settings.py"
_env_examples_settings_mod = None


def _get_settings():
    global _env_examples_settings_mod
    if _env_examples_settings_mod is None:
        spec = importlib.util.spec_from_file_location("_env_examples_settings", _SETTINGS_PY)
        _env_examples_settings_mod = importlib.util.module_from_spec(spec)
        sys.modules["_env_examples_settings"] = _env_examples_settings_mod
        spec.loader.exec_module(_env_examples_settings_mod)
    return _env_examples_settings_mod.get_settings


def _load_env_file(path: Path) -> dict:
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _apply_env(env_dict: dict):
    for k, v in env_dict.items():
        os.environ[k] = str(v)


@pytest.fixture
def clear_settings_cache():
    try:
        _get_settings().cache_clear()
    except Exception:
        pass


def test_canonical_env_example_parses():
    """Repo root .env.example must parse without ValidationError."""
    canonical = REPO_ROOT / ".env.example"
    if not canonical.exists():
        pytest.skip("No .env.example at repo root")
    before = dict(os.environ)
    try:
        os.environ["ENV_PATH"] = str(canonical.resolve())
        get_settings = _get_settings()
        get_settings.cache_clear()
        s = get_settings()
    except Exception as e:
        raise AssertionError(f"Parsing .env.example must not raise: {e}") from e
    finally:
        os.environ.clear()
        os.environ.update(before)
        try:
            get_settings.cache_clear()
        except Exception:
            pass


def test_scalper_env_example_parses_if_present():
    """scalper/.env.example if present must parse without ValidationError."""
    path = REPO_ROOT / "scalper" / ".env.example"
    if not path.exists():
        pytest.skip("No scalper/.env.example")
    before = dict(os.environ)
    try:
        os.environ["ENV_PATH"] = str(path.resolve())
        get_settings = _get_settings()
        get_settings.cache_clear()
        s = get_settings()
    except Exception as e:
        raise AssertionError(f"Parsing scalper/.env.example must not raise: {e}") from e
    finally:
        os.environ.clear()
        os.environ.update(before)
        try:
            get_settings.cache_clear()
        except Exception:
            pass
