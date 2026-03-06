from __future__ import annotations

import csv
import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict

CSV_PATH = Path("signals_log.csv")
CSV_HEADERS = ["timestamp_utc", "symbol", "setup", "direction", "close", "reason"]

_LOG = logging.getLogger(__name__)
_root_storage_module = None
_load_fail_logged = False
_save_fail_logged = False


def append_signal(signal: Dict[str, Any]) -> None:
    file_exists = CSV_PATH.exists()

    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": signal.get("timestamp_utc", ""),
                "symbol": signal.get("symbol", ""),
                "setup": signal.get("setup", ""),
                "direction": signal.get("direction", ""),
                "close": signal.get("close", ""),
                "reason": signal.get("reason", ""),
            }
        )


def _root_storage():
    """Load the project-root storage module (paper state implementation)."""
    global _root_storage_module
    if _root_storage_module is None:
        root_storage_path = Path(__file__).resolve().parent.parent / "storage.py"
        spec = importlib.util.spec_from_file_location("_root_storage", root_storage_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project storage module")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _root_storage_module = mod
    return _root_storage_module


def load_paper_state() -> Dict[str, Any]:
    global _load_fail_logged
    try:
        state = _root_storage().load_paper_state()
        return state if isinstance(state, dict) else {}
    except Exception as exc:
        if not _load_fail_logged:
            _LOG.info("paper_state_load_failed error=%s", exc.__class__.__name__)
            _load_fail_logged = True
        return {}


def save_paper_state(state: Dict[str, Any]) -> None:
    global _save_fail_logged
    try:
        module = _root_storage()
        # Ensure canonical persistence directory exists (sqlite path in root storage).
        resolve_db_path = getattr(module, "_resolve_db_path", None)
        if callable(resolve_db_path):
            Path(resolve_db_path()).parent.mkdir(parents=True, exist_ok=True)
        module.save_paper_state(state if isinstance(state, dict) else {})
    except Exception as exc:
        if not _save_fail_logged:
            _LOG.info("paper_state_save_failed error=%s", exc.__class__.__name__)
            _save_fail_logged = True


def __getattr__(name: str):
    """Delegate missing attributes to canonical root storage module."""
    return getattr(_root_storage(), name)
