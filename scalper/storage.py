<<<<<<< HEAD
import csv
from pathlib import Path
from typing import Dict, Any

CSV_PATH = Path("signals_log.csv")
CSV_HEADERS = ["timestamp_utc", "symbol", "setup", "direction", "close", "reason"]


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
=======
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Dict

_ROOT_STORAGE = None


def _root_storage():
    global _ROOT_STORAGE
    if _ROOT_STORAGE is None:
        root_storage_path = Path(__file__).resolve().parents[2] / "storage.py"
        spec = importlib.util.spec_from_file_location("_root_storage", root_storage_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Unable to resolve project storage module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ROOT_STORAGE = module
    return _ROOT_STORAGE


def load_paper_state() -> Dict[str, Any]:
    state = _root_storage().load_paper_state()
    return state if isinstance(state, dict) else {}


def save_paper_state(state: Dict[str, Any]) -> None:
    _root_storage().save_paper_state(state if isinstance(state, dict) else {})


def __getattr__(name: str):
    return getattr(_root_storage(), name)
>>>>>>> 687e22dccb4ca354fd3fb211e4c4c4cb9c7b2313
