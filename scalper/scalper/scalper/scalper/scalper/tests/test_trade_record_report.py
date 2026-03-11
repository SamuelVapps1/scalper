"""Strategy validation lab: TradeRecord roundtrip and --report on empty DB."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

# ROOT = scalper package dir; REPO_ROOT = project root (parent of scalper)
ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = ROOT.parent
# Scanner entry: python scalper/scanner.py -> REPO_ROOT/scalper/scanner.py
SCANNER = ROOT / "scanner.py"
if not SCANNER.exists():
    SCANNER = REPO_ROOT / "scanner.py"


def test_trade_record_insert_update_roundtrip() -> None:
    """Insert a TradeRecord, update on close, then assert one closed record with expected fields."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        env = os.environ.copy()
        env["DB_PATH"] = str(db_path)
        code = """
import os
import sys
sys.path.insert(0, %r)
os.environ["DB_PATH"] = %r
# Use repo so sqlite_store is used with our DB_PATH
from scalper.repositories import get_storage_repo
repo = get_storage_repo()
repo.insert_trade_record({
    "intent_id": "test-intent-1",
    "ts_open": 1700000000,
    "entry_ts": "2023-11-15T00:00:00",
    "symbol": "BTCUSDT",
    "strategy": "V3",
    "side": "LONG",
    "tf": "15",
    "entry": 30000.0,
    "sl": 29500.0,
    "tp": 31000.0,
    "sl_pct": 1.67,
    "tp_pct": 3.33,
    "confidence": 0.8,
    "atr_pct": 2.0,
    "spread_bps": 3.0,
    "bias_flags": "bull",
})
repo.update_trade_record_on_close("test-intent-1", {
    "close_ts": 1700001000,
    "close_price": 31000.0,
    "close_reason": "TP",
    "pnl_usdt": 50.0,
    "pnl_r": 1.5,
    "bars_held": 5,
    "mfe": 1.8,
    "mae": -0.2,
})
rows = repo.get_trade_records_closed()
assert len(rows) == 1, "expected one closed record"
r = rows[0]
assert r["intent_id"] == "test-intent-1"
assert r["symbol"] == "BTCUSDT"
assert r["strategy"] == "V3"
assert r["close_reason"] == "TP"
assert r.get("pnl_usdt") == 50.0
assert r.get("pnl_r") == 1.5
assert r.get("bars_held") == 5
assert r.get("mfe") == 1.8
assert r.get("mae") == -0.2
print("OK")
""" % (str(REPO_ROOT), str(db_path))
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, (result.stdout or "") + (result.stderr or "")
        assert "OK" in (result.stdout or "")


def test_report_runs_without_crashing_on_empty_db() -> None:
    """--report runs and exits 0 even when there are no closed trades."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "empty.db"
        env = os.environ.copy()
        env["DB_PATH"] = str(db_path)
        result = subprocess.run(
            [sys.executable, str(SCANNER), "--report"],
            env=env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, (result.stdout or "") + (result.stderr or "")
        assert "report" in (result.stdout or "").lower() or "trades" in (result.stdout or "").lower()
