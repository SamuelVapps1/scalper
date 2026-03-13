# scripts/seed_positions.py
"""Seed open positions for cap testing — instantly create 1–5 positions to test blocking rules."""
import sys
import time
import uuid
import argparse
from pathlib import Path

# ensure repo root on path
_repo = Path(__file__).resolve().parents[1]
if str(_repo) not in sys.path:
    sys.path.insert(0, str(_repo))

from scalper.storage import load_paper_state, save_paper_state

DEFAULT = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--symbols", type=str, default=",".join(DEFAULT))
    ap.add_argument("--direction", type=str, default="LONG")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    syms = syms[: max(0, args.n)]
    now = int(time.time())

    s = load_paper_state() or {}
    s["open_positions"] = []
    for sym in syms:
        s["open_positions"].append({
            "id": str(uuid.uuid4()),
            "intent_id": f"seed|{sym}|{now}",
            "symbol": sym,
            "direction": args.direction,
            "side": args.direction,
            "status": "OPEN",
            "entry": 100.0,
            "sl": 95.0,
            "tp": 110.0,
            "entry_price": 100.0,
            "sl_price": 95.0,
            "tp_price": 110.0,
            "notional_usdt": 20.0,
            "qty_est": 0.2,
            "opened_ts": now,
            "entry_ts": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(now)),
        })
    save_paper_state(s)
    print("seeded open_positions:", len(s["open_positions"]))


if __name__ == "__main__":
    main()
