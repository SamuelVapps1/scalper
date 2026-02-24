# scripts/reset_paper.py
"""Wipe paper state clean — start fresh (open positions, counters, etc.) without manually deleting files."""
import sys
from pathlib import Path

# ensure repo root on path
_repo = Path(__file__).resolve().parents[1]
if str(_repo) not in sys.path:
    sys.path.insert(0, str(_repo))

from storage import load_paper_state, save_paper_state


def main():
    s = load_paper_state() or {}
    s["open_positions"] = []
    s["closed_trades"] = []
    s["trade_intents"] = []
    s["risk_events"] = []
    s["trade_count_today"] = 0
    s["daily_pnl_sim"] = 0
    s["consecutive_losses"] = 0
    s["cooldown_until_utc"] = ""
    save_paper_state(s)
    print("paper state reset OK")


if __name__ == "__main__":
    main()
