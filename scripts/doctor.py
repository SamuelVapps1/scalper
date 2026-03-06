# scripts/doctor.py
"""One command: what's broken? Prints env + dotenv + config + paper state + telegram booleans (no secrets)."""
import os
import sys
from pathlib import Path
from datetime import datetime, timezone


def _utc(ts: int | None):
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    print("== Scalper Doctor ==")
    print("python:", sys.version.split()[0])
    print("cwd:", os.getcwd())
    print("repo:", str(repo))

    # import after basic prints
    import config
    from dotenv import find_dotenv

    dotenv_path = find_dotenv(usecwd=True) or "<not found>"
    token_set = bool(os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or ""
    chat_set = bool(chat_id)
    print("\n-- dotenv/config --")
    print("dotenv_path:", dotenv_path)
    print("telegram token_set:", token_set, "chat_set:", chat_set, "chat_len:", len(chat_id))
    print("POSITION_MODE:", getattr(config, "POSITION_MODE", None))
    print("MAX_CONCURRENT_POSITIONS:", getattr(config, "MAX_CONCURRENT_POSITIONS", None))
    print("PAPER_POSITION_USDT:", getattr(config, "PAPER_POSITION_USDT", None))
    print("SCAN_SECONDS:", getattr(config, "SCAN_SECONDS", None))
    wl = getattr(config, "WATCHLIST", []) or []
    print("WATCHLIST_COUNT:", len(wl))
    if len(wl) <= 15:
        print("WATCHLIST:", wl)

    # paper state
    try:
        from scalper.storage import load_paper_state

        state = load_paper_state() or {}
        open_positions = [p for p in (state.get("open_positions") or []) if isinstance(p, dict)]
        print("\n-- paper state --")
        print("open_positions_count:", len(open_positions))
        if open_positions:
            for p in open_positions[:5]:
                sym = p.get("symbol", "")
                st = (p.get("status") or "OPEN").upper()
                direction = p.get("direction") or p.get("side") or "-"
                entry = p.get("entry_price") or p.get("entry") or "-"
                print(f"  {sym} {direction} status={st} entry={entry}")
    except Exception as e:
        print("\n-- paper state --")
        print("error reading paper state:", repr(e))

    # optional: ping dashboard if running
    try:
        import urllib.request
        import json

        url = "http://127.0.0.1:8000/api/health"
        with urllib.request.urlopen(url, timeout=2) as r:
            data = json.loads(r.read().decode("utf-8"))
        print("\n-- dashboard health --")
        print("health ok:", data.get("ok"))
        print("last_scan_ts:", data.get("last_scan_ts"))
    except Exception:
        pass


if __name__ == "__main__":
    main()
