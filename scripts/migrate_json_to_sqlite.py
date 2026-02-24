#!/usr/bin/env python3
"""
One-time migration: import existing JSON/file storage into SQLite.
Best-effort: skips corrupt rows. Dedupes via signal hash / intent id.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ensure repo root on path
_repo = Path(__file__).resolve().parents[1]
if str(_repo) not in sys.path:
    sys.path.insert(0, str(_repo))

from storage import save_paper_state
import sqlite_store


def _load_json(path: Path) -> object:
    """Load JSON file; return None on failure."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  skip {path.name}: {e}", file=sys.stderr)
        return None


def _normalize_paper_state(raw: dict) -> dict:
    """Minimal normalization for paper state."""
    from storage import _default_paper_state, _normalize_paper_state as _norm
    if not isinstance(raw, dict):
        return _default_paper_state()
    return _norm(raw)


def _import_signals(items: list, seen_hashes: set) -> tuple[int, int]:
    """Import signals; dedupe by hash. Returns (imported, skipped)."""
    imported = 0
    skipped = 0
    if not isinstance(items, list):
        return 0, 0
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            skipped += 1
            continue
        try:
            sig_hash = sqlite_store._signal_hash(item)
            if sig_hash in seen_hashes:
                skipped += 1
                continue
            if sqlite_store.store_signal(item):
                seen_hashes.add(sig_hash)
                imported += 1
            else:
                seen_hashes.add(sig_hash)
                skipped += 1
        except Exception as e:
            print(f"  skip signal[{i}]: {e}", file=sys.stderr)
            skipped += 1
    return imported, skipped


def _import_trade_intents(items: list, seen_ids: set) -> tuple[int, int]:
    """Import trade intents; dedupe by id. Returns (imported, skipped)."""
    imported = 0
    skipped = 0
    if not isinstance(items, list):
        return 0, 0
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            skipped += 1
            continue
        try:
            iid = str(item.get("id") or "").strip()
            if not iid:
                iid = f"migrated:{i}:{hash(str(item)) & 0x7FFFFFFF}"
            if iid in seen_ids:
                skipped += 1
                continue
            sqlite_store.store_trade_intent({**item, "id": iid})
            seen_ids.add(iid)
            imported += 1
        except Exception as e:
            print(f"  skip intent[{i}]: {e}", file=sys.stderr)
            skipped += 1
    return imported, skipped


def _import_risk_events(items: list) -> tuple[int, int]:
    """Import risk events. Returns (imported, skipped)."""
    imported = 0
    skipped = 0
    if not isinstance(items, list):
        return 0, 0
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            skipped += 1
            continue
        try:
            sqlite_store.store_risk_event(item)
            imported += 1
        except Exception as e:
            print(f"  skip risk_event[{i}]: {e}", file=sys.stderr)
            skipped += 1
    return imported, skipped


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import JSON/file storage into SQLite (one-time migration)."
    )
    parser.add_argument(
        "--paper-state",
        type=Path,
        default=Path("paper_state.json"),
        help="Path to paper_state.json (default: paper_state.json)",
    )
    parser.add_argument(
        "--signals",
        type=Path,
        default=None,
        help="Path to signals.json (optional)",
    )
    parser.add_argument(
        "--intents",
        type=Path,
        default=None,
        help="Path to trade_intents.json (optional)",
    )
    parser.add_argument(
        "--risk-events",
        type=Path,
        default=None,
        help="Path to risk_events.json (optional)",
    )
    parser.add_argument(
        "--cwd",
        type=Path,
        default=Path.cwd(),
        help="Working directory for relative paths (default: cwd)",
    )
    args = parser.parse_args()
    cwd = args.cwd.resolve()

    # Ensure DB is ready
    sqlite_store._ensure_db()

    seen_hashes: set[str] = set()
    seen_ids: set[str] = set()
    summary: dict[str, tuple[int, int]] = {}

    # 1. Paper state (paper_state.json)
    paper_path = cwd / args.paper_state if not args.paper_state.is_absolute() else args.paper_state
    if paper_path.exists():
        raw = _load_json(paper_path)
        if isinstance(raw, dict):
            normalized = _normalize_paper_state(raw)
            save_paper_state(normalized)
            n_open = len(normalized.get("open_positions") or [])
            n_closed = len(normalized.get("closed_trades") or [])
            summary["paper_state"] = (1, 0)
            print(f"paper_state: imported (open_positions={n_open}, closed_trades={n_closed})")
        else:
            summary["paper_state"] = (0, 1)
    else:
        print(f"paper_state: {paper_path} not found, skip")

    # 2. Signals (from paper_state or --signals file)
    signals_items: list = []
    if args.signals:
        sp = cwd / args.signals if not args.signals.is_absolute() else args.signals
        raw = _load_json(sp)
        if isinstance(raw, list):
            signals_items = raw
        elif isinstance(raw, dict) and "signals" in raw:
            signals_items = raw.get("signals") or []
    elif paper_path.exists():
        raw = _load_json(paper_path)
        if isinstance(raw, dict) and "signals" in raw:
            signals_items = raw.get("signals") or []

    if signals_items:
        imp, skp = _import_signals(signals_items, seen_hashes)
        summary["signals"] = (imp, skp)
        print(f"signals: imported={imp} skipped={skp}")

    # 3. Trade intents (from paper_state or --intents file)
    intents_items: list = []
    if args.intents:
        ip = cwd / args.intents if not args.intents.is_absolute() else args.intents
        raw = _load_json(ip)
        if isinstance(raw, list):
            intents_items = raw
        elif isinstance(raw, dict) and "trade_intents" in raw:
            intents_items = raw.get("trade_intents") or []
    elif paper_path.exists():
        raw = _load_json(paper_path)
        if isinstance(raw, dict) and "trade_intents" in raw:
            intents_items = raw.get("trade_intents") or []

    if intents_items:
        imp, skp = _import_trade_intents(intents_items, seen_ids)
        summary["trade_intents"] = (imp, skp)
        print(f"trade_intents: imported={imp} skipped={skp}")

    # 4. Risk events (from paper_state or --risk-events file)
    events_items: list = []
    if args.risk_events:
        rp = cwd / args.risk_events if not args.risk_events.is_absolute() else args.risk_events
        raw = _load_json(rp)
        if isinstance(raw, list):
            events_items = raw
        elif isinstance(raw, dict) and "risk_events" in raw:
            events_items = raw.get("risk_events") or []
    elif paper_path.exists():
        raw = _load_json(paper_path)
        if isinstance(raw, dict) and "risk_events" in raw:
            events_items = raw.get("risk_events") or []

    if events_items:
        imp, skp = _import_risk_events(events_items)
        summary["risk_events"] = (imp, skp)
        print(f"risk_events: imported={imp} skipped={skp}")

    # Summary
    print("\n--- Summary ---")
    total_imp = sum(s[0] for s in summary.values())
    total_skp = sum(s[1] for s in summary.values())
    for name, (imp, skp) in summary.items():
        print(f"  {name}: imported={imp} skipped={skp}")
    print(f"  total: imported={total_imp} skipped={total_skp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
