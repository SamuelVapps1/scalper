#!/usr/bin/env python3
"""
Deterministic replay harness CLI.
Replays historical candles through StrategyEngine -> RiskEngine -> PaperBroker.
Outputs to ./runs/: equity_curve_<tag>.csv, trades_<tag>.csv, summary_<tag>.json, events_<tag>.jsonl

Usage:
  python scripts/replay_harness.py --symbols "BTCUSDT" --start "2024-01-01" --end "2024-01-07" --interval 15 --out-tag "smoke"
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from scalper.replay_harness import run_replay


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic replay: StrategyEngine -> RiskEngine -> PaperBroker. Outputs to ./runs/."
    )
    parser.add_argument("--symbols", type=str, default="BTCUSDT", help='Comma-separated symbols, e.g. "BTCUSDT,ETHUSDT"')
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--interval", type=int, default=15, help="Candle interval in minutes (default: 15)")
    parser.add_argument("--fees-bps", type=float, default=6, help="Fees in basis points (default: 6)")
    parser.add_argument("--slippage-bps", type=float, default=2, help="Slippage in basis points (default: 2)")
    parser.add_argument("--spread-bps", type=float, default=1, help="Spread in basis points (default: 1)")
    parser.add_argument("--seed", type=int, default=123, help="Random seed for determinism (default: 123)")
    parser.add_argument("--out-tag", type=str, default="replay", help="Output file tag (default: replay)")
    parser.add_argument("--no-cache", action="store_true", help="Disable candle cache (fetch from API)")
    parser.add_argument("--no-events", action="store_true", help="Skip events_<tag>.jsonl output")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("ERROR: No symbols provided", file=sys.stderr)
        return 2

    summary = run_replay(
        symbols=symbols,
        start_str=args.start,
        end_str=args.end,
        interval=args.interval,
        fees_bps=args.fees_bps,
        slippage_bps=args.slippage_bps,
        spread_bps=args.spread_bps,
        seed=args.seed,
        out_tag=args.out_tag,
        use_cache=not args.no_cache,
        emit_events=not args.no_events,
    )

    print(f"Replay complete. Summary: {summary}")
    print(f"Artifacts in ./runs/: equity_curve_{args.out_tag}.csv, trades_{args.out_tag}.csv, summary_{args.out_tag}.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
