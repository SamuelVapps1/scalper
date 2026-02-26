#!/usr/bin/env python3
"""
Walk-forward (minimal) evaluation: rolling windows, run replay on each test window.
V2_TREND_PULLBACK only, TP_R exits, no optimization.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _run_id(symbols: list[str], train_days: int, test_days: int, step_days: int, start_days_ago: int) -> str:
    """Deterministic run_id for this walk-forward run (same args = same id)."""
    sig = json.dumps(
        {"symbols": symbols, "train_days": train_days, "test_days": test_days, "step_days": step_days, "start_days_ago": start_days_ago},
        sort_keys=True,
    )
    return hashlib.sha256(sig.encode()).hexdigest()[:12]


def _iter_windows(
    start_days_ago: int,
    train_days: int,
    test_days: int,
    step_days: int,
):
    """
    Yield (test_end_days_ago, test_start_days_ago) for each rolling window.
    Chronological order: oldest first (largest end_days_ago).
    """
    # Window 0: train [start, start+train], test [start+train, start+train+test]
    # test_end_days_ago = start_days_ago - train_days - test_days
    # test_start_days_ago = start_days_ago - train_days
    # Window i: step forward by step_days
    # test_end_days_ago = start_days_ago - train_days - test_days - i*step_days
    # test_start_days_ago = start_days_ago - train_days - i*step_days
    i = 0
    while True:
        test_start_days_ago = start_days_ago - train_days - i * step_days
        test_end_days_ago = test_start_days_ago - test_days
        if test_end_days_ago < 0:
            break
        yield (test_end_days_ago, test_start_days_ago)
        i += 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Walk-forward evaluation: rolling windows, replay per test window.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--train-days", type=int, default=90, help="Warmup/train days for indicator stability")
    parser.add_argument("--test-days", type=int, default=30, help="Out-of-sample test days per window")
    parser.add_argument("--step-days", type=int, default=30, help="Step between windows (days)")
    parser.add_argument("--start-days-ago", type=int, default=365, help="Total lookback (days from now)")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        _log.error("No symbols provided")
        return 2
    for sym in symbols:
        if any(c.isspace() for c in sym):
            raise ValueError(f"Symbol contains whitespace: {repr(sym)}")
    _log.info("NORMALIZED_SYMBOLS=%s count=%d", symbols, len(symbols))

    # Fixed env: V2 only, TP_R exits (no BE, no partials, no trailing)
    fixed_env = {
        "STRATEGY_V1": "0",
        "V2_TREND_PULLBACK": "1",
        "V1_SETUP_BREAKOUT": "0",
        "V1_SETUP_TRAP": "0",
        "BE_AT_R": "0",
        "PARTIAL_TP_AT_R": "0",
        "TRAIL_AFTER_R": "0",
    }

    replay_script = _project_root / "scripts" / "replay.py"
    if not replay_script.exists():
        _log.error("Replay script not found: %s", replay_script)
        return 1

    run_id = _run_id(symbols, args.train_days, args.test_days, args.step_days, args.start_days_ago)
    data_dir = _project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    symbols_str = ",".join(symbols)

    for win_idx, (test_end_days_ago, test_start_days_ago) in enumerate(
        _iter_windows(args.start_days_ago, args.train_days, args.test_days, args.step_days)
    ):
        _log.info(
            "Window %d: test [%d, %d] days ago",
            win_idx + 1,
            test_start_days_ago,
            test_end_days_ago,
        )

        cmd = [
            sys.executable,
            str(replay_script),
            "--symbols", symbols_str,
            "--start-days-ago", str(test_start_days_ago),
            "--end-days-ago", str(test_end_days_ago),
            "--tf-trigger", "15",
            "--tf-timing", "5",
            "--tf-bias", "240",
            "--tf-setup", "60",
            "--step-bars", "1",
        ]

        env = dict(os.environ)
        for k, v in fixed_env.items():
            env[k] = str(v)

        proc = subprocess.run(
            cmd,
            env=env,
            cwd=str(_project_root),
            capture_output=True,
            text=True,
            timeout=600,
        )

        # Replay writes replay_summary_{run_id}.json - run_id is deterministic from params
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        replay_run_id = None
        for line in combined.splitlines():
            if "Exported summary:" in line:
                # data/replay_summary_abc123.json
                parts = line.split("replay_summary_")
                if len(parts) >= 2:
                    replay_run_id = parts[1].replace(".json", "").strip()
                    break
            if "RUN_SIGNATURE[" in line:
                m = line.split("RUN_SIGNATURE[")[1].split("]")[0]
                if len(m) == 12:
                    replay_run_id = m
                    break

        summary_path = data_dir / f"replay_summary_{replay_run_id}.json" if replay_run_id else None
        if summary_path and summary_path.exists():
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
        else:
            _log.warning("Window %d: no summary found (exit=%d)", win_idx + 1, proc.returncode)
            summary = {}

        kpi = summary.get("overall") or {}
        skip_reasons = summary.get("skip_reasons", {})
        exit_reason_counts = summary.get("exit_reason_counts", {})
        by_setup = summary.get("by_setup", {})
        trades_total = kpi.get("trades_total", 0)

        row = {
            "window": win_idx + 1,
            "test_end_days_ago": test_end_days_ago,
            "test_start_days_ago": test_start_days_ago,
            "run_id": replay_run_id or "",
            "trades_total": trades_total,
            "wins": kpi.get("wins", 0),
            "losses": kpi.get("losses", 0),
            "winrate": kpi.get("winrate", 0) or 0,
            "expectancy_R": kpi.get("expectancy_R", 0) or 0,
            "profit_factor": kpi.get("profit_factor", 0) or 0,
            "avg_win_R": kpi.get("avg_win_R", 0) or 0,
            "avg_loss_R": kpi.get("avg_loss_R", 0) or 0,
            "max_dd_usdt": kpi.get("max_dd_usdt", 0) or 0,
            "skip_reasons": skip_reasons,
            "exit_reason_counts": exit_reason_counts,
            "by_setup": by_setup,
        }
        if trades_total == 0 and skip_reasons:
            top_3 = sorted(
                [(k, v) for k, v in skip_reasons.items() if v > 0],
                key=lambda x: -x[1],
            )[:3]
            row["zero_trades_hint"] = [{"reason": k, "count": v} for k, v in top_3]
        top_skip = ""
        if skip_reasons:
            top = max(skip_reasons.items(), key=lambda x: x[1])
            if top[1] > 0:
                top_skip = f"{top[0]}:{top[1]}"
        row["top_skip_reason"] = top_skip
        results.append(row)
        _log.info(
            "  -> trades=%d winrate=%.1f%% exp_R=%.4f",
            row["trades_total"],
            row["winrate"] * 100,
            row["expectancy_R"],
        )

    # Output
    csv_path = data_dir / f"walkforward_results_{run_id}.csv"
    json_path = data_dir / f"walkforward_results_{run_id}.json"

    csv_cols = [
        "window", "test_end_days_ago", "test_start_days_ago", "run_id",
        "trades_total", "wins", "losses", "winrate", "expectancy_R",
        "profit_factor", "avg_win_R", "avg_loss_R", "max_dd_usdt",
        "top_skip_reason",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    payload = {
        "run_id": run_id,
        "args": {
            "symbols": symbols,
            "train_days": args.train_days,
            "test_days": args.test_days,
            "step_days": args.step_days,
            "start_days_ago": args.start_days_ago,
        },
        "windows": results,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)

    _log.info("")
    _log.info("Wrote %s", csv_path)
    _log.info("Wrote %s", json_path)

    # Console table
    print("\n=== WALK-FORWARD RESULTS ===")
    print(
        f"{'win':>4} {'end_ago':>7} {'start_ago':>9} {'trades':>6} {'winrate':>8} "
        f"{'exp_R':>7} {'pf':>6} {'max_dd':>8}"
    )
    print("-" * 70)
    for r in results:
        print(
            f"{r['window']:>4} {r['test_end_days_ago']:>7} {r['test_start_days_ago']:>9} "
            f"{r['trades_total']:>6} {r['winrate']*100:>7.1f}% "
            f"{r['expectancy_R']:>7.4f} {r['profit_factor']:>6.2f} {r['max_dd_usdt']:>8.2f}"
        )
    print("-" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
