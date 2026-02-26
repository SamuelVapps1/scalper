#!/usr/bin/env python3
"""
V3 parameter sweep with walk-forward evaluation and robust scoring.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_csv_floats(raw: str) -> List[float]:
    return [float(x.strip()) for x in str(raw).split(",") if x.strip()]


def _parse_csv_ints(raw: str) -> List[int]:
    return [int(float(x.strip())) for x in str(raw).split(",") if x.strip()]


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(float(v) for v in values)
    if len(arr) == 1:
        return arr[0]
    rank = (len(arr) - 1) * max(0.0, min(1.0, q))
    lo = int(rank)
    hi = min(lo + 1, len(arr) - 1)
    frac = rank - lo
    return arr[lo] * (1.0 - frac) + arr[hi] * frac


def _iter_windows(
    start_days_ago: int,
    train_days: int,
    test_days: int,
    step_days: int,
) -> List[Tuple[int, int]]:
    windows: List[Tuple[int, int]] = []
    i = 0
    while True:
        test_start_days_ago = start_days_ago - train_days - i * step_days
        test_end_days_ago = test_start_days_ago - test_days
        if test_end_days_ago < 0:
            break
        windows.append((test_end_days_ago, test_start_days_ago))
        i += 1
    return windows


def _run_id(args: argparse.Namespace, symbols: List[str], grid: Dict[str, List[float]]) -> str:
    sig = {
        "symbols": symbols,
        "start_days_ago": args.start_days_ago,
        "train_days": args.train_days,
        "test_days": args.test_days,
        "step_days": args.step_days,
        "grid": grid,
        "top_k": args.top_k,
    }
    return hashlib.sha256(json.dumps(sig, sort_keys=True).encode()).hexdigest()[:12]


def _parse_replay_run_id(output: str) -> str:
    for line in output.splitlines():
        if "RUN_SIGNATURE[" in line:
            try:
                rid = line.split("RUN_SIGNATURE[", 1)[1].split("]", 1)[0].strip()
                if len(rid) == 12:
                    return rid
            except Exception:
                pass
        if "Exported summary:" in line and "replay_summary_" in line:
            try:
                rid = line.split("replay_summary_", 1)[1].split(".json", 1)[0].strip()
                if len(rid) == 12:
                    return rid
            except Exception:
                pass
    return ""


def _robust_score(exp_r_windows: List[float], max_dd_r_windows: List[float], trades_windows: List[int]) -> Tuple[float, float]:
    p25_exp_r = _percentile(exp_r_windows, 0.25)
    dd_term = 0.25 * abs(min(max_dd_r_windows)) if max_dd_r_windows else 0.0
    # Penalize low activity on OOS windows (soft penalty).
    p25_trades = _percentile([float(x) for x in trades_windows], 0.25) if trades_windows else 0.0
    penalty_low_trades = max(0.0, 5.0 - p25_trades) * 0.05
    score = p25_exp_r - dd_term - penalty_low_trades
    return score, penalty_low_trades


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep V3 params with walk-forward robust scoring.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT")
    parser.add_argument("--start-days-ago", type=int, default=365)
    parser.add_argument("--train-days", type=int, default=90)
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--grid-donchian", type=str, default="15,20,25,30")
    parser.add_argument("--grid-bodyatr", type=str, default="0.15,0.20,0.25,0.30")
    parser.add_argument("--grid-trendsep", type=str, default="0.5,0.8,1.1")
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        _log.error("No symbols provided")
        return 2
    for sym in symbols:
        if any(c.isspace() for c in sym):
            raise ValueError(f"Symbol contains whitespace: {repr(sym)}")

    grid = {
        "DONCHIAN_N_15M": _parse_csv_ints(args.grid_donchian),
        "BODY_ATR_15M": _parse_csv_floats(args.grid_bodyatr),
        "TREND_SEP_ATR_1H": _parse_csv_floats(args.grid_trendsep),
    }
    if not grid["DONCHIAN_N_15M"] or not grid["BODY_ATR_15M"] or not grid["TREND_SEP_ATR_1H"]:
        _log.error("One of the parameter grids is empty")
        return 2

    windows = _iter_windows(args.start_days_ago, args.train_days, args.test_days, args.step_days)
    if not windows:
        _log.error("No walk-forward windows generated with provided args")
        return 2

    run_id = _run_id(args, symbols, grid)
    data_dir = _project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    replay_script = _project_root / "scripts" / "replay.py"
    if not replay_script.exists():
        _log.error("Replay script not found: %s", replay_script)
        return 1

    base_env = dict(os.environ)
    # Sweep is V3-only; use cache-only and hard exits.
    base_env.update(
        {
            "STRATEGY_V1": "0",
            "V1_SETUP_BREAKOUT": "0",
            "V1_SETUP_TRAP": "0",
            "V2_TREND_PULLBACK": "0",
            "V3_TREND_BREAKOUT": "1",
            "REPLAY_EXIT_MODE": "hard",
        }
    )

    combos = list(
        itertools.product(
            grid["DONCHIAN_N_15M"],
            grid["BODY_ATR_15M"],
            grid["TREND_SEP_ATR_1H"],
        )
    )
    _log.info("Running %d combos over %d windows", len(combos), len(windows))
    symbols_str = ",".join(symbols)
    results: List[Dict[str, object]] = []
    try:
        equity = float(os.getenv("PAPER_EQUITY_USDT", "200") or 200.0)
    except (TypeError, ValueError):
        equity = 200.0
    equity = equity if equity > 0 else 200.0

    for idx, (donchian_n, body_atr, trend_sep) in enumerate(combos, start=1):
        env = dict(base_env)
        env["DONCHIAN_N_15M"] = str(donchian_n)
        env["BODY_ATR_15M"] = str(body_atr)
        env["TREND_SEP_ATR_1H"] = str(trend_sep)

        exp_r_windows: List[float] = []
        max_dd_r_windows: List[float] = []
        trades_windows: List[int] = []
        window_fail = ""
        replay_ids: List[str] = []

        for test_end_days_ago, test_start_days_ago in windows:
            cmd = [
                sys.executable,
                str(replay_script),
                "--symbols",
                symbols_str,
                "--start-days-ago",
                str(test_start_days_ago),
                "--end-days-ago",
                str(test_end_days_ago),
                "--tf-trigger",
                "15",
                "--tf-timing",
                "5",
                "--tf-bias",
                "240",
                "--tf-setup",
                "60",
                "--step-bars",
                "1",
                "--cache-only",
            ]
            proc = subprocess.run(
                cmd,
                env=env,
                cwd=str(_project_root),
                capture_output=True,
                text=True,
                timeout=900,
            )
            output = (proc.stdout or "") + "\n" + (proc.stderr or "")
            if proc.returncode != 0:
                window_fail = f"window_failed end={test_end_days_ago} exit={proc.returncode}"
                # Fail fast as requested for missing cache in cache-only mode.
                raise RuntimeError(
                    f"Replay failed in cache-only mode for combo "
                    f"DONCHIAN_N_15M={donchian_n}, BODY_ATR_15M={body_atr}, TREND_SEP_ATR_1H={trend_sep}. "
                    f"{window_fail}\n{output[-800:]}"
                )

            replay_rid = _parse_replay_run_id(output)
            if replay_rid:
                replay_ids.append(replay_rid)
            summary_path = data_dir / f"replay_summary_{replay_rid}.json" if replay_rid else None
            if not summary_path or not summary_path.exists():
                raise RuntimeError(f"Missing replay summary file for combo {donchian_n}/{body_atr}/{trend_sep}")
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
            kpi = summary.get("overall") or {}
            exp_r = float(kpi.get("expectancy_R", 0.0) or 0.0)
            max_dd_usdt = float(kpi.get("max_dd_usdt", 0.0) or 0.0)
            trades_total = int(kpi.get("trades_total", 0) or 0)

            exp_r_windows.append(exp_r)
            max_dd_r_windows.append(max_dd_usdt / equity)
            trades_windows.append(trades_total)

        score, penalty_low_trades = _robust_score(exp_r_windows, max_dd_r_windows, trades_windows)
        row: Dict[str, object] = {
            "donchian_n_15m": donchian_n,
            "body_atr_15m": body_atr,
            "trend_sep_atr_1h": trend_sep,
            "windows": len(windows),
            "trades_total": int(sum(trades_windows)),
            "exp_r_mean": sum(exp_r_windows) / len(exp_r_windows) if exp_r_windows else 0.0,
            "exp_r_p25": _percentile(exp_r_windows, 0.25),
            "max_dd_r_min": min(max_dd_r_windows) if max_dd_r_windows else 0.0,
            "penalty_low_trades": penalty_low_trades,
            "robust_score": score,
            "replay_run_ids": "|".join(replay_ids),
            "status": "ok" if not window_fail else window_fail,
        }
        results.append(row)
        _log.info(
            "[%d/%d] n=%s body=%.2f sep=%.2f score=%.4f p25_exp=%.4f trades=%d",
            idx,
            len(combos),
            donchian_n,
            body_atr,
            trend_sep,
            score,
            float(row["exp_r_p25"]),
            int(row["trades_total"]),
        )

    results.sort(key=lambda r: float(r.get("robust_score", 0.0)), reverse=True)
    top_k = max(1, int(args.top_k))
    top = results[:top_k]

    out_results = data_dir / f"sweep_v3_results_{run_id}.csv"
    out_top = data_dir / f"sweep_v3_top_{run_id}.csv"
    cols = [
        "donchian_n_15m",
        "body_atr_15m",
        "trend_sep_atr_1h",
        "windows",
        "trades_total",
        "exp_r_mean",
        "exp_r_p25",
        "max_dd_r_min",
        "penalty_low_trades",
        "robust_score",
        "status",
        "replay_run_ids",
    ]
    with open(out_results, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)
    with open(out_top, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(top)

    print("\n=== TOP 10 V3 COMBOS (ROBUST SCORE) ===")
    print(
        f"{'n':>4} {'body':>7} {'sep':>7} {'score':>10} {'p25_expR':>10} "
        f"{'min_ddR':>9} {'trades':>8}"
    )
    print("-" * 70)
    for r in results[:10]:
        print(
            f"{int(r['donchian_n_15m']):>4} "
            f"{float(r['body_atr_15m']):>7.2f} "
            f"{float(r['trend_sep_atr_1h']):>7.2f} "
            f"{float(r['robust_score']):>10.4f} "
            f"{float(r['exp_r_p25']):>10.4f} "
            f"{float(r['max_dd_r_min']):>9.4f} "
            f"{int(r['trades_total']):>8}"
        )
    print("-" * 70)
    _log.info("Wrote %s", out_results)
    _log.info("Wrote %s", out_top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
