#!/usr/bin/env python3
import argparse
import csv
import hashlib
import itertools
import json
import logging
import subprocess
import sys
from pathlib import Path
from statistics import median
from typing import Iterable, List


_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_int_grid(raw: str) -> List[int]:
    out: List[int] = []
    for x in str(raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.append(int(x))
    if not out:
        raise ValueError("Empty integer grid")
    return out


def _parse_float_grid(raw: str) -> List[float]:
    out: List[float] = []
    for x in str(raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.append(float(x))
    if not out:
        raise ValueError("Empty float grid")
    return out


def _parse_bool_grid(raw: str) -> List[int]:
    vals: List[int] = []
    for x in str(raw or "").split(","):
        s = x.strip().lower()
        if not s:
            continue
        if s in {"1", "true", "yes", "on"}:
            vals.append(1)
        elif s in {"0", "false", "no", "off"}:
            vals.append(0)
        else:
            vals.append(int(s))
    if not vals:
        raise ValueError("Empty bool grid")
    return [1 if int(v) else 0 for v in vals]


def _percentile(values: Iterable[float], p: float) -> float:
    arr = sorted(float(v) for v in values)
    if not arr:
        return 0.0
    if len(arr) == 1:
        return arr[0]
    q = max(0.0, min(1.0, float(p)))
    idx = q * (len(arr) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(arr) - 1)
    frac = idx - lo
    return arr[lo] * (1.0 - frac) + arr[hi] * frac


def _run_id(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _score_combo(
    windows: List[dict],
    *,
    penalty_low_trades: int,
) -> dict:
    exp = [float(w.get("expectancy_R", 0.0) or 0.0) for w in windows]
    pf = [float(w.get("profit_factor", 0.0) or 0.0) for w in windows]
    wr = [float(w.get("winrate", 0.0) or 0.0) for w in windows]
    tr = [int(w.get("trades_total", 0) or 0) for w in windows]
    p20_exp = _percentile(exp, 0.20)
    p20_pf = _percentile(pf, 0.20)
    p20_wr = _percentile(wr, 0.20)
    med_trades = float(median(tr)) if tr else 0.0
    penalty = 0.0
    if med_trades < float(penalty_low_trades):
        penalty = (float(penalty_low_trades) - med_trades) / max(1.0, float(penalty_low_trades))
    score = (1.00 * p20_exp) + (0.20 * p20_pf) + (0.10 * p20_wr) - penalty
    return {
        "score": score,
        "p20_expectancy_R": p20_exp,
        "p20_profit_factor": p20_pf,
        "p20_winrate": p20_wr,
        "median_trades": med_trades,
        "penalty_low_trades": penalty,
        "windows_count": len(windows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep V3 params via walk-forward with robust percentile scoring.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT")
    parser.add_argument("--train-days", type=int, default=90)
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--start-days-ago", type=int, default=365)
    parser.add_argument("--cache-only", action="store_true", help="Require cache-only mode (recommended/required).")
    parser.add_argument("--grid-donchian-n-15m", type=str, default="20,30")
    parser.add_argument("--grid-body-atr-15m", type=str, default="0.20,0.25")
    parser.add_argument("--grid-trend-sep-atr-1h", type=str, default="0.60,0.80")
    parser.add_argument("--grid-use-5m-confirm", type=str, default="1,0")
    parser.add_argument("--penalty-low-trades", type=int, default=10)
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()

    if not args.cache_only:
        _log.error("sweep_v3 requires --cache-only. Refusing to run without cache-only mode.")
        return 2

    walkforward_script = _project_root / "scripts" / "walkforward.py"
    if not walkforward_script.exists():
        _log.error("walkforward.py not found: %s", walkforward_script)
        return 1

    donchian_grid = _parse_int_grid(args.grid_donchian_n_15m)
    body_grid = _parse_float_grid(args.grid_body_atr_15m)
    sep_grid = _parse_float_grid(args.grid_trend_sep_atr_1h)
    confirm_grid = _parse_bool_grid(args.grid_use_5m_confirm)

    combos = list(itertools.product(donchian_grid, body_grid, sep_grid, confirm_grid))
    _log.info("SWEEP_V3 combos=%d cache_only=%s", len(combos), bool(args.cache_only))

    rows: List[dict] = []
    data_dir = _project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    for idx, (donchian_n, body_atr, trend_sep, use_5m_confirm) in enumerate(combos, start=1):
        _log.info(
            "Combo %d/%d donchian=%d body_atr=%.4f trend_sep=%.4f use_5m_confirm=%d",
            idx,
            len(combos),
            donchian_n,
            body_atr,
            trend_sep,
            use_5m_confirm,
        )
        cmd = [
            sys.executable,
            str(walkforward_script),
            "--symbols", str(args.symbols),
            "--train-days", str(args.train_days),
            "--test-days", str(args.test_days),
            "--step-days", str(args.step_days),
            "--start-days-ago", str(args.start_days_ago),
            "--cache-only",
            "--strategy", "v3",
            "--donchian-n-15m", str(donchian_n),
            "--body-atr-15m", str(body_atr),
            "--trend-sep-atr-1h", str(trend_sep),
            "--use-5m-confirm", str(int(use_5m_confirm)),
        ]
        proc = subprocess.run(
            cmd,
            cwd=str(_project_root),
            capture_output=True,
            text=True,
            timeout=1800,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            tail = (stderr or stdout)[-1200:]
            _log.error("walkforward failed for combo %d (exit=%d)", idx, proc.returncode)
            _log.error("tail: %s", tail)
            _log.error("Fail-fast due to missing/insufficient cache in --cache-only mode.")
            return 1

        wf_run_id = None
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        for line in combined.splitlines():
            if "Wrote" in line and "walkforward_results_" in line and ".json" in line:
                part = line.split("walkforward_results_")[-1]
                wf_run_id = part.split(".json")[0].strip()
        if not wf_run_id:
            _log.error("Could not parse walkforward run_id from output for combo %d", idx)
            return 1

        wf_json = data_dir / f"walkforward_results_{wf_run_id}.json"
        if not wf_json.exists():
            _log.error("Walkforward result missing: %s", wf_json)
            return 1
        with open(wf_json, "r", encoding="utf-8") as f:
            payload = json.load(f)
        windows = list(payload.get("windows", []) or [])
        robust = _score_combo(windows, penalty_low_trades=int(args.penalty_low_trades))
        rows.append(
            {
                "wf_run_id": wf_run_id,
                "donchian_n_15m": donchian_n,
                "body_atr_15m": body_atr,
                "trend_sep_atr_1h": trend_sep,
                "use_5m_confirm": int(use_5m_confirm),
                **robust,
            }
        )

    rows.sort(key=lambda r: float(r.get("score", 0.0)), reverse=True)
    top_k = max(1, int(args.top_k))

    sweep_id = _run_id(
        {
            "symbols": args.symbols,
            "train_days": args.train_days,
            "test_days": args.test_days,
            "step_days": args.step_days,
            "start_days_ago": args.start_days_ago,
            "cache_only": bool(args.cache_only),
            "grid": {
                "donchian": donchian_grid,
                "body_atr": body_grid,
                "trend_sep": sep_grid,
                "use_5m_confirm": confirm_grid,
            },
            "penalty_low_trades": int(args.penalty_low_trades),
        }
    )
    out_csv = data_dir / f"sweep_v3_{sweep_id}.csv"
    out_json = data_dir / f"sweep_v3_{sweep_id}.json"
    cols = [
        "wf_run_id",
        "donchian_n_15m",
        "body_atr_15m",
        "trend_sep_atr_1h",
        "use_5m_confirm",
        "score",
        "p20_expectancy_R",
        "p20_profit_factor",
        "p20_winrate",
        "median_trades",
        "penalty_low_trades",
        "windows_count",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "sweep_id": sweep_id,
                "args": {
                    "symbols": args.symbols,
                    "train_days": args.train_days,
                    "test_days": args.test_days,
                    "step_days": args.step_days,
                    "start_days_ago": args.start_days_ago,
                    "cache_only": bool(args.cache_only),
                    "penalty_low_trades": int(args.penalty_low_trades),
                    "top_k": top_k,
                },
                "rows": rows,
                "top_k": rows[:top_k],
            },
            f,
            indent=2,
            ensure_ascii=True,
        )

    _log.info("Wrote %s", out_csv)
    _log.info("Wrote %s", out_json)
    print("\n=== SWEEP V3 TOP K ===")
    for i, r in enumerate(rows[:top_k], start=1):
        print(
            f"{i:>2}. score={float(r['score']):.4f} "
            f"donchian={r['donchian_n_15m']} body_atr={float(r['body_atr_15m']):.3f} "
            f"trend_sep={float(r['trend_sep_atr_1h']):.3f} use_5m={r['use_5m_confirm']} "
            f"p20_exp={float(r['p20_expectancy_R']):.4f} med_trades={float(r['median_trades']):.1f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3

import argparse
import csv
import hashlib
import itertools
import json
import logging
import subprocess
import sys
from pathlib import Path
from statistics import median
from typing import Iterable, List


_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_int_grid(raw: str) -> List[int]:
    out: List[int] = []
    for x in str(raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.append(int(x))
    if not out:
        raise ValueError("Empty integer grid")
    return out


def _parse_float_grid(raw: str) -> List[float]:
    out: List[float] = []
    for x in str(raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.append(float(x))
    if not out:
        raise ValueError("Empty float grid")
    return out


def _parse_bool_grid(raw: str) -> List[int]:
    vals: List[int] = []
    for x in str(raw or "").split(","):
        s = x.strip().lower()
        if not s:
            continue
        if s in {"1", "true", "yes", "on"}:
            vals.append(1)
        elif s in {"0", "false", "no", "off"}:
            vals.append(0)
        else:
            vals.append(int(s))
    if not vals:
        raise ValueError("Empty bool grid")
    return [1 if int(v) else 0 for v in vals]


def _percentile(values: Iterable[float], p: float) -> float:
    arr = sorted(float(v) for v in values)
    if not arr:
        return 0.0
    if len(arr) == 1:
        return arr[0]
    q = max(0.0, min(1.0, float(p)))
    idx = q * (len(arr) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(arr) - 1)
    frac = idx - lo
    return arr[lo] * (1.0 - frac) + arr[hi] * frac


def _run_id(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _score_combo(
    windows: List[dict],
    *,
    penalty_low_trades: int,
) -> dict:
    exp = [float(w.get("expectancy_R", 0.0) or 0.0) for w in windows]
    pf = [float(w.get("profit_factor", 0.0) or 0.0) for w in windows]
    wr = [float(w.get("winrate", 0.0) or 0.0) for w in windows]
    tr = [int(w.get("trades_total", 0) or 0) for w in windows]
    p20_exp = _percentile(exp, 0.20)
    p20_pf = _percentile(pf, 0.20)
    p20_wr = _percentile(wr, 0.20)
    med_trades = float(median(tr)) if tr else 0.0
    penalty = 0.0
    if med_trades < float(penalty_low_trades):
        penalty = (float(penalty_low_trades) - med_trades) / max(1.0, float(penalty_low_trades))
    score = (1.00 * p20_exp) + (0.20 * p20_pf) + (0.10 * p20_wr) - penalty
    return {
        "score": score,
        "p20_expectancy_R": p20_exp,
        "p20_profit_factor": p20_pf,
        "p20_winrate": p20_wr,
        "median_trades": med_trades,
        "penalty_low_trades": penalty,
        "windows_count": len(windows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep V3 params via walk-forward with robust percentile scoring.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT")
    parser.add_argument("--train-days", type=int, default=90)
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--start-days-ago", type=int, default=365)
    parser.add_argument("--cache-only", action="store_true", help="Require cache-only mode (recommended/required).")
    parser.add_argument("--grid-donchian-n-15m", type=str, default="20,30")
    parser.add_argument("--grid-body-atr-15m", type=str, default="0.20,0.25")
    parser.add_argument("--grid-trend-sep-atr-1h", type=str, default="0.60,0.80")
    parser.add_argument("--grid-use-5m-confirm", type=str, default="1,0")
    parser.add_argument("--penalty-low-trades", type=int, default=10)
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()

    if not args.cache_only:
        _log.error("sweep_v3 requires --cache-only. Refusing to run without cache-only mode.")
        return 2

    walkforward_script = _project_root / "scripts" / "walkforward.py"
    if not walkforward_script.exists():
        _log.error("walkforward.py not found: %s", walkforward_script)
        return 1

    donchian_grid = _parse_int_grid(args.grid_donchian_n_15m)
    body_grid = _parse_float_grid(args.grid_body_atr_15m)
    sep_grid = _parse_float_grid(args.grid_trend_sep_atr_1h)
    confirm_grid = _parse_bool_grid(args.grid_use_5m_confirm)

    combos = list(itertools.product(donchian_grid, body_grid, sep_grid, confirm_grid))
    _log.info("SWEEP_V3 combos=%d cache_only=%s", len(combos), bool(args.cache_only))

    rows: List[dict] = []
    data_dir = _project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    for idx, (donchian_n, body_atr, trend_sep, use_5m_confirm) in enumerate(combos, start=1):
        _log.info(
            "Combo %d/%d donchian=%d body_atr=%.4f trend_sep=%.4f use_5m_confirm=%d",
            idx,
            len(combos),
            donchian_n,
            body_atr,
            trend_sep,
            use_5m_confirm,
        )
        cmd = [
            sys.executable,
            str(walkforward_script),
            "--symbols", str(args.symbols),
            "--train-days", str(args.train_days),
            "--test-days", str(args.test_days),
            "--step-days", str(args.step_days),
            "--start-days-ago", str(args.start_days_ago),
            "--cache-only",
            "--strategy", "v3",
            "--donchian-n-15m", str(donchian_n),
            "--body-atr-15m", str(body_atr),
            "--trend-sep-atr-1h", str(trend_sep),
            "--use-5m-confirm", str(int(use_5m_confirm)),
        ]
        proc = subprocess.run(
            cmd,
            cwd=str(_project_root),
            capture_output=True,
            text=True,
            timeout=1800,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            tail = (stderr or stdout)[-1200:]
            _log.error("walkforward failed for combo %d (exit=%d)", idx, proc.returncode)
            _log.error("tail: %s", tail)
            _log.error("Fail-fast due to missing/insufficient cache in --cache-only mode.")
            return 1

        wf_run_id = None
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        for line in combined.splitlines():
            if "Wrote" in line and "walkforward_results_" in line and ".json" in line:
                part = line.split("walkforward_results_")[-1]
                wf_run_id = part.split(".json")[0].strip()
        if not wf_run_id:
            _log.error("Could not parse walkforward run_id from output for combo %d", idx)
            return 1

        wf_json = data_dir / f"walkforward_results_{wf_run_id}.json"
        if not wf_json.exists():
            _log.error("Walkforward result missing: %s", wf_json)
            return 1
        with open(wf_json, "r", encoding="utf-8") as f:
            payload = json.load(f)
        windows = list(payload.get("windows", []) or [])
        robust = _score_combo(windows, penalty_low_trades=int(args.penalty_low_trades))
        rows.append(
            {
                "wf_run_id": wf_run_id,
                "donchian_n_15m": donchian_n,
                "body_atr_15m": body_atr,
                "trend_sep_atr_1h": trend_sep,
                "use_5m_confirm": int(use_5m_confirm),
                **robust,
            }
        )

    rows.sort(key=lambda r: float(r.get("score", 0.0)), reverse=True)
    top_k = max(1, int(args.top_k))

    sweep_id = _run_id(
        {
            "symbols": args.symbols,
            "train_days": args.train_days,
            "test_days": args.test_days,
            "step_days": args.step_days,
            "start_days_ago": args.start_days_ago,
            "cache_only": bool(args.cache_only),
            "grid": {
                "donchian": donchian_grid,
                "body_atr": body_grid,
                "trend_sep": sep_grid,
                "use_5m_confirm": confirm_grid,
            },
            "penalty_low_trades": int(args.penalty_low_trades),
        }
    )
    out_csv = data_dir / f"sweep_v3_{sweep_id}.csv"
    out_json = data_dir / f"sweep_v3_{sweep_id}.json"
    cols = [
        "wf_run_id",
        "donchian_n_15m",
        "body_atr_15m",
        "trend_sep_atr_1h",
        "use_5m_confirm",
        "score",
        "p20_expectancy_R",
        "p20_profit_factor",
        "p20_winrate",
        "median_trades",
        "penalty_low_trades",
        "windows_count",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "sweep_id": sweep_id,
                "args": {
                    "symbols": args.symbols,
                    "train_days": args.train_days,
                    "test_days": args.test_days,
                    "step_days": args.step_days,
                    "start_days_ago": args.start_days_ago,
                    "cache_only": bool(args.cache_only),
                    "penalty_low_trades": int(args.penalty_low_trades),
                    "top_k": top_k,
                },
                "rows": rows,
                "top_k": rows[:top_k],
            },
            f,
            indent=2,
            ensure_ascii=True,
        )

    _log.info("Wrote %s", out_csv)
    _log.info("Wrote %s", out_json)
    print("\n=== SWEEP V3 TOP K ===")
    for i, r in enumerate(rows[:top_k], start=1):
        print(
            f"{i:>2}. score={float(r['score']):.4f} "
            f"donchian={r['donchian_n_15m']} body_atr={float(r['body_atr_15m']):.3f} "
            f"trend_sep={float(r['trend_sep_atr_1h']):.3f} use_5m={r['use_5m_confirm']} "
            f"p20_exp={float(r['p20_expectancy_R']):.4f} med_trades={float(r['median_trades']):.1f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
V3 parameter sweep with walk-forward evaluation and robust scoring.
"""

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
