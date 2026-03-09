#!/usr/bin/env python3
"""
Parameter sweep runner for replay. Runs each config in an isolated subprocess.
DRY RUN only. No Telegram. Outputs top 5 configs to ./data/sweep_top5.json.
V2 TREND_PULLBACK_EMA20 sweep: STRATEGY_V1=0, V2_TREND_PULLBACK=1 for all configs.
"""
from __future__ import annotations

import itertools
import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_replay_stdout(stdout: str) -> dict:
    """Parse replay stdout for KPI summary. Returns dict with expectancy_R, avg_win_R, etc."""
    out = {
        "expectancy_R": 0.0,
        "avg_win_R": 0.0,
        "avg_loss_R": 0.0,
        "trades_total": 0,
        "winrate": 0.0,
        "max_dd_usdt": 0.0,
        "profit_factor": 0.0,
    }
    for line in stdout.splitlines():
        if "trades_total=" in line:
            m = re.search(r"trades_total=(\d+)", line)
            if m:
                out["trades_total"] = int(m.group(1))
        if "expectancy_R=" in line:
            m = re.search(r"expectancy_R=([-\d.]+)", line)
            if m:
                out["expectancy_R"] = float(m.group(1))
        if "avg_win_R=" in line:
            m = re.search(r"avg_win_R=([-\d.]+)", line)
            if m:
                out["avg_win_R"] = float(m.group(1))
        if "avg_loss_R=" in line:
            m = re.search(r"avg_loss_R=([-\d.]+)", line)
            if m:
                out["avg_loss_R"] = float(m.group(1))
        if "winrate=" in line and "expectancy" in line:
            m = re.search(r"winrate=([-\d.]+)%", line)
            if m:
                out["winrate"] = float(m.group(1)) / 100.0
        if "max_dd_usdt=" in line:
            m = re.search(r"max_dd_usdt=([-\d.]+)", line)
            if m:
                out["max_dd_usdt"] = float(m.group(1))
        if "profit_factor=" in line:
            m = re.search(r"profit_factor=([-\d.]+)", line)
            if m:
                out["profit_factor"] = float(m.group(1))
    return out


def main() -> int:
    symbols = ["BTCUSDT", "ETHUSDT"]
    symbols_str = ",".join(symbols)
    days = 7
    tf_trigger = 15
    tf_timing = 5
    tf_bias = 240
    tf_setup = 60
    step_bars = 1

    # Fixed for all configs: V2 TREND_PULLBACK only, TP_R=2.0, SL_ATR=0.8, TOL_ATR=0.05
    fixed_env = {
        "STRATEGY_V1": "0",
        "V2_TREND_PULLBACK": "1",
        "PULLBACK_TP_R": "2.0",
        "PULLBACK_SL_ATR_MULT": "0.8",
        "PULLBACK_TOL_ATR": "0.05",
    }

    grid = {
        "TREND_MIN_SEP_ATR": [0.25, 0.35, 0.50],
        "MOMO_MIN_BODY_ATR_5M": [0.20, 0.25, 0.35],
    }

    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    results: list[dict] = []

    replay_script = _project_root / "scripts" / "replay.py"
    if not replay_script.exists():
        _log.error("Replay script not found: %s", replay_script)
        return 1

    cmd_base = [
        sys.executable,
        str(replay_script),
        "--days", str(days),
        "--symbols", symbols_str,
        "--tf-trigger", str(tf_trigger),
        "--tf-timing", str(tf_timing),
        "--tf-bias", str(tf_bias),
        "--tf-setup", str(tf_setup),
        "--step-bars", str(step_bars),
    ]

    total = 1
    for v in values:
        total *= len(v)
    _log.info("Running %d configs in isolated subprocesses (cwd=%s)", total, _project_root)

    for i, combo in enumerate(itertools.product(*values)):
        overrides = dict(zip(keys, combo))
        env = dict(os.environ)
        for k, v in fixed_env.items():
            env[k] = str(v)
        for k, v in overrides.items():
            env[k] = str(v)

        proc = subprocess.run(
            cmd_base,
            env=env,
            cwd=str(_project_root),
            capture_output=True,
            text=True,
            timeout=300,
        )

        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if proc.returncode != 0:
            _log.warning("Config %s failed (exit=%d): %s", overrides, proc.returncode, (proc.stderr or "")[:200])
            kpi = {}
        else:
            kpi = _parse_replay_stdout(combined)

        if "=== REPLAY KPI SUMMARY ===" not in combined:
            _log.warning("Config %s: REPLAY KPI SUMMARY not found in output", overrides)

        row = {
            "config": overrides,
            "expectancy_R": kpi.get("expectancy_R", 0.0),
            "avg_win_R": kpi.get("avg_win_R", 0.0),
            "avg_loss_R": kpi.get("avg_loss_R", 0.0),
            "max_dd_usdt": kpi.get("max_dd_usdt", 0.0),
            "trades_total": kpi.get("trades_total", 0),
            "winrate": kpi.get("winrate", 0.0),
            "profit_factor": kpi.get("profit_factor", 0.0),
        }
        results.append(row)
        _log.info(
            "  [%d/%d] TREND_SEP=%s MOMO_BODY=%s -> exp_R=%.4f trades=%d",
            i + 1, total,
            overrides.get("TREND_MIN_SEP_ATR", "?"),
            overrides.get("MOMO_MIN_BODY_ATR_5M", "?"),
            row["expectancy_R"],
            row["trades_total"],
        )

    results.sort(key=lambda r: (-r["expectancy_R"], r["max_dd_usdt"]))
    top5 = results[:5]

    print("\n=== SWEEP RESULTS (sorted by expectancy_R desc, max_dd_usdt asc) ===")
    print(
        f"{'TREND_SEP':>9} {'MOMO_BODY':>9} {'exp_R':>7} {'winrate':>8} "
        f"{'avg_win_R':>9} {'avg_loss_R':>10} {'trades':>6} {'max_dd_usdt':>11}"
    )
    print("-" * 90)
    for r in results[:10]:
        cfg = r["config"]
        print(
            f"{cfg.get('TREND_MIN_SEP_ATR', '?'):>9} {cfg.get('MOMO_MIN_BODY_ATR_5M', '?'):>9} "
            f"{r['expectancy_R']:>7.4f} {(r['winrate'] or 0)*100:>7.1f}% "
            f"{r['avg_win_R']:>9.4f} {r['avg_loss_R']:>10.4f} "
            f"{r['trades_total']:>6} {r['max_dd_usdt']:>11.2f}"
        )

    data_dir = _project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "sweep_top5.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(top5, f, indent=2, ensure_ascii=True)
    _log.info("\nTop 5 configs written to %s", out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
