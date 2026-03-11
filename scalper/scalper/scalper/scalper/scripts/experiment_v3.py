#!/usr/bin/env python3
"""
V3 A/B experiment runner. Runs replay for baseline vs variant, outputs comparison.
Deterministic. No RiskEngine changes. Uses scripts/replay run_replay with v3_params_override.

Usage:
  python scripts/experiment_v3.py --symbols "BTCUSDT,ETHUSDT" --days 14 --tag "exp1"
  python scripts/experiment_v3.py --baseline --variant "USE_5M_CONFIRM=0;ATR_REGIME_MIN_PCTL=50" --symbols "BTCUSDT" --days 7 --tag "exp_5m_off"
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "scripts") not in sys.path:
    sys.path.insert(0, str(_project_root / "scripts"))

os.environ.setdefault("V3_TREND_BREAKOUT", "1")
os.environ.setdefault("V2_TREND_PULLBACK", "0")
os.environ.setdefault("STRATEGY_V1", "0")


def _parse_variant(s: str) -> dict:
    """Parse 'KEY=VAL;KEY2=VAL2' into dict. Booleans: 1/0, true/false."""
    out = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if v.lower() in ("1", "true", "yes"):
            out[k] = True
        elif v.lower() in ("0", "false", "no"):
            out[k] = False
        elif v.isdigit():
            out[k] = int(v)
        else:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out


def _extract_metrics(result: dict, days: int) -> dict:
    kpi = result.get("kpi") or {}
    trades = int(kpi.get("trades_total", 0) or 0)
    return {
        "PF": float(kpi.get("profit_factor", 0) or 0),
        "maxDD": float(kpi.get("max_dd_usdt", 0) or 0),
        "winrate": float(kpi.get("winrate", 0) or 0) * 100.0,
        "trades_per_day": round(trades / max(days, 1), 2),
        "avgR": float(kpi.get("expectancy_R", 0) or 0),
        "trades": trades,
        "run_id": result.get("run_id", ""),
    }


def run_experiment(
    symbols: str,
    days: int,
    tag: str,
    variant: str = "",
    *,
    end_days_ago: int = 0,
    silent: bool = True,
) -> dict:
    from replay import run_replay

    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        raise ValueError("No symbols")

    out_dir = Path("runs") / "experiments" / (tag or "default")
    out_dir.mkdir(parents=True, exist_ok=True)

    baseline_result = run_replay(
        symbols=sym_list,
        days=days,
        tf_trigger=15,
        tf_timing=5,
        tf_bias=240,
        tf_setup=60,
        step_bars=1,
        use_cache=True,
        silent=silent,
        end_days_ago=end_days_ago if end_days_ago else None,
        v3_params_override=None,
    )
    baseline_metrics = _extract_metrics(baseline_result, days)

    variant_override = _parse_variant(variant) if variant else None
    variant_result = run_replay(
        symbols=sym_list,
        days=days,
        tf_trigger=15,
        tf_timing=5,
        tf_bias=240,
        tf_setup=60,
        step_bars=1,
        use_cache=True,
        silent=silent,
        end_days_ago=end_days_ago if end_days_ago else None,
        v3_params_override=variant_override,
    )
    variant_metrics = _extract_metrics(variant_result, days)

    diff = {
        "PF": round(variant_metrics["PF"] - baseline_metrics["PF"], 4),
        "maxDD": round(variant_metrics["maxDD"] - baseline_metrics["maxDD"], 4),
        "winrate": round(variant_metrics["winrate"] - baseline_metrics["winrate"], 2),
        "trades_per_day": round(variant_metrics["trades_per_day"] - baseline_metrics["trades_per_day"], 2),
        "avgR": round(variant_metrics["avgR"] - baseline_metrics["avgR"], 4),
        "trades": variant_metrics["trades"] - baseline_metrics["trades"],
    }

    summary = {
        "tag": tag,
        "symbols": sym_list,
        "days": days,
        "variant": variant or "(none)",
        "variant_params": variant_override or {},
        "baseline": baseline_metrics,
        "variant": variant_metrics,
        "diff": diff,
    }

    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(out_dir / "diff.json", "w", encoding="utf-8") as f:
        json.dump(diff, f, indent=2)

    return summary


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="V3 A/B experiment: baseline vs variant replay comparison.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--days", type=int, default=14, help="Days of history")
    parser.add_argument("--tag", type=str, default="exp", help="Experiment tag (output dir)")
    parser.add_argument("--variant", type=str, default="", help='Variant params: "USE_5M_CONFIRM=0;ATR_REGIME_MIN_PCTL=50"')
    parser.add_argument("--end-days-ago", type=int, default=0, help="End date = now - N days (0 = now)")
    parser.add_argument("--baseline", action="store_true", help="No-op; baseline always runs for comparison")
    parser.add_argument("--verbose", action="store_true", help="Show replay logs")
    args = parser.parse_args()

    summary = run_experiment(
        symbols=args.symbols,
        days=args.days,
        tag=args.tag,
        variant=args.variant,
        end_days_ago=args.end_days_ago if args.end_days_ago else 0,
        silent=not args.verbose,
    )
    out_dir = Path("runs") / "experiments" / (args.tag or "exp")
    print(f"Experiment complete. Output: {out_dir}/summary.json, {out_dir}/diff.json")
    print(f"Baseline: PF={summary['baseline']['PF']:.2f} maxDD={summary['baseline']['maxDD']:.2f} winrate={summary['baseline']['winrate']:.1f}% trades/day={summary['baseline']['trades_per_day']:.2f} avgR={summary['baseline']['avgR']:.4f}")
    print(f"Variant:  PF={summary['variant']['PF']:.2f} maxDD={summary['variant']['maxDD']:.2f} winrate={summary['variant']['winrate']:.1f}% trades/day={summary['variant']['trades_per_day']:.2f} avgR={summary['variant']['avgR']:.4f}")
    print(f"Diff:     PF={summary['diff']['PF']:+.2f} maxDD={summary['diff']['maxDD']:+.2f} winrate={summary['diff']['winrate']:+.1f}% trades/day={summary['diff']['trades_per_day']:+.2f} avgR={summary['diff']['avgR']:+.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
