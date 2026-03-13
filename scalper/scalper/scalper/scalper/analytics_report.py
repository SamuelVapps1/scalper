from __future__ import annotations

import argparse
import json
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from scalper import sqlite_store

log = logging.getLogger(__name__)


@dataclass
class AnalyticsSummary:
    total_intents: int
    total_allow: int
    total_block: int
    paper_trades: int
    tp_count: int
    sl_count: int
    timeout_count: int
    other_exit_count: int
    winrate: float
    expectancy: float
    profit_factor: float
    avg_rr: float
    degraded_plan_rate: float
    atr_failure_rate: float
    cooldown_block_rate: float
    consecutive_losses_max: int
    per_setup: Dict[str, Dict[str, Any]]
    per_symbol: Dict[str, Dict[str, Any]]


def _fetch_recent_trade_intents(limit: int = 10000) -> List[Dict[str, Any]]:
    sqlite_store._ensure_db()
    with sqlite_store._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT ts, symbol, setup, timeframe, risk_verdict, block_reason, json "
            "FROM trade_intents ORDER BY ts DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        base = dict(r)
        try:
            extra = json.loads(base.get("json") or "{}")
        except Exception:
            extra = {}
        merged = {**extra, **base}
        out.append(merged)
    return out


def _fetch_recent_trade_records(limit: int = 10000) -> List[Dict[str, Any]]:
    sqlite_store._ensure_db()
    with sqlite_store._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT * FROM trade_records ORDER BY ts_close DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        base = dict(r)
        try:
            extra = json.loads(base.get("json") or "{}")
        except Exception:
            extra = {}
        merged = {**extra, **base}
        out.append(merged)
    return out


def compute_analytics(
    intents: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
) -> AnalyticsSummary:
    total_intents = len(intents)
    allow = [i for i in intents if str(i.get("risk_verdict", "")).upper() == "ALLOW"]
    block = [i for i in intents if str(i.get("risk_verdict", "")).upper() == "BLOCK"]
    total_allow = len(allow)
    total_block = len(block)

    closed = [r for r in records if r.get("ts_close")]
    paper_trades = len(closed)

    # Exit reason counts
    tp_count = sum(1 for r in closed if str(r.get("close_reason", "")).upper() == "TP")
    sl_count = sum(1 for r in closed if str(r.get("close_reason", "")).upper() == "SL")
    timeout_count = sum(1 for r in closed if str(r.get("close_reason", "")).upper() == "TIMEOUT")
    other_exit_count = paper_trades - tp_count - sl_count - timeout_count

    wins = [r for r in closed if float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0) > 0]
    losses = [r for r in closed if float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0) < 0]
    winrate = (len(wins) / paper_trades * 100.0) if paper_trades else 0.0

    total_pnl = sum(float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0) for r in closed)
    avg_pnl = total_pnl / paper_trades if paper_trades else 0.0

    # Expectancy, profit factor, RR
    rr_values: List[float] = []
    gross_win = 0.0
    gross_loss = 0.0
    for r in closed:
        pnl = float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0)
        rr = r.get("pnl_r") or r.get("pnl_R") or r.get("r_multiple")
        if rr is not None:
            try:
                rr_values.append(float(rr))
            except (TypeError, ValueError):
                pass
        if pnl > 0:
            gross_win += pnl
        elif pnl < 0:
            gross_loss += abs(pnl)
    avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0.0
    avg_win = (gross_win / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    expectancy = (len(wins) / paper_trades * avg_win - len(losses) / paper_trades * avg_loss) if paper_trades else 0.0
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else 0.0

    # Degraded / ATR / cooldown rates from block reasons in intents
    block_reasons = [str(i.get("block_reason", "") or "") for i in block]
    degraded_blocks = [r for r in block_reasons if "TRADE_PLAN_ERROR" in r or "INVALID_TRADE_PLAN" in r or "PLAN_DEGRADED" in r]
    atr_fail_blocks = [r for r in block_reasons if "ATR_UNAVAILABLE" in r or "ATR_DEGRADED" in r]
    cooldown_blocks = [r for r in block_reasons if "cooldown" in r.lower()]
    degraded_plan_rate = (len(degraded_blocks) / total_intents * 100.0) if total_intents else 0.0
    atr_failure_rate = (len(atr_fail_blocks) / total_intents * 100.0) if total_intents else 0.0
    cooldown_block_rate = (len(cooldown_blocks) / total_intents * 100.0) if total_intents else 0.0

    # Max consecutive losses from records
    pnl_series = [float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0) for r in reversed(closed)]
    max_consec = 0
    current = 0
    for pnl in pnl_series:
        if pnl < 0:
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0

    # Per-setup and per-symbol
    per_setup: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"trades": 0, "pnl_usdt": 0.0, "wins": 0, "losses": 0})
    per_symbol: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"trades": 0, "pnl_usdt": 0.0, "wins": 0, "losses": 0})
    for r in closed:
        setup = str(r.get("strategy", r.get("setup", "")) or "")
        symbol = str(r.get("symbol", "") or "")
        pnl = float(r.get("pnl_usdt", r.get("pnl", 0.0)) or 0.0)
        for bucket, key in ((per_setup, setup), (per_symbol, symbol)):
            b = bucket[key]
            b["trades"] += 1
            b["pnl_usdt"] += pnl
            if pnl > 0:
                b["wins"] += 1
            elif pnl < 0:
                b["losses"] += 1

    return AnalyticsSummary(
        total_intents=total_intents,
        total_allow=total_allow,
        total_block=total_block,
        paper_trades=paper_trades,
        tp_count=tp_count,
        sl_count=sl_count,
        timeout_count=timeout_count,
        other_exit_count=other_exit_count,
        winrate=winrate,
        expectancy=expectancy,
        profit_factor=profit_factor,
        avg_rr=avg_rr,
        degraded_plan_rate=degraded_plan_rate,
        atr_failure_rate=atr_failure_rate,
        cooldown_block_rate=cooldown_block_rate,
        consecutive_losses_max=max_consec,
        per_setup=per_setup,
        per_symbol=per_symbol,
    )


def _print_human_readable(summary: AnalyticsSummary) -> None:
    print("=== Scalper Performance Analytics ===")
    print(f"Total intents: {summary.total_intents} | allow={summary.total_allow} block={summary.total_block}")
    print(f"Paper trades: {summary.paper_trades} | TP={summary.tp_count} SL={summary.sl_count} TIMEOUT={summary.timeout_count} OTHER={summary.other_exit_count}")
    print(f"Winrate: {summary.winrate:.2f}%")
    print(f"Expectancy (per trade): {summary.expectancy:.4f} USDT")
    print(f"Profit factor: {summary.profit_factor:.3f}")
    print(f"Average realized R: {summary.avg_rr:.3f}")
    print(f"Max consecutive losses: {summary.consecutive_losses_max}")
    print(f"Degraded-plan block rate: {summary.degraded_plan_rate:.2f}%")
    print(f"ATR failure block rate: {summary.atr_failure_rate:.2f}%")
    print(f"Cooldown block rate: {summary.cooldown_block_rate:.2f}%")
    print("")
    print("Per-setup performance (top 10 by trades):")
    for setup, stats in sorted(summary.per_setup.items(), key=lambda kv: kv[1]["trades"], reverse=True)[:10]:
        trades = stats["trades"]
        if trades == 0:
            continue
        pnl = stats["pnl_usdt"]
        wins = stats["wins"]
        losses = stats["losses"]
        wr = (wins / trades * 100.0) if trades else 0.0
        print(f"  {setup or '<unknown>'}: trades={trades} pnl={pnl:.4f} wins={wins} losses={losses} winrate={wr:.2f}%")
    print("")
    print("Per-symbol performance (top 10 by trades):")
    for symbol, stats in sorted(summary.per_symbol.items(), key=lambda kv: kv[1]["trades"], reverse=True)[:10]:
        trades = stats["trades"]
        if trades == 0:
            continue
        pnl = stats["pnl_usdt"]
        wins = stats["wins"]
        losses = stats["losses"]
        wr = (wins / trades * 100.0) if trades else 0.0
        print(f"  {symbol or '<unknown>'}: trades={trades} pnl={pnl:.4f} wins={wins} losses={losses} winrate={wr:.2f}%")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scalper performance analytics report.")
    parser.add_argument(
        "--recent-intents",
        type=int,
        default=5000,
        help="Number of most recent trade_intents rows to include (default: 5000).",
    )
    parser.add_argument(
        "--recent-trades",
        type=int,
        default=2000,
        help="Number of most recent trade_records rows to include (default: 2000).",
    )
    parser.add_argument(
        "--json",
        type=str,
        default="",
        help="Optional path to write JSON summary to.",
    )
    args = parser.parse_args(argv)

    intents = _fetch_recent_trade_intents(limit=args.recent_intents)
    records = _fetch_recent_trade_records(limit=args.recent_trades)
    if not intents and not records:
        print("No intents or trade records found in DB; run scanner in paper mode first.")
        return 0

    summary = compute_analytics(intents, records)
    _print_human_readable(summary)

    if args.json:
        path = Path(args.json)
        payload = asdict(summary)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nJSON summary written to: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

