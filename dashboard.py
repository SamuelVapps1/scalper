from datetime import datetime, timezone
from typing import Any, Dict, List

from storage import get_risk_metrics


def _fmt_float(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def build_dashboard_report(run_context: Dict[str, Any]) -> str:
    ts = str(run_context.get("ts") or datetime.now(timezone.utc).isoformat())
    interval = str(run_context.get("interval", "n/a"))
    watchlist = run_context.get("watchlist", []) or []
    watchlist_mode = str(run_context.get("watchlist_mode", "static"))
    paper_state = run_context.get("paper_state", {}) or {}
    risk_metrics = get_risk_metrics(paper_state)
    symbols = run_context.get("symbols", []) or []
    include_snapshot = bool(run_context.get("include_market_snapshot", True))
    include_blocked = bool(run_context.get("include_blocked", True))
    include_debug_why_none = bool(run_context.get("include_debug_why_none", False))
    top_n = max(1, int(run_context.get("top_n", 3)))

    open_positions = paper_state.get("open_positions") or []
    open_count = len(open_positions) if isinstance(open_positions, list) else 0
    max_open = int(run_context.get("max_open_positions", 0))
    trade_count_today = int(risk_metrics.get("trade_count_today", 0) or 0)
    daily_pnl_sim = float(risk_metrics.get("daily_pnl_sim", 0.0) or 0.0)
    consecutive_losses = int(risk_metrics.get("consecutive_losses", 0) or 0)
    cooldown_until = str(risk_metrics.get("cooldown_until_utc", "") or "").strip()
    cooldown_text = cooldown_until if cooldown_until else "OFF"

    lines: List[str] = []
    lines.append(f"📊 BybitSignalBot Dashboard (DRY RUN) | TF={interval}m | ts={ts}")
    lines.append(
        f"Watchlist (mode={watchlist_mode}): {', '.join(watchlist) if watchlist else 'n/a'}"
    )
    lines.append(
        f"Risk: open={open_count}/{max_open} trades_today={trade_count_today} "
        f"daily_pnl_sim={daily_pnl_sim:.4f} consec_losses={consecutive_losses} "
        f"cooldown={cooldown_text}"
    )
    if open_count:
        lines.append("Open Positions:")
        for pos in open_positions[: min(open_count, 5)]:
            lines.append(
                "- "
                f"{pos.get('symbol', '?')} {pos.get('side', '?')} {pos.get('strategy', '?')} "
                f"entry={_fmt_float(pos.get('entry_price'))} sl={_fmt_float(pos.get('sl_price'))} "
                f"tp={_fmt_float(pos.get('tp_price'))} bars={int(pos.get('bars_held', 0))}"
            )
    closed_trades = paper_state.get("closed_trades") or []
    if isinstance(closed_trades, list) and closed_trades:
        lines.append("Last Closed Trades:")
        for trade in closed_trades[-3:]:
            lines.append(
                "- "
                f"{trade.get('symbol', '?')} {trade.get('side', '?')} {trade.get('strategy', '?')} "
                f"pnl={_fmt_float(trade.get('pnl_usdt'))} reason={trade.get('close_reason', 'n/a')}"
            )
    lines.append("")

    if include_snapshot:
        lines.append("Market Snapshot:")
        for symbol_ctx in symbols:
            snap = symbol_ctx.get("market_snapshot", {}) or {}
            if not snap:
                symbol_name = symbol_ctx.get("symbol", "?")
                err = symbol_ctx.get("error", "snapshot_unavailable")
                lines.append(f"- {symbol_name} snapshot unavailable ({err})")
                continue
            symbol_name = snap.get("symbol", "?")
            lines.append(
                "- "
                f"{symbol_name} px={_fmt_float(snap.get('last_close'))} "
                f"ATR14={_fmt_float(snap.get('atr14'))} ({_fmt_float(snap.get('atr14_pct'), 2)}%) "
                f"EMA200={_fmt_float(snap.get('ema200'))} dist={_fmt_float(snap.get('ema_distance_pct'), 2)}% "
                f"range[24]=L{_fmt_float(snap.get('range_low'))} H{_fmt_float(snap.get('range_high'))} "
                f"pos={snap.get('range_position', 'n/a')} bar_ts_used={snap.get('bar_ts_used', snap.get('ts', 'n/a'))}"
            )
        lines.append("")

    ranked_intents: List[Dict[str, Any]] = []
    collisions: List[Dict[str, Any]] = []
    for symbol_ctx in symbols:
        collisions.extend(symbol_ctx.get("collisions", []) or [])
        for item in symbol_ctx.get("final_intents", []) or []:
            verdict = item.get("risk", {}) or {}
            allowed = bool(verdict.get("allowed", False))
            if not allowed and not include_blocked:
                continue
            ranked_intents.append(item)

    ranked_intents.sort(key=lambda x: float(x.get("confidence", 0.0)), reverse=True)
    lines.append("Top Intents:")
    if not ranked_intents:
        lines.append("- none")
        if include_debug_why_none:
            lines.append("")
            lines.append("Debug (why none):")
            debug_lines = 0
            for symbol_ctx in symbols:
                if debug_lines >= 10:
                    break
                compact = symbol_ctx.get("debug_why_none", {}) or {}
                if not compact:
                    continue
                lines.append(
                    "- "
                    f"{symbol_ctx.get('symbol', '?')} "
                    f"RB_RTG={compact.get('RB_RTG', 'n/a')} "
                    f"FB_FADE={compact.get('FB_FADE', 'n/a')}"
                )
                debug_lines += 1
            if debug_lines == 0:
                lines.append("- n/a")
    else:
        for idx, intent in enumerate(ranked_intents[:top_n], start=1):
            risk = intent.get("risk", {}) or {}
            verdict = "ALLOW" if risk.get("allowed") else "BLOCK"
            lines.append(
                f"{idx}) {intent.get('symbol', '?')} {intent.get('side', '?')} "
                f"{intent.get('strategy', '?')} conf={float(intent.get('confidence', 0.0)):.2f} "
                f"verdict={verdict} bar_ts_used={intent.get('bar_ts_used', intent.get('ts', 'n/a'))}"
            )
            lines.append(f"   reason: {intent.get('reason', '')}")
            lines.append(f"   risk_reason: {risk.get('reason', '')}")

    lines.append("")
    lines.append("Collisions (same bar):")
    if not collisions:
        lines.append("- none")
    else:
        for col in collisions:
            dropped = col.get("dropped", []) or []
            dropped_txt = ", ".join(
                f"{d.get('strategy', '?')}({float(d.get('confidence', 0.0)):.2f})"
                for d in dropped
            )
            lines.append(
                f"- {col.get('symbol', '?')}: chose {col.get('chosen_strategy', '?')}"
                f"({float(col.get('chosen_confidence', 0.0)):.2f}) over {dropped_txt or 'n/a'}"
            )

    return "\n".join(lines)
