from __future__ import annotations

from typing import Any, Dict

STRATEGY_HUMAN_NOTES = {
    "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE": "EMA200 fade after sweep (trap)",
    "RANGE_BREAKOUT_RETEST_GO": "Range breakout -> retest -> go",
    "TREND_PULLBACK_EMA20": "V2 trend pullback to EMA20",
    "EMA_PULLBACK_GO": "EMA pullback in trend (ema20/50)",
    "FORCE_TEST": "Synthetic stress-test intent",
}


_TRUNCATED_SUFFIX = "...(truncated)"


def _fmt_float(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def _cooldown_text(cooldown_until_utc: Any) -> str:
    raw = str(cooldown_until_utc or "").strip()
    return raw if raw else "OFF"


def _cooldown_on_off(cooldown_until_utc: Any) -> str:
    return "ON" if str(cooldown_until_utc or "").strip() else "OFF"


def _truncate_message(text: str, max_chars: int) -> str:
    msg = str(text or "")
    cap = max(80, int(max_chars or 0))
    if len(msg) <= cap:
        return msg
    keep = max(1, cap - len(_TRUNCATED_SUFFIX))
    return msg[:keep] + _TRUNCATED_SUFFIX


def _compact_or_verbose(
    lines_compact: list[str],
    lines_verbose: list[str],
    ctx: Dict[str, Any],
) -> str:
    fmt = str(ctx.get("telegram_format", "compact") or "compact").strip().lower()
    compact_max = int(ctx.get("telegram_max_chars_compact", 900) or 900)
    verbose_max = int(ctx.get("telegram_max_chars_verbose", 2500) or 2500)
    if fmt == "verbose":
        return _truncate_message("\n".join(lines_verbose), verbose_max)
    return _truncate_message("\n".join(lines_compact), compact_max)


def _strategy_display(raw_strategy: Any) -> str:
    strategy = str(raw_strategy or "?")
    human = STRATEGY_HUMAN_NOTES.get(strategy, strategy.replace("_", " ").title())
    return f"{human} ({strategy})"


def format_intent_allow(intent: Dict[str, Any], risk: Dict[str, Any], market_ctx: Dict[str, Any]) -> str:
    symbol = str(intent.get("symbol", "?"))
    tf = str(market_ctx.get("tf", "n/a"))
    side = str(intent.get("side", "?"))
    strategy = _strategy_display(intent.get("strategy", "?"))
    conf = _fmt_float(intent.get("confidence", 0.0), 2)
    bias = str(market_ctx.get("bias", "") or "").upper()
    entry = _fmt_float(market_ctx.get("entry"))
    sl = _fmt_float(market_ctx.get("sl"))
    tp = _fmt_float(market_ctx.get("tp"))
    sl_pct = _fmt_float(market_ctx.get("sl_pct"), 2)
    tp_pct = _fmt_float(market_ctx.get("tp_pct"), 2)
    qty = _fmt_float(market_ctx.get("qty"), 6)
    notional = _fmt_float(market_ctx.get("notional"), 2)
    bar_ts_used = str(market_ctx.get("bar_ts_used", "") or "")
    intent_id = str(intent.get("intent_id", market_ctx.get("intent_id", "")) or "")
    note = str(intent.get("reason", "") or "")
    risk_reason = str(risk.get("reason", "") or "")
    open_now = int(market_ctx.get("open_now", 0) or 0)
    open_max = int(market_ctx.get("open_max", 0) or 0)
    trades_today = int(market_ctx.get("trades_today", 0) or 0)
    cooldown = _cooldown_text(market_ctx.get("cooldown_until_utc"))
    cooldown_state = _cooldown_on_off(market_ctx.get("cooldown_until_utc"))
    human_note = STRATEGY_HUMAN_NOTES.get(str(intent.get("strategy", "")), note or "n/a")
    break_level = market_ctx.get("break_level")
    retest_level = market_ctx.get("retest_level")
    levels_str = ""
    if break_level is not None or retest_level is not None:
        break_s = _fmt_float(break_level) if break_level is not None else "n/a"
        retest_s = _fmt_float(retest_level) if retest_level is not None else "n/a"
        levels_str = f" break={break_s} retest={retest_s}"

    compact = [
        f"ALLOW[{tf}m] {symbol} {side} conf={conf}",
        f"setup={human_note} bias={bias or 'n/a'}{levels_str}",
        f"entry={entry} sl={sl} tp={tp} sl%={sl_pct} tp%={tp_pct}",
        f"bar_ts={bar_ts_used}",
        f"risk open={open_now}/{open_max} trades={trades_today} cooldown={cooldown_state}",
    ]
    verbose = [
        f"✅ CONFIRMED[{tf}m] ALLOW",
        f"{symbol} {side} {strategy} conf={conf} bias={bias or 'n/a'}",
        f"break={_fmt_float(break_level) if break_level is not None else 'n/a'} retest={_fmt_float(retest_level) if retest_level is not None else 'n/a'}",
        f"entry={entry} sl={sl} tp={tp} sl%={sl_pct} tp%={tp_pct} qty={qty} notional={notional}",
        f"bar_ts_used={bar_ts_used} intent_id={intent_id}",
        f"setup_note={note}",
        f"risk_reason={risk_reason}",
        f"risk open={open_now}/{open_max} trades_today={trades_today} cooldown={cooldown}",
        "mode=DRY_RUN",
    ]
    return _compact_or_verbose(compact, verbose, market_ctx)


def format_intent_block(intent: Dict[str, Any], risk: Dict[str, Any], market_ctx: Dict[str, Any]) -> str:
    symbol = str(intent.get("symbol", "?"))
    tf = str(market_ctx.get("tf", "n/a"))
    side = str(intent.get("side", "?"))
    strategy = _strategy_display(intent.get("strategy", "?"))
    conf = _fmt_float(intent.get("confidence", 0.0), 2)
    note = str(intent.get("reason", "") or "")
    risk_reason = str(risk.get("reason", "") or "")
    human_note = STRATEGY_HUMAN_NOTES.get(str(intent.get("strategy", "")), note or "n/a")

    compact = [
        f"BLOCK[{tf}m] {symbol} {side} conf={conf}",
        f"setup={human_note}",
        f"risk_reason={risk_reason}",
    ]
    verbose = [
        f"⛔ BLOCKED[{tf}m]",
        f"{symbol} {side} {strategy} conf={conf}",
        f"setup_note={note}",
        f"risk_reason={risk_reason}",
        "mode=DRY_RUN",
    ]
    return _compact_or_verbose(compact, verbose, market_ctx)


def format_paper_close(trade: Dict[str, Any], state_after: Dict[str, Any]) -> str:
    symbol = str(trade.get("symbol", "?"))
    side = str(trade.get("side", "?"))
    strategy = _strategy_display(trade.get("strategy", "?"))
    pnl = _fmt_float(trade.get("pnl_usdt"))
    close_reason = str(trade.get("close_reason", "n/a"))
    bars_held = int(trade.get("bars_held", 0) or 0)
    tf = str(state_after.get("tf", "15"))
    open_positions = list(state_after.get("open_positions", []) or [])
    open_now = int(state_after.get("open_now", len(open_positions)) or 0)
    open_max = int(state_after.get("open_max", 0) or 0)
    trades_today = int(state_after.get("trades_today", state_after.get("trade_count_today", 0)) or 0)
    daily_pnl_sim = _fmt_float(state_after.get("daily_pnl_sim"))
    consec_losses = int(state_after.get("consec_losses", state_after.get("consecutive_losses", 0)) or 0)
    cooldown = _cooldown_text(state_after.get("cooldown_until_utc"))
    cooldown_state = _cooldown_on_off(state_after.get("cooldown_until_utc"))

    compact = [
        f"CLOSE[{tf}m] {symbol} {side}",
        f"pnl={pnl} reason={close_reason} bars={bars_held}",
        f"after daily_pnl={daily_pnl_sim} consec_losses={consec_losses} cooldown={cooldown_state}",
    ]
    verbose = [
        f"📉 PAPER CLOSE[{tf}m]",
        f"{symbol} {side} {strategy}",
        f"pnl={pnl} reason={close_reason} bars_held={bars_held}",
        f"open={open_now}/{open_max} trades_today={trades_today}",
        f"after daily_pnl_sim={daily_pnl_sim} consec_losses={consec_losses} cooldown={cooldown}",
        "mode=DRY_RUN",
    ]
    return _compact_or_verbose(compact, verbose, state_after)


def format_paper_open(open_event: Dict[str, Any], market_ctx: Dict[str, Any]) -> str:
    symbol = str(open_event.get("symbol", "?"))
    tf = str(market_ctx.get("tf", "n/a"))
    side = str(open_event.get("side", "?"))
    strategy = _strategy_display(open_event.get("strategy", "?"))
    conf = _fmt_float(open_event.get("confidence", 0.0), 2)
    entry = _fmt_float(open_event.get("entry"))
    sl = _fmt_float(open_event.get("sl"))
    tp = _fmt_float(open_event.get("tp"))
    sl_pct = _fmt_float(open_event.get("sl_pct"), 2)
    tp_pct = _fmt_float(open_event.get("tp_pct"), 2)
    qty = _fmt_float(open_event.get("qty"), 6)
    notional = _fmt_float(open_event.get("notional"), 2)
    bar_ts_used = str(open_event.get("bar_ts_used", "") or "")
    intent_id = str(open_event.get("intent_id", "") or "")
    risk_reason = str(open_event.get("risk_reason", "") or "")
    note = str(open_event.get("note", "") or "")
    open_now = int(market_ctx.get("open_now", 0) or 0)
    open_max = int(market_ctx.get("open_max", 0) or 0)
    trades_today = int(market_ctx.get("trades_today", 0) or 0)
    cooldown = _cooldown_text(market_ctx.get("cooldown_until_utc"))
    cooldown_state = _cooldown_on_off(market_ctx.get("cooldown_until_utc"))
    human_note = STRATEGY_HUMAN_NOTES.get(str(open_event.get("strategy", "")), note or "n/a")
    compact = [
        f"OPEN[{tf}m] {symbol} {side} conf={conf}",
        f"entry={entry} sl={sl} tp={tp}",
        f"sl%={sl_pct} tp%={tp_pct}",
        f"setup={human_note}",
        f"risk open={open_now}/{open_max} trades={trades_today} cooldown={cooldown_state}",
    ]
    verbose = [
        f"🟦 PAPER OPEN[{tf}m]",
        f"{symbol} {side} {strategy} conf={conf}",
        f"entry={entry} sl={sl} tp={tp} sl%={sl_pct} tp%={tp_pct} qty={qty} notional={notional}",
        f"bar_ts_used={bar_ts_used} intent_id={intent_id}",
        f"setup_note={note}",
        f"risk_reason={risk_reason}",
        f"risk open={open_now}/{open_max} trades_today={trades_today} cooldown={cooldown}",
        "mode=DRY_RUN",
    ]
    return _compact_or_verbose(compact, verbose, market_ctx)


def format_early_alert(early: Dict[str, Any], market_ctx: Dict[str, Any]) -> str:
    symbol = str(early.get("symbol", "?"))
    tf = str(market_ctx.get("tf", "5"))
    side = str(early.get("side", "?"))
    strategy = _strategy_display(early.get("strategy", "?"))
    conf = _fmt_float(early.get("confidence", 0.0), 2)
    bar_ts_15m = str(early.get("bar_ts_15m", "") or "")
    bar_ts_5m = str(early.get("bar_ts_5m", early.get("bar_ts_used", early.get("ts", ""))) or "")
    note = STRATEGY_HUMAN_NOTES.get(str(early.get("strategy", "")), "Early candidate")
    compact = [
        f"EARLY[{tf}m] {symbol} {side} conf={conf}",
        f"setup={note}",
        f"bar15={bar_ts_15m}",
        f"bar5={bar_ts_5m}",
    ]
    verbose = [
        f"⚠️ EARLY[{tf}m]",
        f"{symbol} {side} {strategy} conf={conf}",
        f"setup_note={note}",
        f"bar_ts_15m={bar_ts_15m}",
        f"bar_ts_5m={bar_ts_5m}",
        "mode=DRY_RUN",
    ]
    return _compact_or_verbose(compact, verbose, market_ctx)


def format_dashboard_compact(text: str, max_len: int = 1200) -> str:
    return _truncate_message(str(text or ""), max_len)
