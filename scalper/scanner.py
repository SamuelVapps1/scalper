<<<<<<< HEAD
import argparse
import logging
import time
import threading
from datetime import timezone, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

try:
    import config as _config  # shim/backcompat
except Exception:
    _config = None


_INTENT_FINGERPRINT_CACHE: set[str] = set()
_EARLY_ALERT_CACHE: set[str] = set()
_LAST_SCAN_SUMMARY_AT: float = 0.0
_SCANS_COMPLETED: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bybit Signal Bot (DRY RUN only, alerts and logging)."
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a Telegram test message and exit immediately.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one scan cycle and exit.",
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Run paper/shadow broker mode (no exchange private endpoints).",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help='Override symbols, e.g. "BTCUSDT,ETHUSDT,SOLUSDT".',
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run scan cycle forever (single process). Sleep SCAN_SECONDS between cycles. Ctrl+C for graceful shutdown. Keeps cache warm.",
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=30,
        help="Cooldown minutes per symbol+setup (default: 30).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO"],
        default="INFO",
        help="Log verbosity (default: INFO).",
    )
    parser.add_argument(
        "--force-intents",
        type=int,
        default=0,
        help="In --once mode, generate N synthetic intents for RiskAutopilot stress test.",
    )
    parser.add_argument(
        "--sizing-test",
        action="store_true",
        help="Run one paper sizing smoke test and exit.",
    )
    parser.add_argument(
        "--reconcile",
        type=str,
        default="",
        help="Print detailed candle/indicator/condition reconciliation for SYMBOL and exit.",
    )
    parser.add_argument(
        "--test-telegram-formats",
        action="store_true",
        help="Print sample Telegram formatter messages to stdout (no send).",
    )
    parser.add_argument(
        "--serve-dashboard",
        action="store_true",
        help="Start local DRY-RUN dashboard web server and exit scan loop.",
    )
    parser.add_argument(
        "--dryrun-notify-always",
        action="store_true",
        help="Force Telegram notifications for both ALLOW and BLOCK intents in DRY RUN.",
    )
    parser.add_argument(
        "--enable-scan-summary",
        action="store_true",
        help="Enable scan summary for this run (sets DISABLE_SCAN_SUMMARY=False, NOTIFY_SCAN_SUMMARY=True).",
    )
    parser.add_argument(
        "--validate-env",
        action="store_true",
        help="Load settings, validate required env, print OK and exit 0, or print missing keys and exit non-zero.",
    )
    return parser.parse_args()


def setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def _warn_missing_telegram_once() -> None:
    from scalper.notifier import warn_missing_telegram_once

    warn_missing_telegram_once()




def _send_telegram_with_logging(*, kind: str, token: str, chat_id: str, text: str, strict: bool = False) -> bool:
    from scalper.notifier import send_telegram_with_logging

    return send_telegram_with_logging(
        kind=kind,
        token=token,
        chat_id=chat_id,
        text=text,
        strict=strict,
    )

def _intent_fingerprint(symbol: str, strategy: str, side: str, bar_ts: str) -> str:
    return f"{symbol}|{strategy}|{side}|{bar_ts}"


def _fingerprint_group_key(symbol: str, strategy: str, side: str) -> str:
    return f"{symbol}|{strategy}|{side}"


def _apply_preview_gate(allowed: bool, gate_reason: str, preview: Dict[str, Any]) -> tuple[bool, str]:
    if not allowed:
        return (False, str(gate_reason or ""))
    if not preview or not bool(preview.get("ok")):
        return (False, str((preview or {}).get("reason") or "PREVIEW_BUILD_FAILED"))
    return (True, str(gate_reason or ""))


def _early_group_key(symbol: str, bar_ts_15m: str) -> str:
    return f"{symbol}|{bar_ts_15m}"


def _is_duplicate_early_alert(
    *,
    state: Dict[str, Any],
    early_key: str,
    symbol: str,
    bar_ts_15m: str,
    max_alerts: int,
) -> bool:
    global _EARLY_ALERT_CACHE
    if early_key in _EARLY_ALERT_CACHE:
        return True
    entries = list(state.get("early_alert_keys", []) or [])
    group = _early_group_key(symbol, bar_ts_15m)
    group_entries = [
        item
        for item in entries
        if isinstance(item, dict)
        and _early_group_key(
            str(item.get("symbol", "")),
            str(item.get("bar_ts_15m", "")),
        )
        == group
    ]
    # Enforce per-symbol-per-15m dedupe with configurable cap (default 1).
    if len(group_entries) >= max(1, int(max_alerts)):
        return True
    if any(str(item.get("key", "")) == early_key for item in group_entries):
        _EARLY_ALERT_CACHE.add(early_key)
        return True
    return False


def _remember_early_alert(
    *,
    state: Dict[str, Any],
    early_key: str,
    symbol: str,
    bar_ts_15m: str,
    bar_ts_5m: str,
    max_alerts: int,
) -> None:
    global _EARLY_ALERT_CACHE
    entries = list(state.get("early_alert_keys", []) or [])
    entries.append(
        {
            "key": early_key,
            "symbol": symbol,
            "bar_ts_15m": bar_ts_15m,
            "bar_ts_5m": bar_ts_5m,
        }
    )
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        if not key:
            continue
        g = _early_group_key(
            str(item.get("symbol", "")),
            str(item.get("bar_ts_15m", "")),
        )
        grouped.setdefault(g, []).append(item)
    compacted: List[Dict[str, Any]] = []
    for group_entries in grouped.values():
        compacted.extend(group_entries[-max(1, int(max_alerts)):])
    state["early_alert_keys"] = compacted[-1000:]
    _EARLY_ALERT_CACHE = {
        str(item.get("key", ""))
        for item in state["early_alert_keys"]
        if isinstance(item, dict) and str(item.get("key", "")).strip()
    }


def _is_duplicate_intent_fingerprint(
    *,
    state: Dict[str, Any],
    fingerprint: str,
    symbol: str,
    strategy: str,
    side: str,
    dedup_bars: int,
) -> bool:
    global _INTENT_FINGERPRINT_CACHE
    if fingerprint in _INTENT_FINGERPRINT_CACHE:
        return True

    entries = list(state.get("intent_fingerprints", []) or [])
    group = _fingerprint_group_key(symbol, strategy, side)
    recent_group_entries = [
        item
        for item in entries
        if isinstance(item, dict)
        and _fingerprint_group_key(
            str(item.get("symbol", "")),
            str(item.get("strategy", "")),
            str(item.get("side", "")),
        )
        == group
    ]
    recent_group_entries = recent_group_entries[-max(1, dedup_bars):]
    if any(str(item.get("fingerprint", "")) == fingerprint for item in recent_group_entries):
        _INTENT_FINGERPRINT_CACHE.add(fingerprint)
        return True
    return False


def _remember_intent_fingerprint(
    *,
    state: Dict[str, Any],
    fingerprint: str,
    symbol: str,
    strategy: str,
    side: str,
    bar_ts: str,
    dedup_bars: int,
) -> None:
    global _INTENT_FINGERPRINT_CACHE
    entries = list(state.get("intent_fingerprints", []) or [])
    entries.append(
        {
            "fingerprint": fingerprint,
            "symbol": symbol,
            "strategy": strategy,
            "side": side,
            "bar_ts": bar_ts,
        }
    )
    by_group: Dict[str, List[Dict[str, Any]]] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        fp = str(item.get("fingerprint", "")).strip()
        if not fp:
            continue
        g = _fingerprint_group_key(
            str(item.get("symbol", "")),
            str(item.get("strategy", "")),
            str(item.get("side", "")),
        )
        by_group.setdefault(g, []).append(item)
    compacted: List[Dict[str, Any]] = []
    for group_entries in by_group.values():
        compacted.extend(group_entries[-max(1, dedup_bars):])
    state["intent_fingerprints"] = compacted[-500:]
    _INTENT_FINGERPRINT_CACHE = {
        str(item.get("fingerprint", ""))
        for item in state["intent_fingerprints"]
        if isinstance(item, dict) and str(item.get("fingerprint", "")).strip()
    }


def run_sizing_test(config_module) -> int:
    from paper import open_paper_position

    intent = SimpleNamespace(
        symbol="BTCUSDT",
        side="LONG",
        strategy="SIZING_TEST",
        intent_id="SIZING_TEST_1",
    )
    position = open_paper_position(
        intent=intent,
        price=100.0,
        atr=2.0,  # with SL_ATR=1.0 -> SL distance = 2.0
        ts=datetime.now(timezone.utc).isoformat(),
        sl_atr_mult=config_module.PAPER_SL_ATR,
        tp_atr_mult=config_module.PAPER_TP_ATR,
        paper_equity_usdt=config_module.PAPER_EQUITY_USDT,
        risk_per_trade_pct=config_module.RISK_PER_TRADE_PCT,
        max_notional_usdt=config_module.MAX_NOTIONAL_USDT,
    )
    sl_pct = abs(position.entry_price - position.sl_price) / max(position.entry_price, 1e-10)
    risk_usdt = float(config_module.PAPER_EQUITY_USDT) * (float(config_module.RISK_PER_TRADE_PCT) / 100.0)
    logging.info(
        "SIZING_TEST entry=%.4f sl=%.4f sl_pct=%.6f risk_usdt=%.4f notional=%.4f qty=%.6f",
        position.entry_price,
        position.sl_price,
        sl_pct,
        risk_usdt,
        position.notional_usdt,
        position.qty_est,
    )
    return 0


def run_test_telegram_formats(config_module) -> int:
    import sys

    from telegram_format import (
        format_early_alert,
        format_intent_allow,
        format_intent_block,
        format_paper_close,
    )

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    compact_cap = int(getattr(config_module, "TELEGRAM_MAX_CHARS_COMPACT", 900))
    verbose_cap = int(getattr(config_module, "TELEGRAM_MAX_CHARS_VERBOSE", 2500))

    allow_intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "RANGE_BREAKOUT_RETEST_GO",
        "confidence": 0.74,
        "reason": "RB_RTG confirmed on closed bar",
        "intent_id": "BTCUSDT|RANGE_BREAKOUT_RETEST_GO|LONG|2026-01-01T12:00:00Z",
    }
    allow_risk = {"reason": "allowed"}
    allow_ctx = {
        "tf": "15",
        "entry": 65780.0,
        "sl": 65466.33,
        "tp": 66250.49,
        "sl_pct": 0.48,
        "tp_pct": 0.72,
        "qty": 0.00076,
        "notional": 50.0,
        "bar_ts_used": "2026-01-01T12:00:00+00:00",
        "open_now": 1,
        "open_max": 3,
        "trades_today": 4,
        "cooldown_until_utc": "",
        "telegram_max_chars_compact": compact_cap,
        "telegram_max_chars_verbose": verbose_cap,
    }

    block_intent = {
        "symbol": "ETHUSDT",
        "side": "SHORT",
        "strategy": "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
        "confidence": 0.62,
        "reason": "FB_FADE setup note",
    }
    block_risk = {"reason": "MAX_OPEN_POSITIONS_REACHED (5)"}
    block_ctx = {
        "tf": "15",
        "open_now": 5,
        "open_max": 5,
        "trades_today": 6,
        "cooldown_until_utc": "",
        "telegram_max_chars_compact": compact_cap,
        "telegram_max_chars_verbose": verbose_cap,
    }

    close_trade = {
        "symbol": "XRPUSDT",
        "side": "SHORT",
        "strategy": "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
        "pnl_usdt": 0.4503,
        "close_reason": "TP",
        "bars_held": 3,
    }
    close_state = {
        "tf": "15",
        "open_now": 2,
        "open_max": 5,
        "trades_today": 7,
        "daily_pnl_sim": 1.3432,
        "consec_losses": 0,
        "cooldown_until_utc": "",
        "telegram_max_chars_compact": compact_cap,
        "telegram_max_chars_verbose": verbose_cap,
    }

    early_intent = {
        "symbol": "SOLUSDT",
        "side": "SHORT",
        "strategy": "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
        "confidence": 0.41,
        "bar_ts_15m": "2026-01-01T12:00:00+00:00",
        "bar_ts_5m": "2026-01-01T12:10:00+00:00",
    }
    early_ctx = {
        "tf": "5",
        "telegram_max_chars_compact": compact_cap,
        "telegram_max_chars_verbose": verbose_cap,
    }

    for fmt in ("compact", "verbose"):
        print(f"--- {fmt.upper()} ---")
        allow_ctx["telegram_format"] = fmt
        block_ctx["telegram_format"] = fmt
        close_state["telegram_format"] = fmt
        early_ctx["telegram_format"] = fmt
        print(format_intent_allow(allow_intent, allow_risk, allow_ctx))
        print("")
        print(format_intent_block(block_intent, block_risk, block_ctx))
        print("")
        print(format_paper_close(close_trade, close_state))
        print("")
        print(format_early_alert(early_intent, early_ctx))
        print("")
    return 0


def run_reconcile(config_module, symbol: str) -> int:
    from scalper.bybit import fetch_klines
    from scalper.signals import build_reconcile_report

    clean_symbol = str(symbol or "").strip().upper()
    if not clean_symbol:
        logging.error("--reconcile requires a symbol, e.g. --reconcile BTCUSDT")
        return 2

    candles = fetch_klines(
        symbol=clean_symbol,
        interval=str(config_module.INTERVAL),
        limit=int(config_module.LOOKBACK),
    )
    candles_5m = fetch_klines(
        symbol=clean_symbol,
        interval=str(getattr(config_module, "EARLY_TF", "5")),
        limit=int(getattr(config_module, "EARLY_LOOKBACK_5M", 180)),
    )
    report = build_reconcile_report(
        clean_symbol,
        candles,
        candles_5m=candles_5m,
        threshold_profile=str(getattr(config_module, "THRESHOLD_PROFILE", "A")),
    )
    logging.info("\n%s", report)
    return 0


def _parse_symbols_override(raw: str) -> list[str]:
    txt = str(raw or "").strip()
    if not txt:
        return []
    return [p.strip().upper() for p in txt.split(",") if p.strip()]


def resolve_watchlist(config_module, symbols_override: Optional[list[str]] = None) -> tuple[list, str]:
    """Resolve watchlist via watchlist.get_watchlist (static/dynamic/topn)."""
    if symbols_override:
        return (list(dict.fromkeys(symbols_override)), "cli")
    from scalper.watchlist import get_watchlist

    return get_watchlist(config_module, bybit_client=None, logger=logging.getLogger(__name__))


def run_scan_cycle(
    watchlist,
    watchlist_mode: str,
    interval: str,
    lookback: int,
    telegram_token: str,
    telegram_chat_id: str,
    risk_autopilot,
    notify_blocked_telegram: bool,
    always_notify_intents: bool,
    signal_debug: bool,
    early_enabled: bool,
    early_tf: str,
    early_lookback_5m: int,
    early_min_conf: float,
    early_require_15m_context: bool,
    early_max_alerts_per_symbol_per_15m: int,
    telegram_early_enabled: bool,
    telegram_early_max_per_symbol_per_15m: int,
    threshold_profile: str,
    telegram_format: str,
    telegram_compact: bool,
    telegram_max_chars_compact: int,
    telegram_max_chars_verbose: int,
    paper_mode: bool = False,
) -> Dict[str, Any]:
    from scalper.bybit import fetch_klines
    from scalper.paper import PaperPosition, update_and_maybe_close
    from scalper.paper_engine import try_open_position
    from scalper.models import TradeRecord
    from scalper.risk_engine_core import RiskEngine
    from scalper.paper_broker import PaperBroker
    from scalper.settings import get_settings
    from scalper.signals import (
        evaluate_early_intents_from_5m,
        evaluate_higher_tf_context,
        evaluate_symbol_intents,
    )
    from scalper.strategy_engine import StrategyEngine
    import scalper.storage as state_store
    from scalper.storage import (
        append_signal,
        load_paper_state,
        save_paper_state,
        set_defer_position_sync,
        set_last_block_reason,
        set_last_bias_json,
        set_near_misses,
        set_symbols_v3,
        store_trade_intent,
        sync_paper_positions_from_state,
        upsert_paper_position,
        insert_paper_trade,
        delete_paper_position,
    )
    from scalper.telegram_format import (
        format_early_alert,
        format_intent_allow,
        format_intent_block,
        format_paper_open,
        format_paper_close,
    )
    from scalper.trade_preview import build_trade_preview

    run_context: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": interval,
        "watchlist_mode": watchlist_mode,
        "watchlist": list(watchlist),
        "symbols": [],
        "symbols_v3": {},
        "mtf_snapshots": {},
    }
    def _cfg(name: str, default: Any) -> Any:
        return getattr(_config, name, default) if _config is not None else default

    v3_params_dict = {
        "DONCHIAN_N_15M": _cfg("DONCHIAN_N_15M", 20),
        "BODY_ATR_15M": _cfg("BODY_ATR_15M", 0.25),
        "TREND_SEP_ATR_1H": _cfg("TREND_SEP_ATR_1H", 0.8),
        "USE_5M_CONFIRM": _cfg("USE_5M_CONFIRM", True),
    }
    format_caps = {
        "telegram_format": "compact" if telegram_compact else telegram_format,
        "telegram_max_chars_compact": int(telegram_max_chars_compact),
        "telegram_max_chars_verbose": int(telegram_max_chars_verbose),
    }
    watchlist_set = set(watchlist)
    klines_5m_cache: Dict[str, List[Dict[str, float]]] = {}
    initial_state = load_paper_state()
    open_position_symbols = [
        str(pos.get("symbol", "")).upper()
        for pos in (initial_state.get("open_positions", []) or [])
        if isinstance(pos, dict) and str(pos.get("symbol", "")).strip()
    ]
    symbols_to_process = list(dict.fromkeys(list(watchlist) + open_position_symbols))

    from mtf import build_mtf_snapshot, compute_4h_bias, log_mtf_ready

    snapshot_by_symbol: Dict[str, Dict[int, Dict[str, Any]]] = {}
    strategy_engine = StrategyEngine()
    settings_obj = get_settings()
    risk_engine = RiskEngine(load_paper_state(), settings_obj.risk, state_store)
    paper_broker = PaperBroker(
        store=SimpleNamespace(
            load_paper_state=load_paper_state,
            upsert_paper_position=upsert_paper_position,
            insert_paper_trade=insert_paper_trade,
            delete_paper_position=delete_paper_position,
        ),
        risk_settings=settings_obj.risk,
    ) if paper_mode else None
    strategy_v1_enabled = bool(settings_obj.strategy_v3.strategy_v1)
    tf_bias = int(getattr(_config, "TF_BIAS", 240))
    bias_list: List[Dict[str, Any]] = []
    has_any_allow = False
    all_near_miss_candidates: List[Dict[str, Any]] = []

    set_defer_position_sync(True)
    for symbol in symbols_to_process:
        symbol_context: Dict[str, Any] = {
            "symbol": symbol,
            "market_snapshot": {},
            "mtf_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "debug_why_none": {},
            "error": None,
        }
        try:
            snap_result = build_mtf_snapshot(symbol)
            mtf_snap = snap_result[0] if isinstance(snap_result, tuple) else snap_result
            snapshot_by_symbol[symbol] = mtf_snap
            symbol_context["mtf_snapshot"] = mtf_snap
            run_context["mtf_snapshots"][symbol] = mtf_snap
            if mtf_snap:
                log_mtf_ready(symbol, mtf_snap, logger=logging.getLogger(__name__))

            snap_4h = (mtf_snap or {}).get(tf_bias, {}) or {}
            bias_info = compute_4h_bias(symbol, snap_4h)
            bias_list.append(dict(bias_info))

            # DRY RUN only: public market data endpoint.
            candles = fetch_klines(symbol=symbol, interval=interval, limit=lookback)
            if candles:
                state = load_paper_state()
                open_positions = list(state.get("open_positions", []) or [])
                updated_open_positions: List[Dict[str, Any]] = []
                closed_trades = list(state.get("closed_trades", []) or [])
                latest_candle = candles[-1]
                for pos_raw in open_positions:
                    if str(pos_raw.get("symbol", "")) != symbol:
                        updated_open_positions.append(pos_raw)
                        continue
                    try:
                        position = PaperPosition.from_dict(pos_raw)
                    except Exception:
                        continue
                    updated, closed, pnl_usdt, close_reason, partial_trade = update_and_maybe_close(
                        position=position,
                        last_candle=latest_candle,
                        fees_bps=getattr(risk_autopilot, "paper_fees_bps", 6.0),
                        timeout_bars=getattr(risk_autopilot, "paper_timeout_bars", 12),
                    )
                    if partial_trade:
                        risk_engine.on_fill(
                            TradeRecord(
                                symbol=str(partial_trade.get("symbol", position.symbol) or ""),
                                side=str(partial_trade.get("side", position.side) or ""),
                                setup=str(partial_trade.get("strategy", position.strategy) or ""),
                                entry_ts=str(partial_trade.get("entry_ts", "") or ""),
                                close_ts=str(partial_trade.get("close_ts", updated.last_ts) or ""),
                                pnl_usdt=float(partial_trade.get("pnl_usdt", 0) or 0.0),
                                close_reason=str(partial_trade.get("close_reason", "partial") or "partial"),
                            )
                        )
                        closed_trades.append(partial_trade)
                        if paper_broker is not None:
                            partial_trade["position_id"] = str(updated.intent_id or "")
                            paper_broker.persist_close(partial_trade)
                        position = updated
                        # Update open_positions with reduced qty
                        updated_pos = updated.to_dict()
                        updated_open_positions.append(updated_pos)
                        if paper_broker is not None:
                            paper_broker.persist_open(updated_pos)
                        continue
                    if closed:
                        risk_engine.on_fill(
                            TradeRecord(
                                symbol=updated.symbol,
                                side=updated.side,
                                setup=updated.strategy,
                                entry_ts=str(updated.entry_ts or ""),
                                close_ts=str(updated.last_ts or ""),
                                pnl_usdt=float(pnl_usdt),
                                close_reason=str(close_reason or ""),
                            )
                        )
                        risk_state_after = load_paper_state()
                        close_event = {
                            "symbol": updated.symbol,
                            "side": updated.side,
                            "strategy": updated.strategy,
                            "intent_id": updated.intent_id,
                            "entry_ts": updated.entry_ts,
                            "close_ts": updated.last_ts,
                            "bars_held": updated.bars_held,
                            "pnl_usdt": float(pnl_usdt),
                            "close_reason": close_reason,
                            "status": "CLOSED",
                        }
                        closed_trades.append(close_event)
                        if paper_broker is not None:
                            close_event["position_id"] = str(updated.intent_id or "")
                            paper_broker.persist_close(close_event)
                        logging.info(
                            "PAPER CLOSE %s %s %s pnl=%.4f reason=%s",
                            updated.symbol,
                            updated.side,
                            updated.strategy,
                            float(pnl_usdt),
                            close_reason,
                        )
                        logging.info(
                            "risk_after_close: daily_pnl_sim=%.4f consec_losses=%d cooldown_until=%s",
                            float(risk_state_after.get("daily_pnl_sim", 0.0)),
                            int(risk_state_after.get("consecutive_losses", 0)),
                            str(risk_state_after.get("cooldown_until_utc", "") or ""),
                        )
                        if telegram_token and telegram_chat_id:
                            close_msg = format_paper_close(
                                close_event,
                                {
                                    "tf": str(interval),
                                    "open_positions": risk_state_after.get("open_positions", []) or [],
                                    "open_max": int(getattr(risk_autopilot, "max_open_positions", 0) or 0),
                                    "trade_count_today": int(risk_state_after.get("trade_count_today", 0) or 0),
                                    "daily_pnl_sim": float(risk_state_after.get("daily_pnl_sim", 0.0) or 0.0),
                                    "consec_losses": int(risk_state_after.get("consecutive_losses", 0) or 0),
                                    "cooldown_until_utc": str(
                                        risk_state_after.get("cooldown_until_utc", "") or ""
                                    ),
                                    **format_caps,
                                },
                            )
                            _send_telegram_with_logging(
                                kind="intent",
                                token=telegram_token,
                                chat_id=telegram_chat_id,
                                text=close_msg,
                            )
                    else:
                        updated_pos = updated.to_dict()
                        updated_open_positions.append(updated_pos)
                        if paper_broker is not None:
                            paper_broker.persist_open(updated_pos)
                # Reload to preserve risk updates written by RiskAutopilot during closes.
                merged_state = load_paper_state()
                merged_state["open_positions"] = updated_open_positions
                merged_state["closed_trades"] = closed_trades[-50:]
                save_paper_state(merged_state)

            if symbol in watchlist_set:
                active_profile = str(threshold_profile or "A").strip().upper()
                if strategy_engine.has_enabled and (bias_info.get("bias") or "NONE") in ("LONG", "SHORT"):
                    if symbol not in klines_5m_cache:
                        klines_5m_cache[symbol] = fetch_klines(
                            symbol=symbol,
                            interval=str(getattr(_config, "TF_TIMING", 5)),
                            limit=int(getattr(_config, "LOOKBACK_5M", 400)),
                        ) or []
                    snap_15m = (mtf_snap or {}).get(15, {}) or {}
                    bar_ts_v2 = str(
                        snap_15m.get("ts", "")
                        or (candles[-2].get("timestamp_utc", "") if len(candles) >= 2 else "")
                    )
                    evaluated, plugin_result = strategy_engine.evaluate_symbol(
                        symbol=symbol,
                        candles_15m=candles or [],
                        candles_5m=klines_5m_cache.get(symbol) or [],
                        mtf_snapshot=mtf_snap or {},
                        bias_info=bias_info,
                        signal_debug=signal_debug,
                        interval=str(interval),
                        bar_ts_used=bar_ts_v2,
                        v3_params=v3_params_dict,
                        sl_atr_mult=float(getattr(_config, "PULLBACK_SL_ATR_MULT", 0.60)),
                        tp_r=float(getattr(_config, "PAPER_TP_ATR", 1.5)),
                    )
                    if plugin_result.debug and isinstance(plugin_result.debug, dict):
                        if plugin_result.debug.get("close_15m") is not None or plugin_result.breakout_level is not None:
                            run_context["symbols_v3"][symbol] = {
                                "v3": {
                                    "ok": bool(plugin_result.ok),
                                    "side": plugin_result.side,
                                    "reason": plugin_result.reason,
                                    "breakout_level": plugin_result.breakout_level,
                                }
                            }
                    for nm in evaluated.get("near_miss_candidates", []) or []:
                        all_near_miss_candidates.append(dict(nm))
                else:
                    candles_1h = (run_context.get("candles_1h") or {}).get(symbol)
                    if candles_1h is None and getattr(_config, "LOOKBACK_1H", 0):
                        try:
                            candles_1h = fetch_klines(
                                symbol=symbol,
                                interval="60",
                                limit=int(getattr(_config, "LOOKBACK_1H", 100)),
                            )
                        except Exception:
                            candles_1h = []
                        run_context.setdefault("candles_1h", {})[symbol] = candles_1h or []
                    higher_tf_context = (
                        evaluate_higher_tf_context(symbol, candles_1h=candles_1h or [])
                        if (candles_1h and len(candles_1h) >= 50)
                        else None
                    )
                    evaluated = evaluate_symbol_intents(
                        symbol=symbol,
                        candles=candles,
                        signal_debug=signal_debug,
                        early_min_conf=early_min_conf,
                        threshold_profile=active_profile,
                        higher_tf_context=higher_tf_context,
                    )
                symbol_context["market_snapshot"] = dict(evaluated.get("market_snapshot", {}) or {})
                symbol_context["candidates_before"] = list(
                    evaluated.get("candidates_before", []) or []
                )
                symbol_context["early_intents"] = list(evaluated.get("early_intents", []) or [])
                symbol_context["collisions"] = list(evaluated.get("collisions", []) or [])
                symbol_context["rejections"] = list(evaluated.get("rejections", []) or [])
                symbol_context["debug_why_none"] = dict(evaluated.get("debug_why_none", {}) or {})
                symbol_context["error"] = evaluated.get("error")
                symbol_context["threshold_profile"] = active_profile

                if signal_debug:
                    profile_results: Dict[str, str] = {}
                    for prof in ("A", "B", "C"):
                        if prof == active_profile:
                            ev_prof = evaluated
                        else:
                            ev_prof = evaluate_symbol_intents(
                                symbol=symbol,
                                candles=candles,
                                signal_debug=True,
                                early_min_conf=early_min_conf,
                                threshold_profile=prof,
                                higher_tf_context=higher_tf_context,
                            )
                        finals = list(ev_prof.get("final_intents", []) or [])
                        if finals:
                            profile_results[prof] = (
                                f"TRIGGER_{str(finals[0].get('side', '')).upper()}"
                            )
                        else:
                            dbg = dict(ev_prof.get("debug_why_none", {}) or {})
                            profile_results[prof] = str(
                                dbg.get("FB_FADE") or dbg.get("RB_RTG") or "NO_TRIGGER"
                            )
                    logging.info(
                        "SHADOW symbol=%s active=%s A=%s B=%s C=%s",
                        symbol,
                        active_profile,
                        profile_results.get("A", "NO_TRIGGER"),
                        profile_results.get("B", "NO_TRIGGER"),
                        profile_results.get("C", "NO_TRIGGER"),
                    )

                # EARLY pre-alert from 5m bridge: notify only, never opens paper positions.
                symbol_context["early_intents"] = []
                if early_enabled:
                    candidates_15m = list(symbol_context.get("candidates_before", []) or [])
                    has_15m_ctx = bool(
                        [c for c in candidates_15m if str(c.get("strategy", "")) in {
                            "RANGE_BREAKOUT_RETEST_GO",
                            "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                        }]
                    )
                    if (not early_require_15m_context) or has_15m_ctx:
                        if symbol not in klines_5m_cache:
                            klines_5m_cache[symbol] = fetch_klines(
                                symbol=symbol,
                                interval=str(early_tf),
                                limit=int(max(30, early_lookback_5m)),
                            )
                        early_intents = evaluate_early_intents_from_5m(
                            symbol=symbol,
                            candles_5m=klines_5m_cache.get(symbol, []) or [],
                            context_15m=symbol_context,
                            early_min_conf=early_min_conf,
                            require_15m_context=early_require_15m_context,
                        )
                        symbol_context["early_intents"] = list(early_intents)

                        for early_signal in symbol_context["early_intents"]:
                            if not telegram_early_enabled:
                                continue
                            bar_ts_15m = str(
                                early_signal.get(
                                    "bar_ts_15m",
                                    symbol_context.get("market_snapshot", {}).get(
                                        "bar_ts_used",
                                        symbol_context.get("market_snapshot", {}).get("ts", ""),
                                    ),
                                )
                            )
                            bar_ts_5m = str(
                                early_signal.get(
                                    "bar_ts_5m",
                                    early_signal.get(
                                        "bar_ts_used",
                                        early_signal.get("ts", datetime.now(timezone.utc).isoformat()),
                                    ),
                                )
                            )
                            early_key = _intent_fingerprint(
                                symbol=str(early_signal.get("symbol", symbol)),
                                strategy=str(early_signal.get("strategy", early_signal.get("setup", ""))),
                                side=str(early_signal.get("side", early_signal.get("direction", ""))),
                                bar_ts=bar_ts_15m,
                            )
                            early_state = load_paper_state()
                            if _is_duplicate_early_alert(
                                state=early_state,
                                early_key=early_key,
                                symbol=str(early_signal.get("symbol", symbol)),
                                bar_ts_15m=bar_ts_15m,
                                max_alerts=telegram_early_max_per_symbol_per_15m,
                            ):
                                continue
                            _remember_early_alert(
                                state=early_state,
                                early_key=early_key,
                                symbol=str(early_signal.get("symbol", symbol)),
                                bar_ts_15m=bar_ts_15m,
                                bar_ts_5m=bar_ts_5m,
                                max_alerts=telegram_early_max_per_symbol_per_15m,
                            )
                            save_paper_state(early_state)
                            if telegram_early_enabled and telegram_token and telegram_chat_id:
                                _send_telegram_with_logging(
                                    kind="intent",
                                    token=telegram_token,
                                    chat_id=telegram_chat_id,
                                    text=format_early_alert(
                                        {
                                            "symbol": str(early_signal.get("symbol", symbol)),
                                            "side": str(
                                                early_signal.get("side", early_signal.get("direction", ""))
                                            ),
                                            "strategy": str(
                                                early_signal.get("strategy", early_signal.get("setup", ""))
                                            ),
                                            "confidence": float(early_signal.get("confidence", 0.0)),
                                            "bar_ts_15m": bar_ts_15m,
                                            "bar_ts_5m": bar_ts_5m,
                                        },
                                        {"tf": str(early_tf), **format_caps},
                                    ),
                                )
                            else:
                                _warn_missing_telegram_once()

                detected = list(evaluated.get("final_intents", []) or [])
                for signal in detected:
                    append_signal(signal)
                    intent_symbol = symbol
                    intent_side = str(signal.get("side", signal.get("direction", "")))
                    intent_strategy = str(signal.get("strategy", signal.get("setup", "")))
                    intent_reason = str(signal.get("reason", ""))
                    intent_confidence = float(signal.get("confidence", 0.0))
                    intent_ts = str(signal.get("ts", signal.get("timestamp_utc", datetime.now(timezone.utc).isoformat())))
                    bar_ts_used = str(
                        signal.get(
                            "bar_ts_used",
                            signal.get(
                                "timestamp_utc",
                                signal.get(
                                    "ts",
                                    symbol_context.get("market_snapshot", {}).get(
                                        "bar_ts_used",
                                        symbol_context.get("market_snapshot", {}).get("ts", ""),
                                    ),
                                ),
                            ),
                        )
                    )
                    bar_ts = str(
                        signal.get(
                            "timestamp_utc",
                            signal.get(
                                "ts",
                                symbol_context.get("market_snapshot", {}).get(
                                    "ts",
                                    datetime.now(timezone.utc).isoformat(),
                                ),
                            ),
                        )
                    )
                    dedup_bars = max(1, int(getattr(risk_autopilot, "dedup_bars", 2)))
                    fp = _intent_fingerprint(
                        intent_symbol,
                        intent_strategy,
                        intent_side,
                        bar_ts,
                    )
                    dedup_state = load_paper_state()
                    if _is_duplicate_intent_fingerprint(
                        state=dedup_state,
                        fingerprint=fp,
                        symbol=intent_symbol,
                        strategy=intent_strategy,
                        side=intent_side,
                        dedup_bars=dedup_bars,
                    ):
                        gate_reason = "DUPLICATE_INTENT"
                        symbol_context["final_intents"].append(
                            {
                                "symbol": intent_symbol,
                                "side": intent_side,
                                "strategy": intent_strategy,
                                "reason": intent_reason,
                                "confidence": intent_confidence,
                                "ts": intent_ts,
                                "bar_ts_used": bar_ts_used,
                                "intent_id": str(signal.get("intent_id", "")),
                                "risk": {"allowed": False, "reason": gate_reason},
                            }
                        )
                        logging.debug(
                            "Risk gate blocked intent (%s | %s | bar_ts_used=%s): %s",
                            intent_symbol,
                            intent_strategy,
                            bar_ts_used,
                            gate_reason,
                        )
                        set_last_block_reason(gate_reason)
                        dup_trade_intent = {
                            "id": str(signal.get("intent_id") or fp),
                            "ts": intent_ts,
                            "symbol": intent_symbol,
                            "setup": intent_strategy,
                            "direction": intent_side,
                            "timeframe": str(interval),
                            "entry_type": str(signal.get("entry_type", "MARKET_SIM") or "MARKET_SIM"),
                            "invalid_level": signal.get("invalid_level", ""),
                            "sl_hint": signal.get("sl_hint", ""),
                            "tp_hint": signal.get("tp_hint", ""),
                            "status": "BLOCKED",
                            "risk_verdict": "BLOCK",
                            "block_reason": gate_reason,
                        }
                        store_trade_intent(dup_trade_intent)
                        logging.debug(
                            "INTENT %s %s %s verdict=%s reason=%s",
                            str(dup_trade_intent.get("symbol", "")),
                            str(dup_trade_intent.get("setup", "")),
                            str(dup_trade_intent.get("direction", "")),
                            str(dup_trade_intent.get("risk_verdict", "")),
                            str(dup_trade_intent.get("block_reason", "")),
                        )
                        continue

                    state_for_risk = load_paper_state()
                    risk_snapshot = {
                        "equity": float(getattr(risk_autopilot, "paper_equity_usdt", 0.0) or 0.0)
                        + float(state_for_risk.get("daily_pnl_realized", state_for_risk.get("daily_pnl_sim", 0.0)) or 0.0),
                        "open_positions": list(state_for_risk.get("open_positions", []) or []),
                        "open_positions_count": len(
                            [
                                p
                                for p in (state_for_risk.get("open_positions", []) or [])
                                if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN"
                            ]
                        ),
                    }
                    verdict = risk_engine.evaluate(
                        {
                            "symbol": intent_symbol,
                            "setup": intent_strategy,
                            "direction": intent_side,
                            "timeframe": str(interval),
                            "candle_ts": bar_ts_used,
                            "ts": intent_ts,
                            "confidence": float(intent_confidence) if intent_confidence is not None else 0.0,
                        },
                        snapshot=risk_snapshot,
                    )
                    allowed = bool(verdict.allowed)
                    gate_reason = str(verdict.reason or "")
                    meta = dict(signal.get("meta") or {})
                    if "retest_level" not in meta and "failed_level" not in meta:
                        meta["retest_level"] = signal.get("level_ref")
                    snap = symbol_context.get("market_snapshot", {}) or {}
                    preview: Dict[str, Any] = {}
                    execution_status = "not_opened"
                    if allowed:
                        preview = build_trade_preview(
                            signal={
                                **dict(signal or {}),
                                "symbol": intent_symbol,
                                "side": intent_side,
                                "strategy": intent_strategy,
                                "confidence": float(intent_confidence),
                                "bar_ts_used": bar_ts_used,
                            },
                            market_snapshot=snap,
                            candles=candles,
                            mtf_snapshot=mtf_snap,
                            risk_settings=settings_obj.risk,
                            equity_usdt=(
                                paper_broker.current_equity()
                                if paper_broker is not None
                                else float(getattr(risk_autopilot, "paper_position_usdt", 20.0) or 20.0)
                            ),
                            for_execution=True,
                        )
                        allowed, gate_reason = _apply_preview_gate(allowed, gate_reason, preview)
                        if not allowed:
                            logging.warning(
                                "ALLOW downgraded to BLOCK %s %s %s reason=%s",
                                intent_symbol,
                                intent_strategy,
                                intent_side,
                                gate_reason,
                            )
                        else:
                            logging.info(
                                "PREVIEW built %s %s %s entry=%.8f sl=%.8f tp=%.8f atr_source=%s",
                                intent_symbol,
                                intent_strategy,
                                intent_side,
                                float(preview.get("entry", 0.0) or 0.0),
                                float(preview.get("sl", 0.0) or 0.0),
                                float(preview.get("tp", 0.0) or 0.0),
                                str(preview.get("atr_source", "")),
                            )
                    trade_intent = {
                        "id": str(signal.get("intent_id") or fp),
                        "ts": intent_ts,
                        "symbol": intent_symbol,
                        "setup": intent_strategy,
                        "direction": intent_side,
                        "timeframe": str(interval),
                        "entry_type": str(signal.get("entry_type", "MARKET_SIM") or "MARKET_SIM"),
                        "meta": meta,
                        "level_ref": signal.get("level_ref"),
                        "invalid_level": signal.get("invalid_level", ""),
                        "sl_hint": signal.get("sl_hint", ""),
                        "tp_hint": signal.get("tp_hint", ""),
                        "status": "OPEN" if allowed else "BLOCKED",
                        "risk_verdict": "ALLOW" if allowed else "BLOCK",
                        "block_reason": "" if allowed else gate_reason,
                    }
                    store_trade_intent(trade_intent)
                    logging.info(
                        "INTENT %s %s %s verdict=%s reason=%s",
                        str(trade_intent.get("symbol", "")),
                        str(trade_intent.get("setup", "")),
                        str(trade_intent.get("direction", "")),
                        str(trade_intent.get("risk_verdict", "")),
                        str(trade_intent.get("block_reason", "")),
                    )
                    final_intent_record = {
                        "symbol": intent_symbol,
                        "side": intent_side,
                        "strategy": intent_strategy,
                        "reason": intent_reason,
                        "confidence": intent_confidence,
                        "ts": intent_ts,
                        "bar_ts_used": bar_ts_used,
                        "intent_id": str(signal.get("intent_id", "")),
                        "risk": {"allowed": allowed, "reason": gate_reason},
                    }
                    symbol_context["final_intents"].append(final_intent_record)
                    _remember_intent_fingerprint(
                        state=dedup_state,
                        fingerprint=fp,
                        symbol=intent_symbol,
                        strategy=intent_strategy,
                        side=intent_side,
                        bar_ts=bar_ts,
                        dedup_bars=dedup_bars,
                    )
                    latest_state = load_paper_state()
                    latest_state["intent_fingerprints"] = list(dedup_state.get("intent_fingerprints", []) or [])
                    save_paper_state(latest_state)
                    if allowed:
                        has_any_allow = True
                        opened_position = None
                        if paper_broker is not None:
                            pos_dict, skip_reason = paper_broker.open_from_preview(
                                preview=preview,
                                intent_id=str(signal.get("intent_id", "")),
                                ts=str((candles[-1] if candles else {}).get("timestamp_utc", intent_ts)),
                                strategy=intent_strategy,
                            )
                        else:
                            pos_dict, skip_reason = try_open_position(
                                trade_intent,
                                candles,
                                snap,
                                paper_position_usdt=getattr(
                                    risk_autopilot, "paper_position_usdt", 20.0
                                ),
                                sl_atr_mult=getattr(risk_autopilot, "paper_sl_atr", 1.0),
                                tp_atr_mult=getattr(risk_autopilot, "paper_tp_atr", 1.5),
                                intent_id=str(signal.get("intent_id", "")),
                                preview=preview,
                            )
                        if pos_dict:
                            execution_status = "open"
                            opened_position = PaperPosition.from_dict(pos_dict)
                            state = load_paper_state()
                            open_positions = list(
                                state.get("open_positions", []) or []
                            )
                            open_positions.append(pos_dict)
                            state["open_positions"] = open_positions
                            save_paper_state(state)
                            if paper_broker is not None:
                                paper_broker.persist_open(pos_dict)
                            sl_pct = abs(
                                opened_position.entry_price
                                - opened_position.sl_price
                            ) / max(opened_position.entry_price, 1e-10)
                            logging.info(
                                "PAPER OPEN %s %s %s bar_ts_used=%s notional=%.4f qty=%.6f sl_pct=%.6f",
                                opened_position.symbol,
                                opened_position.side,
                                opened_position.strategy,
                                bar_ts_used,
                                opened_position.notional_usdt,
                                opened_position.qty_est,
                                sl_pct,
                            )
                            if telegram_token and telegram_chat_id:
                                state_after_open = load_paper_state()
                                open_msg = format_paper_open(
                                    {
                                        "symbol": opened_position.symbol,
                                        "side": opened_position.side,
                                        "strategy": opened_position.strategy,
                                        "confidence": float(intent_confidence),
                                        "entry": opened_position.entry_price,
                                        "sl": float(preview.get("sl", opened_position.sl_price)),
                                        "tp": float(preview.get("tp", opened_position.tp_price)),
                                        "sl_pct": float(preview.get("sl_pct", 0.0) or 0.0),
                                        "tp_pct": float(preview.get("tp_pct", 0.0) or 0.0),
                                        "qty": opened_position.qty_est,
                                        "notional": opened_position.notional_usdt,
                                        "bar_ts_used": bar_ts_used,
                                        "intent_id": str(
                                            signal.get("intent_id", "")
                                        ),
                                        "risk_reason": gate_reason,
                                        "note": intent_reason,
                                        "preview_status": str(preview.get("reason", "")),
                                        "execution_status": execution_status,
                                    },
                                    {
                                        "tf": str(interval),
                                        "open_now": len(
                                            state_after_open.get(
                                                "open_positions", []
                                            )
                                            or []
                                        ),
                                        "open_max": int(
                                            getattr(
                                                risk_autopilot,
                                                "max_open_positions",
                                                0,
                                            )
                                            or 0
                                        ),
                                        "trades_today": int(
                                            state_after_open.get(
                                                "trade_count_today", 0
                                            )
                                            or 0
                                        ),
                                        "cooldown_until_utc": str(
                                            state_after_open.get(
                                                "cooldown_until_utc", ""
                                            )
                                            or ""
                                        ),
                                        **format_caps,
                                    },
                                )
                                _send_telegram_with_logging(
                                    kind="intent",
                                    token=telegram_token,
                                    chat_id=telegram_chat_id,
                                    text=open_msg,
                                )
                            else:
                                _warn_missing_telegram_once()
                        else:
                            logging.warning(
                                "Paper open skipped %s %s %s reason=%s",
                                intent_symbol,
                                intent_strategy,
                                intent_side,
                                str(skip_reason or "UNKNOWN"),
                            )
                        if telegram_token and telegram_chat_id:
                            msg = format_intent_allow(
                                {
                                    "symbol": intent_symbol,
                                    "side": intent_side,
                                    "strategy": intent_strategy,
                                    "confidence": float(intent_confidence),
                                    "reason": intent_reason,
                                    "intent_id": str(
                                        signal.get("intent_id", "")
                                    ),
                                },
                                {"reason": gate_reason},
                                {
                                    "tf": str(interval),
                                    "entry": float(preview.get("entry", 0.0) or 0.0),
                                    "sl": float(preview.get("sl", 0.0) or 0.0),
                                    "tp": float(preview.get("tp", 0.0) or 0.0),
                                    "sl_pct": float(preview.get("sl_pct", 0.0) or 0.0),
                                    "tp_pct": float(preview.get("tp_pct", 0.0) or 0.0),
                                    "qty": float(preview.get("qty", 0.0) or 0.0),
                                    "notional": float(preview.get("notional", 0.0) or 0.0),
                                    "preview_status": "ok",
                                    "execution_status": execution_status,
                                    "atr_source": str(preview.get("atr_source", "")),
                                    "bar_ts_used": bar_ts_used,
                                    "bias": str(signal.get("bias", "") or ""),
                                    "break_level": signal.get("break_level"),
                                    "retest_level": signal.get("retest_level"),
                                    "open_now": len(
                                        load_paper_state().get(
                                            "open_positions", []
                                        )
                                        or []
                                    ),
                                    "open_max": int(
                                        getattr(
                                            risk_autopilot,
                                            "max_open_positions",
                                            0,
                                        )
                                        or 0
                                    ),
                                    "trades_today": int(
                                        load_paper_state().get(
                                            "trade_count_today", 0
                                        )
                                        or 0
                                    ),
                                    "cooldown_until_utc": str(
                                        load_paper_state().get(
                                            "cooldown_until_utc", ""
                                        )
                                        or ""
                                    ),
                                    **format_caps,
                                },
                            )
                            if always_notify_intents:
                                msg = f"[ALLOW] {msg}"
                            _send_telegram_with_logging(
                                kind="intent",
                                token=telegram_token,
                                chat_id=telegram_chat_id,
                                text=msg,
                            )
                        else:
                            _warn_missing_telegram_once()
                    else:
                        logging.warning(
                            "Risk gate blocked intent (%s | %s | bar_ts_used=%s): %s",
                            intent_symbol,
                            intent_strategy,
                            bar_ts_used,
                            gate_reason,
                        )
                        set_last_block_reason(gate_reason)
                        _skip_block_telegram = (
                            gate_reason == "DUPLICATE_INTENT"
                            or "cooldown" in str(gate_reason or "").lower()
                        )
                        should_notify_block = bool(
                            (notify_blocked_telegram or always_notify_intents)
                            and not _skip_block_telegram
                        )
                        if should_notify_block and telegram_token and telegram_chat_id:
                            state_now = load_paper_state()
                            blocked_msg = format_intent_block(
                                {
                                    "symbol": intent_symbol,
                                    "side": intent_side,
                                    "strategy": intent_strategy,
                                    "confidence": float(intent_confidence),
                                    "reason": intent_reason,
                                },
                                {"reason": gate_reason},
                                {
                                    "tf": str(interval),
                                    "entry": None,
                                    "sl": None,
                                    "tp": None,
                                    "sl_pct": None,
                                    "tp_pct": None,
                                    "open_now": len(state_now.get("open_positions", []) or []),
                                    "open_max": int(getattr(risk_autopilot, "max_open_positions", 0) or 0),
                                    "trades_today": int(state_now.get("trade_count_today", 0) or 0),
                                    "cooldown_until_utc": str(state_now.get("cooldown_until_utc", "") or ""),
                                    **format_caps,
                                },
                            )
                            if always_notify_intents:
                                blocked_msg = f"[BLOCK] {blocked_msg}"
                            _send_telegram_with_logging(
                                kind="intent",
                                token=telegram_token,
                                chat_id=telegram_chat_id,
                                text=blocked_msg,
                            )
                        elif should_notify_block:
                            _warn_missing_telegram_once()
        except Exception as exc:
            symbol_context["error"] = str(exc)
            logging.exception("Scan failed for %s: %s", symbol, exc)
        finally:
            run_context["symbols"].append(symbol_context)

    if signal_debug:
        logged = 0
        for symbol_ctx in run_context["symbols"]:
            if logged >= 10:
                break
            if symbol_ctx.get("final_intents"):
                continue
            debug_none = symbol_ctx.get("debug_why_none", {}) or {}
            if not debug_none:
                continue
            logging.info(
                "SIGNAL_DEBUG %s RB_RTG=%s | FB_FADE=%s",
                symbol_ctx.get("symbol", "?"),
                str(debug_none.get("RB_RTG", "n/a")),
                str(debug_none.get("FB_FADE", "n/a")),
            )
            logged += 1
    set_last_bias_json(bias_list)
    if strategy_v1_enabled and not has_any_allow:
        sorted_near = sorted(
            [n for n in all_near_miss_candidates if float(n.get("dist_atr", 0) or 0) > 0],
            key=lambda x: float(x.get("dist_atr", 0) or 0),
        )
        set_near_misses(sorted_near[:3])
    else:
        set_near_misses([])
    set_defer_position_sync(False)
    sync_paper_positions_from_state()
    set_symbols_v3(run_context.get("symbols_v3") or {})
    return run_context


def run_force_intents(force_intents: int, risk_autopilot) -> Dict[str, Any]:
    from risk_autopilot import SignalIntent

    symbols = ("BTCUSDT", "ETHUSDT")
    sides = ("LONG", "SHORT")
    run_context: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": "n/a",
        "watchlist_mode": "force_test",
        "watchlist": list(symbols),
        "symbols": [],
    }

    for idx in range(force_intents):
        symbol = symbols[idx % len(symbols)]
        side = sides[idx % len(sides)]
        now_iso = datetime.now(timezone.utc).isoformat()
        intent = SignalIntent(
            symbol=symbol,
            side=side,
            strategy="FORCE_TEST",
            reason="Synthetic stress-test intent",
            confidence=0.5,
            ts=now_iso,
        )
        allowed, reason = risk_autopilot.evaluate(intent)
        symbol_ctx = {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [
                {
                    "symbol": symbol,
                    "side": side,
                    "strategy": "FORCE_TEST",
                    "reason": "Synthetic stress-test intent",
                    "confidence": 0.5,
                    "ts": now_iso,
                    "intent_id": f"FORCE_TEST|{idx+1}|{symbol}|{side}",
                }
            ],
            "early_intents": [],
            "final_intents": [
                {
                    "symbol": symbol,
                    "side": side,
                    "strategy": "FORCE_TEST",
                    "reason": "Synthetic stress-test intent",
                    "confidence": 0.5,
                    "ts": now_iso,
                    "intent_id": f"FORCE_TEST|{idx+1}|{symbol}|{side}",
                    "risk": {"allowed": allowed, "reason": reason},
                }
            ],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": None,
        }
        run_context["symbols"].append(symbol_ctx)
        if allowed:
            risk_autopilot.record_allowed_intent(intent)
            logging.info("FORCE_TEST intent %d -> ALLOW reason=%s", idx + 1, reason)
        else:
            logging.warning("FORCE_TEST intent %d -> BLOCK reason=%s", idx + 1, reason)
    return run_context


def _build_scan_summary(run_context: Dict[str, Any]) -> str:
    """Build one-line scan summary: ts, watchlist, scanned, signals, intents, allow, block, top 3."""
    ts = str(run_context.get("ts", ""))[:19]
    watchlist = list(run_context.get("watchlist", []) or [])
    watchlist_count = len(watchlist)
    watchlist_preview = ", ".join(watchlist[:10]) if watchlist else "-"
    symbols = list(run_context.get("symbols", []) or [])
    scanned = len(symbols)
    signals_found = 0
    all_intents: List[Dict[str, Any]] = []
    last_error = ""
    for sym_ctx in symbols:
        signals_found += len(sym_ctx.get("candidates_before", []) or [])
        for rec in (sym_ctx.get("final_intents", []) or []):
            all_intents.append(rec)
        err = str(sym_ctx.get("error", "") or "").strip()
        if err:
            last_error = err
    intents_count = len(all_intents)
    allow_count = sum(1 for r in all_intents if (r.get("risk") or {}).get("allowed"))
    block_count = intents_count - allow_count
    top3 = all_intents[:3]
    top_lines = [
        f"  {r.get('symbol', '?')} {r.get('strategy', '?')} {((r.get('risk') or {}).get('allowed') and 'ALLOW' or 'BLOCK')}"
        for r in top3
    ]
    lines = [
        f"SCAN {ts} | watchlist={watchlist_count} ({watchlist_preview})",
        f"  symbols={scanned} signals={signals_found} intents={intents_count} allow={allow_count} block={block_count}",
        *top_lines,
    ]
    return "\n".join(lines)


def _emit_scan_summary_and_heartbeat(
    run_context: Dict[str, Any],
    *,
    config_module,
    telegram_token: str,
    telegram_chat_id: str,
    run_mode: str = "loop",
) -> None:
    """
    Send scan summary / heartbeat as event notifications.
    TELEGRAM_POLICY:
      - off: disable all telegram
      - signals: signal alerts only (no summary/heartbeat)
      - events: event alerts only (summary/heartbeat/open/close/block)
      - both: both signals and events
    Heartbeat: not sent on startup (requires at least 2 completed scans), includes run_mode, watchlist count, last_scan_ts.
    """
    from scalper.notifier import get_last_telegram_sent_at

    global _LAST_SCAN_SUMMARY_AT, _SCANS_COMPLETED
    _SCANS_COMPLETED += 1
    if not telegram_token or not telegram_chat_id:
        return
    policy = str(getattr(config_module, "TELEGRAM_POLICY", "events") or "events").strip().lower()
    if policy not in {"off", "signals", "events", "both"}:
        policy = "events"
    notify_summary = bool(getattr(config_module, "NOTIFY_SCAN_SUMMARY", False))
    disable_summary = bool(getattr(config_module, "DISABLE_SCAN_SUMMARY", True))
    heartbeat_min = int(getattr(config_module, "HEARTBEAT_MINUTES", 10) or 10)
    summary_min = int(getattr(config_module, "SCAN_SUMMARY_MINUTES", 30) or 30)
    now = time.time()
    idle_sec = now - get_last_telegram_sent_at()

    if policy == "off":
        return

    events_enabled = policy in {"events", "both"}
    may_send_scan_summary = events_enabled and notify_summary and not disable_summary
    if events_enabled and not may_send_scan_summary:
        logging.info(
            "SCAN_SUMMARY_SKIP policy=%s notify=%s disabled=%s",
            policy,
            notify_summary,
            disable_summary,
        )
    if may_send_scan_summary:
        elapsed = now - _LAST_SCAN_SUMMARY_AT
        if elapsed >= summary_min * 60:
            summary = _build_scan_summary(run_context)
            _send_telegram_with_logging(
                kind="scan_summary",
                token=telegram_token,
                chat_id=telegram_chat_id,
                text=summary,
            )
            _LAST_SCAN_SUMMARY_AT = now
    threshold_sec = heartbeat_min * 60
    may_send_heartbeat = (
        events_enabled
        and heartbeat_min > 0
        and idle_sec >= threshold_sec
        and _SCANS_COMPLETED >= 2
    )
    if not may_send_heartbeat and events_enabled and heartbeat_min > 0:
        logging.info(
            "HEARTBEAT_SKIP elapsed=%.0f threshold=%.0f scans=%d",
            idle_sec,
            threshold_sec,
            _SCANS_COMPLETED,
        )
    if may_send_heartbeat:
        from scalper.storage import get_last_scan_ts

        last_scan_ts = get_last_scan_ts() or 0
        watchlist = run_context.get("watchlist", []) or []
        last_error = ""
        for sym_ctx in (run_context.get("symbols", []) or []):
            err = str(sym_ctx.get("error", "") or "").strip()
            if err:
                last_error = err[:80]
                break
        try:
            from datetime import datetime, timezone
            last_scan_human = (
                datetime.fromtimestamp(int(last_scan_ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                if last_scan_ts else "-"
            )
        except (TypeError, ValueError, OSError):
            last_scan_human = "-"
        msg = (
            f"HEARTBEAT ok | mode={run_mode} | watchlist={len(watchlist)} | "
            f"last_scan={last_scan_human}"
        )
        if last_error:
            msg += f" | last_error={last_error}"
        _send_telegram_with_logging(
            kind="heartbeat",
            token=telegram_token,
            chat_id=telegram_chat_id,
            text=msg,
        )


def emit_dashboard(
    run_context: Dict[str, Any],
    *,
    config_module,
    telegram_token: str,
    telegram_chat_id: str,
    max_open_positions: int,
    run_mode: str = "loop",
) -> None:
    from scalper.dashboard import build_dashboard_report
    from scalper.storage import load_paper_state
    from scalper.telegram_format import format_dashboard_compact

    ctx = dict(run_context)
    ctx["top_n"] = config_module.DASHBOARD_TOP_N
    ctx["include_blocked"] = config_module.DASHBOARD_INCLUDE_BLOCKED
    ctx["include_market_snapshot"] = config_module.DASHBOARD_INCLUDE_MARKET_SNAPSHOT
    ctx["include_debug_why_none"] = bool(config_module.DASHBOARD_INCLUDE_DEBUG_WHY_NONE)
    ctx["max_open_positions"] = max_open_positions
    ctx["paper_state"] = load_paper_state()

    report = build_dashboard_report(ctx)
    logging.info("\n%s", report)

    if bool(getattr(config_module, "TELEGRAM_SEND_DASHBOARD", False)) and telegram_token and telegram_chat_id:
        telegram_context = dict(ctx)
        telegram_context["include_debug_why_none"] = False
        telegram_report = build_dashboard_report(telegram_context)
        telegram_report = format_dashboard_compact(telegram_report, max_len=1200)
        _send_telegram_with_logging(
            kind="intent",
            token=telegram_token,
            chat_id=telegram_chat_id,
            text=telegram_report,
        )
    elif bool(getattr(config_module, "TELEGRAM_SEND_DASHBOARD", False)):
        _warn_missing_telegram_once()

    _emit_scan_summary_and_heartbeat(
        run_context,
        config_module=config_module,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        run_mode=run_mode,
    )



def build_risk_autopilot(config_module):
    from risk_autopilot import RiskAutopilot

    risk_autopilot = RiskAutopilot.from_config(config_module)
    risk_autopilot.paper_fees_bps = float(getattr(config_module, "PAPER_FEES_BPS", 6.0))
    risk_autopilot.paper_sl_atr = float(getattr(config_module, "PAPER_SL_ATR", 1.0))
    risk_autopilot.paper_tp_atr = float(getattr(config_module, "PAPER_TP_ATR", 1.5))
    risk_autopilot.paper_timeout_bars = int(getattr(config_module, "PAPER_TIMEOUT_BARS", 12))
    risk_autopilot.paper_equity_usdt = float(getattr(config_module, "PAPER_EQUITY_USDT", 200.0))
    risk_autopilot.risk_per_trade_pct = float(getattr(config_module, "RISK_PER_TRADE_PCT", 0.15))
    risk_autopilot.max_notional_usdt = float(getattr(config_module, "MAX_NOTIONAL_USDT", 50.0))
    risk_autopilot.dedup_bars = int(getattr(config_module, "DEDUP_BARS", 4))
    risk_autopilot.paper_position_usdt = getattr(
        config_module, "PAPER_POSITION_USDT", 20.0
    )
    return risk_autopilot


def _run_one_scan_iteration(
    *,
    config_module,
    risk_autopilot,
    dryrun_notify_always: bool,
    symbols_override: Optional[list[str]] = None,
    paper_mode: bool = False,
) -> None:
    from scalper.storage import set_last_scan_ts

    watchlist, watchlist_mode = resolve_watchlist(config_module, symbols_override=symbols_override)
    if not watchlist:
        logging.error("Resolved watchlist is empty. Retrying on next cycle.")
        return

    run_context = run_scan_cycle(
        watchlist=watchlist,
        watchlist_mode=watchlist_mode,
        interval=config_module.INTERVAL,
        lookback=config_module.LOOKBACK,
        telegram_token=config_module.TELEGRAM_BOT_TOKEN,
        telegram_chat_id=config_module.TELEGRAM_CHAT_ID,
        risk_autopilot=risk_autopilot,
        notify_blocked_telegram=bool(
            getattr(config_module, "NOTIFY_BLOCKED", False)
            or getattr(config_module, "TELEGRAM_SEND_BLOCKED", False)
            or getattr(config_module, "RISK_NOTIFY_BLOCKED_TELEGRAM", False)
        ),
        always_notify_intents=bool(dryrun_notify_always or getattr(config_module, "ALWAYS_NOTIFY_INTENTS", False)),
        signal_debug=config_module.SIGNAL_DEBUG,
        early_enabled=bool(getattr(config_module, "EARLY_ENABLED", False)),
        early_tf=str(getattr(config_module, "EARLY_TF", 5)),
        early_lookback_5m=int(getattr(config_module, "EARLY_LOOKBACK_5M", 60)),
        early_min_conf=float(getattr(config_module, "EARLY_MIN_CONF", 0.35)),
        early_require_15m_context=bool(getattr(config_module, "EARLY_REQUIRE_15M_CONTEXT", True)),
        early_max_alerts_per_symbol_per_15m=int(
            getattr(config_module, "EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M", 1)
        ),
        telegram_early_enabled=bool(getattr(config_module, "TELEGRAM_EARLY_ENABLED", False)),
        telegram_early_max_per_symbol_per_15m=int(
            getattr(config_module, "TELEGRAM_EARLY_MAX_PER_SYMBOL_PER_15M", 1)
        ),
        threshold_profile=str(getattr(config_module, "THRESHOLD_PROFILE", "A")),
        telegram_format=str(getattr(config_module, "TELEGRAM_FORMAT", "compact")),
        telegram_compact=bool(getattr(config_module, "TELEGRAM_COMPACT", True)),
        telegram_max_chars_compact=int(getattr(config_module, "TELEGRAM_MAX_CHARS_COMPACT", 900)),
        telegram_max_chars_verbose=int(getattr(config_module, "TELEGRAM_MAX_CHARS_VERBOSE", 2500)),
        paper_mode=paper_mode,
    )
    from scalper.storage import set_selected_watchlist, set_stall_alerted

    set_last_scan_ts(int(time.time()))
    set_stall_alerted(False)
    set_selected_watchlist(
        list(run_context.get("watchlist", []) or []),
        str(run_context.get("watchlist_mode", "static") or "static"),
    )
    emit_dashboard(
        run_context,
        config_module=config_module,
        telegram_token=config_module.TELEGRAM_BOT_TOKEN,
        telegram_chat_id=config_module.TELEGRAM_CHAT_ID,
        max_open_positions=config_module.MAX_OPEN_POSITIONS,
        run_mode="loop",
    )


def _check_stall_and_alert(
    *,
    config_module,
    telegram_token: str,
    telegram_chat_id: str,
) -> None:
    """If stalled (no successful scan for 2*SCAN_SECONDS+30), send Telegram alert once per episode."""
    if not telegram_token or not telegram_chat_id:
        return
    from scalper.storage import get_last_scan_ts, get_last_scan_error, get_stall_alerted, set_stall_alerted

    scan_sec = max(1, int(getattr(config_module, "SCAN_SECONDS", 60)))
    stall_threshold = 2 * scan_sec + 30
    last_ts = get_last_scan_ts() or 0
    if last_ts <= 0:
        return
    now = int(time.time())
    if now - last_ts <= stall_threshold:
        return
    if get_stall_alerted():
        return
    last_error = get_last_scan_error()
    try:
        dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)
        last_success_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        last_success_str = str(last_ts)
    msg = f"STALL DETECTED | last_success={last_success_str} | last_error={last_error}"
    _send_telegram_with_logging(kind="stall", token=telegram_token, chat_id=telegram_chat_id, text=msg)
    set_stall_alerted(True)


def _run_one_scan_iteration_with_timeout(
    *,
    config_module,
    risk_autopilot,
    timeout_seconds: int,
    dryrun_notify_always: bool,
    symbols_override: Optional[list[str]],
    paper_mode: bool,
) -> bool:
    from scalper.storage import set_last_scan_error

    _check_stall_and_alert(
        config_module=config_module,
        telegram_token=config_module.TELEGRAM_BOT_TOKEN,
        telegram_chat_id=config_module.TELEGRAM_CHAT_ID,
    )
    done = threading.Event()

    def _target() -> None:
        try:
            _run_one_scan_iteration(
                config_module=config_module,
                risk_autopilot=risk_autopilot,
                dryrun_notify_always=dryrun_notify_always,
                symbols_override=symbols_override,
                paper_mode=paper_mode,
            )
        except Exception as exc:
            set_last_scan_error(str(exc))
            logging.exception("Scan iteration crashed: %s", exc)
        finally:
            done.set()

    worker = threading.Thread(target=_target, name="scan-cycle", daemon=True)
    worker.start()
    completed = done.wait(timeout=max(5, int(timeout_seconds)))
    if not completed:
        set_last_scan_error(f"Scan cycle exceeded timeout ({timeout_seconds}s)")
        logging.error(
            "Scan cycle exceeded timeout (%ss); skipping to keep dashboard responsive.",
            int(timeout_seconds),
        )
    return completed


def run_scan_loop_worker(
    *,
    config_module,
    risk_autopilot,
    stop_event: threading.Event,
    scan_timeout_seconds: int,
    dryrun_notify_always: bool,
    symbols_override: Optional[list[str]],
    paper_mode: bool,
) -> None:
    scan_seconds = max(1, int(getattr(config_module, "SCAN_SECONDS", 60)))
    while not stop_event.is_set():
        _run_one_scan_iteration_with_timeout(
            config_module=config_module,
            risk_autopilot=risk_autopilot,
            timeout_seconds=scan_timeout_seconds,
            dryrun_notify_always=dryrun_notify_always,
            symbols_override=symbols_override,
            paper_mode=paper_mode,
        )
        if stop_event.wait(scan_seconds):
            break


def run_with_args(args: argparse.Namespace, config_module=None) -> int:
    logger = logging.getLogger(__name__)

    config = config_module
    if config is None:
        import config as _config

        config = _config

    if getattr(args, "enable_scan_summary", False):
        config.DISABLE_SCAN_SUMMARY = False
        config.NOTIFY_SCAN_SUMMARY = True

    if args.log_level.upper() == "DEBUG":
        config.debug_env(logger)
        config.debug_risk_config(logger)

    symbols_override = _parse_symbols_override(getattr(args, "symbols", ""))
    if symbols_override:
        logging.info("CLI symbols override active: %s", ",".join(symbols_override))
    paper_mode = bool(getattr(args, "paper", False))
    if paper_mode:
        logging.info("PAPER mode enabled (--paper). No exchange private endpoints are used.")

    if args.cooldown_minutes <= 0:
        logging.error("--cooldown-minutes must be a positive integer.")
        return 2
    if args.force_intents < 0:
        logging.error("--force-intents must be >= 0.")
        return 2
    if args.test_telegram_formats:
        return run_test_telegram_formats(config)
    if args.reconcile:
        return run_reconcile(config, args.reconcile)
    if args.sizing_test:
        return run_sizing_test(config)

    if args.serve_dashboard:
        from dashboard_server import run_dashboard_server

        risk_autopilot = build_risk_autopilot(config)
        logging.info(
            "TELEGRAM_POLICY=%s NOTIFY_SCAN_SUMMARY=%s DISABLE_SCAN_SUMMARY=%s HEARTBEAT_MINUTES=%d",
            str(getattr(config, "TELEGRAM_POLICY", "events")),
            bool(getattr(config, "NOTIFY_SCAN_SUMMARY", False)),
            bool(getattr(config, "DISABLE_SCAN_SUMMARY", True)),
            int(getattr(config, "HEARTBEAT_MINUTES", 10)),
        )
        host = str(getattr(config, "DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1")
        port = int(getattr(config, "DASHBOARD_PORT", 8000) or 8000)
        scan_timeout_seconds = max(
            10,
            int(getattr(config, "SCAN_CYCLE_TIMEOUT_SECONDS", max(int(config.SCAN_SECONDS) * 2, 90))),
        )
        stop_event = threading.Event()
        scan_thread = threading.Thread(
            target=run_scan_loop_worker,
            kwargs={
                "config_module": config,
                "risk_autopilot": risk_autopilot,
                "stop_event": stop_event,
                "scan_timeout_seconds": scan_timeout_seconds,
                "dryrun_notify_always": bool(args.dryrun_notify_always),
                "symbols_override": symbols_override,
                "paper_mode": paper_mode,
            },
            name="scan-loop-worker",
            daemon=True,
        )
        scan_thread.start()
        logging.info(
            "Starting local DRY-RUN dashboard + scan loop at http://%s:%d (scan timeout=%ss)",
            host,
            port,
            scan_timeout_seconds,
        )
        try:
            run_dashboard_server(host=host, port=port, log_level=args.log_level)
        except KeyboardInterrupt:
            logging.info("Ctrl+C received, shutting down dashboard and scan loop.")
        finally:
            stop_event.set()
            scan_thread.join(timeout=15)
            if scan_thread.is_alive():
                logging.warning("Scan loop worker did not stop within timeout; exiting anyway.")
        return 0
    if args.test_telegram:
        try:
            _send_telegram_with_logging(
                kind="test",
                token=config.TELEGRAM_BOT_TOKEN,
                chat_id=config.TELEGRAM_CHAT_ID,
                text="Telegram OK (test)",
                strict=True,
            )
            logging.info("Telegram test message sent. Exiting --test-telegram mode.")
            return 0
        except Exception:
            logging.error("Telegram test failed. Check token/chat_id/network.")
            return 1

    if config.WATCHLIST_MODE == "static" and not config.WATCHLIST:
        logging.error("WATCHLIST is empty. Set WATCHLIST in .env for static mode.")
        return 2

    risk_autopilot = build_risk_autopilot(config)

    logging.info("Starting Bybit Signal Bot in DRY RUN mode (no trading).")
    logging.info(
        "TELEGRAM_POLICY=%s NOTIFY_SCAN_SUMMARY=%s DISABLE_SCAN_SUMMARY=%s HEARTBEAT_MINUTES=%d",
        str(getattr(config, "TELEGRAM_POLICY", "events")),
        bool(getattr(config, "NOTIFY_SCAN_SUMMARY", False)),
        bool(getattr(config, "DISABLE_SCAN_SUMMARY", True)),
        int(getattr(config, "HEARTBEAT_MINUTES", 10)),
    )
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        _warn_missing_telegram_once()

    if args.once:
        if args.force_intents > 0:
            logging.info(
                "Running FORCE_TEST in --once mode with %d synthetic intents.",
                args.force_intents,
            )
            force_context = run_force_intents(
                force_intents=args.force_intents,
                risk_autopilot=risk_autopilot,
            )
            from scalper.storage import set_last_scan_ts, set_selected_watchlist, set_stall_alerted

            set_last_scan_ts(int(time.time()))
            set_stall_alerted(False)
            set_selected_watchlist(
                list(force_context.get("watchlist", []) or []),
                str(force_context.get("watchlist_mode", "static") or "static"),
            )
            emit_dashboard(
                force_context,
                config_module=config,
                telegram_token=config.TELEGRAM_BOT_TOKEN,
                telegram_chat_id=config.TELEGRAM_CHAT_ID,
                max_open_positions=config.MAX_OPEN_POSITIONS,
                run_mode="once",
            )
            logging.info("Completed FORCE_TEST run. Exiting --once mode.")
            return 0
        watchlist, watchlist_mode = resolve_watchlist(config, symbols_override=symbols_override)
        if not watchlist:
            logging.error("Resolved watchlist is empty. Check WATCHLIST or WATCHLIST_MODE settings.")
            return 2
        run_context = run_scan_cycle(
            watchlist=watchlist,
            watchlist_mode=watchlist_mode,
            interval=config.INTERVAL,
            lookback=config.LOOKBACK,
            telegram_token=config.TELEGRAM_BOT_TOKEN,
            telegram_chat_id=config.TELEGRAM_CHAT_ID,
            risk_autopilot=risk_autopilot,
            notify_blocked_telegram=bool(
                getattr(config, "NOTIFY_BLOCKED", False)
                or getattr(config, "TELEGRAM_SEND_BLOCKED", False)
                or getattr(config, "RISK_NOTIFY_BLOCKED_TELEGRAM", False)
            ),
            always_notify_intents=bool(args.dryrun_notify_always or getattr(config, "ALWAYS_NOTIFY_INTENTS", False)),
            signal_debug=config.SIGNAL_DEBUG,
            early_enabled=bool(getattr(config, "EARLY_ENABLED", False)),
            early_tf=str(getattr(config, "EARLY_TF", 5)),
            early_lookback_5m=int(getattr(config, "EARLY_LOOKBACK_5M", 60)),
            early_min_conf=float(getattr(config, "EARLY_MIN_CONF", 0.35)),
            early_require_15m_context=bool(getattr(config, "EARLY_REQUIRE_15M_CONTEXT", True)),
            early_max_alerts_per_symbol_per_15m=int(
                getattr(config, "EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M", 1)
            ),
            telegram_early_enabled=bool(getattr(config, "TELEGRAM_EARLY_ENABLED", False)),
            telegram_early_max_per_symbol_per_15m=int(
                getattr(config, "TELEGRAM_EARLY_MAX_PER_SYMBOL_PER_15M", 1)
            ),
            threshold_profile=str(getattr(config, "THRESHOLD_PROFILE", "A")),
            telegram_format=str(getattr(config, "TELEGRAM_FORMAT", "compact")),
            telegram_compact=bool(getattr(config, "TELEGRAM_COMPACT", True)),
            telegram_max_chars_compact=int(getattr(config, "TELEGRAM_MAX_CHARS_COMPACT", 900)),
            telegram_max_chars_verbose=int(getattr(config, "TELEGRAM_MAX_CHARS_VERBOSE", 2500)),
            paper_mode=paper_mode,
        )
        from scalper.storage import set_last_scan_ts, set_selected_watchlist, set_stall_alerted

        set_last_scan_ts(int(time.time()))
        set_stall_alerted(False)
        set_selected_watchlist(list(run_context.get("watchlist", []) or []), str(run_context.get("watchlist_mode", "static") or "static"))
        emit_dashboard(
            run_context,
            config_module=config,
            telegram_token=config.TELEGRAM_BOT_TOKEN,
            telegram_chat_id=config.TELEGRAM_CHAT_ID,
            max_open_positions=config.MAX_OPEN_POSITIONS,
            run_mode="once",
        )
        logging.info("Completed one scan cycle. Exiting --once mode.")
        return 0

    if args.force_intents > 0:
        logging.warning("--force-intents is only applied with --once; ignoring in loop mode.")

    from scalper.storage import set_last_scan_error, set_last_scan_ts, set_selected_watchlist, set_stall_alerted

    scan_seconds = max(1, int(config.SCAN_SECONDS))
    logging.info(
        "Starting --loop mode (single process, scan every %ds). Ctrl+C to stop. Cache stays warm.",
        scan_seconds,
    )
    try:
        while True:
            _check_stall_and_alert(
                config_module=config,
                telegram_token=config.TELEGRAM_BOT_TOKEN,
                telegram_chat_id=config.TELEGRAM_CHAT_ID,
            )
            watchlist, watchlist_mode = resolve_watchlist(config, symbols_override=symbols_override)
            if not watchlist:
                logging.error("Resolved watchlist is empty. Retrying on next cycle.")
                time.sleep(scan_seconds)
                continue
            try:
                run_context = run_scan_cycle(
                    watchlist=watchlist,
                    watchlist_mode=watchlist_mode,
                    interval=config.INTERVAL,
                    lookback=config.LOOKBACK,
                    telegram_token=config.TELEGRAM_BOT_TOKEN,
                    telegram_chat_id=config.TELEGRAM_CHAT_ID,
                    risk_autopilot=risk_autopilot,
                    notify_blocked_telegram=bool(
                        getattr(config, "NOTIFY_BLOCKED", False)
                        or getattr(config, "TELEGRAM_SEND_BLOCKED", False)
                        or getattr(config, "RISK_NOTIFY_BLOCKED_TELEGRAM", False)
                    ),
                    always_notify_intents=bool(args.dryrun_notify_always or getattr(config, "ALWAYS_NOTIFY_INTENTS", False)),
                    signal_debug=config.SIGNAL_DEBUG,
                    early_enabled=bool(getattr(config, "EARLY_ENABLED", False)),
                    early_tf=str(getattr(config, "EARLY_TF", 5)),
                    early_lookback_5m=int(getattr(config, "EARLY_LOOKBACK_5M", 60)),
                    early_min_conf=float(getattr(config, "EARLY_MIN_CONF", 0.35)),
                    early_require_15m_context=bool(getattr(config, "EARLY_REQUIRE_15M_CONTEXT", True)),
                    early_max_alerts_per_symbol_per_15m=int(
                        getattr(config, "EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M", 1)
                    ),
                    telegram_early_enabled=bool(getattr(config, "TELEGRAM_EARLY_ENABLED", False)),
                    telegram_early_max_per_symbol_per_15m=int(
                        getattr(config, "TELEGRAM_EARLY_MAX_PER_SYMBOL_PER_15M", 1)
                    ),
                    threshold_profile=str(getattr(config, "THRESHOLD_PROFILE", "A")),
                    telegram_format=str(getattr(config, "TELEGRAM_FORMAT", "compact")),
                    telegram_compact=bool(getattr(config, "TELEGRAM_COMPACT", True)),
                    telegram_max_chars_compact=int(getattr(config, "TELEGRAM_MAX_CHARS_COMPACT", 900)),
                    telegram_max_chars_verbose=int(getattr(config, "TELEGRAM_MAX_CHARS_VERBOSE", 2500)),
                    paper_mode=paper_mode,
                )
                set_last_scan_ts(int(time.time()))
                set_stall_alerted(False)
                set_selected_watchlist(list(run_context.get("watchlist", []) or []), str(run_context.get("watchlist_mode", "static") or "static"))
                emit_dashboard(
                    run_context,
                    config_module=config,
                    telegram_token=config.TELEGRAM_BOT_TOKEN,
                    telegram_chat_id=config.TELEGRAM_CHAT_ID,
                    max_open_positions=config.MAX_OPEN_POSITIONS,
                    run_mode="loop",
                )
            except Exception as exc:
                set_last_scan_error(str(exc))
                logging.exception("Scan iteration crashed: %s", exc)
            time.sleep(scan_seconds)
    except KeyboardInterrupt:
        logging.info("Ctrl+C received, shutting down gracefully.")
    return 0


class Scanner:
    """Main scanner runtime preserving existing DRY RUN behavior."""

    def __init__(self, config_module, args: argparse.Namespace):
        self.config = config_module
        self.args = args

    def run(self) -> int:
        return run_with_args(self.args, config_module=self.config)


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    return run_with_args(args)
=======
from scalper.scalper.scanner import *  # noqa: F401,F403
from scalper.scalper.scanner import _apply_preview_gate  # noqa: F401
>>>>>>> b1a8f4e7765cfa90c470121f7cfaad7339fce0ee


if __name__ == "__main__":
    from scalper.scalper.scanner import main

    raise SystemExit(main())
