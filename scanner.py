import argparse
import logging
import os
import sys
import time
import threading
from pathlib import Path
from datetime import timezone, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

try:
    import config as _config  # shim/backcompat
except Exception:
    _config = None


_INTENT_FINGERPRINT_CACHE: set[str] = set()
_EARLY_ALERT_CACHE: set[str] = set()
_EXIT_ALERTS_SENT: Dict[str, set] = {}  # intent_id -> set of event_type for dedupe
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
        "--instance-id",
        type=str,
        default="",
        help="Optional instance suffix for lock/log files (for intentional multi-instance runs).",
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
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Run offline diagnostics (env sanitize, strategies, telegram config, conflict check) and exit.",
    )
    parser.add_argument(
        "--test-intent-notify",
        nargs=2,
        metavar=("SYMBOL", "SIDE"),
        help="Run synthetic approved-intent notification pipeline and exit.",
    )
    parser.add_argument(
        "--test-real-intent-format",
        type=str,
        default="",
        help="Build real-like intent from live data, run real formatter path, and exit.",
    )
    return parser.parse_args()


def setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    root = logging.getLogger()
    # Avoid adding duplicate stdout handlers if called multiple times
    if not any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in root.handlers
    ):
        root.addHandler(handler)
    root.setLevel(level)


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


def _approved_intent_notify_decision(
    *,
    telegram_token: str,
    telegram_chat_id: str,
    policy: str,
    budget: int,
    paper_mode: bool,
    dryrun_notify: bool,
    always_notify_intents: bool,
) -> Dict[str, Any]:
    telegram_enabled = bool(str(telegram_token or "").strip()) and bool(str(telegram_chat_id or "").strip())
    policy_norm = str(policy or "events").strip().lower()
    allowed = True
    reason = "enabled"
    if not telegram_enabled:
        allowed = False
        reason = "disabled_no_config"
    elif policy_norm in {"off", "none", "disabled"}:
        allowed = False
        reason = "policy_off"
    logging.info(
        "TELEGRAM_POLICY_EVAL kind=intent policy=%s allowed=%s reason=%s",
        policy_norm,
        bool(allowed),
        reason,
    )
    if not telegram_enabled:
        return {
            "should_notify": False,
            "notify_reason": "DISABLED",
            "policy": policy_norm,
            "budget": int(budget),
            "paper_mode": bool(paper_mode),
            "dryrun_notify": bool(dryrun_notify),
            "always_notify_intents": bool(always_notify_intents),
        }
    if policy_norm in {"off", "none", "disabled"}:
        return {
            "should_notify": False,
            "notify_reason": "POLICY_OFF",
            "policy": policy_norm,
            "budget": int(budget),
            "paper_mode": bool(paper_mode),
            "dryrun_notify": bool(dryrun_notify),
            "always_notify_intents": bool(always_notify_intents),
        }
    return {
        "should_notify": True,
        "notify_reason": "ENABLED",
        "policy": policy_norm,
        "budget": int(budget),
        "paper_mode": bool(paper_mode),
        "dryrun_notify": bool(dryrun_notify),
        "always_notify_intents": bool(always_notify_intents),
    }


def _dispatch_intent_notification(
    *,
    symbol: str,
    strategy: str,
    side: str,
    kind: str,
    text: str,
    entry: Any,
    sl: Any,
    tp: Any,
    conf: Any,
    source: str,
    telegram_token: str,
    telegram_chat_id: str,
    should_notify: bool,
    notify_reason: str,
) -> str:
    from scalper.notifier import get_last_telegram_meta, get_last_telegram_status

    if not should_notify:
        logging.info(
            "TELEGRAM_PAYLOAD symbol=%s side=%s strategy=%s entry=%s sl=%s tp=%s conf=%s source=%s",
            symbol,
            side,
            strategy,
            str(entry),
            str(sl),
            str(tp),
            str(conf),
            str(source),
        )
        logging.info(
            "INTENT_NOTIFY_BYPASSED reason=%s symbol=%s",
            str(notify_reason or "DISABLED"),
            symbol,
        )
        logging.info(
            "INTENT_NOTIFY_RESULT symbol=%s strategy=%s side=%s status=DISABLED",
            symbol,
            strategy,
            side,
        )
        return "DISABLED"
    logging.info(
        "INTENT_NOTIFY_START symbol=%s strategy=%s side=%s kind=%s",
        symbol,
        strategy,
        side,
        kind,
    )
    logging.info(
        "TELEGRAM_PAYLOAD symbol=%s side=%s strategy=%s entry=%s sl=%s tp=%s conf=%s source=%s",
        symbol,
        side,
        strategy,
        str(entry),
        str(sl),
        str(tp),
        str(conf),
        str(source),
    )
    _send_telegram_with_logging(
        kind=kind,
        token=telegram_token,
        chat_id=telegram_chat_id,
        text=text,
    )
    status = str(get_last_telegram_status() or "SEND_FAILURE")
    meta = get_last_telegram_meta()
    sent_today = meta.get("sent_today")
    budget = meta.get("budget")
    if status == "BUDGET_BLOCK":
        logging.info(
            "INTENT_NOTIFY_RESULT symbol=%s strategy=%s side=%s status=%s sent_today=%s budget=%s",
            symbol,
            strategy,
            side,
            status,
            sent_today,
            budget,
        )
    else:
        logging.info(
            "INTENT_NOTIFY_RESULT symbol=%s strategy=%s side=%s status=%s",
            symbol,
            strategy,
            side,
            status,
        )
    return status


def _validate_real_intent_pricing(
    *,
    symbol: str,
    entry: Any,
    sl: Any,
    tp: Any,
    market_px: Any,
) -> tuple[bool, str]:
    try:
        e = float(entry)
        s = float(sl)
        t = float(tp)
    except (TypeError, ValueError):
        return (False, "missing_or_non_numeric_prices")
    if e <= 0 or s <= 0 or t <= 0:
        return (False, "non_positive_prices")
    # Synthetic test payload pattern guard.
    if abs(e - 100.0) <= 1e-8 and abs(s - 99.2) <= 1e-8 and abs(t - 101.2) <= 1e-8:
        return (False, "synthetic_test_pattern_detected")
    try:
        m = float(market_px)
    except (TypeError, ValueError):
        m = 0.0
    if m > 0:
        diff_pct = abs((e - m) / m) * 100.0
        if diff_pct > 20.0:
            logging.error(
                "PRICING_SANITY_FAIL symbol=%s entry=%.8f market_px=%.8f diff_pct=%.2f",
                symbol,
                e,
                m,
                diff_pct,
            )
            return (False, "pricing_sanity_fail")
    return (True, "")

def _atr14_from_candles(candles: List[Dict[str, Any]]) -> float:
    """Compute ATR(14) from list of candle dicts (high, low, close). Returns 0 if insufficient data."""
    if not candles or len(candles) < 15:
        return 0.0
    tr_list: List[float] = []
    prev_close = float((candles[0] or {}).get("close", 0) or 0)
    for i in range(1, len(candles)):
        c = candles[i] or {}
        high = float(c.get("high", 0) or 0)
        low = float(c.get("low", 0) or 0)
        close = float(c.get("close", 0) or 0)
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        tr_list.append(tr)
        prev_close = close
    if len(tr_list) < 14:
        return 0.0
    # Wilder smoothing: first ATR = SMA(TR, 14), then ATR = (prev_ATR * 13 + TR) / 14
    atr = sum(tr_list[-14:]) / 14.0
    return atr


def _is_breakout_strategy(strategy: str) -> bool:
    """True if strategy is a breakout type (blocked when BTC ATR% > 2.5)."""
    s = str(strategy or "").upper()
    return "BREAKOUT" in s


def _compute_signal_score(
    signal: Dict[str, Any],
    snap: Dict[str, Any],
    candles: List[Dict[str, Any]],
    atr_pct: float,
) -> float:
    """Score = 0.4*breakout_strength + 0.3*volume_spike + 0.2*ema_distance + 0.1*atr_quality. All components 0-1."""
    breakout_strength = min(1.0, max(0.0, float(signal.get("confidence", 0.5) or 0.5)))
    volume_spike = 0.5
    if candles and len(candles) >= 14:
        vols = [float((c or {}).get("volume", 0) or 0) for c in candles[-20:]]
        avg_v = sum(vols) / len(vols) if vols else 0
        cur_v = float((candles[-1] or {}).get("volume", 0) or 0)
        if avg_v > 0 and cur_v > 0:
            volume_spike = min(1.0, cur_v / avg_v)
    close = float(snap.get("last_close", 0) or snap.get("close", 0) or 0)
    if close <= 0 and candles:
        close = float((candles[-1] or {}).get("close", 0) or 0)
    ema200 = float(snap.get("ema200", 0) or 0)
    ema_distance = 0.5
    if close > 0 and ema200 > 0:
        dist_pct = abs(close - ema200) / close * 100.0
        ema_distance = min(1.0, dist_pct / 2.0)
    atr_quality = 1.0 - min(1.0, abs(atr_pct - 1.5) / 1.5) if 0.5 <= atr_pct <= 3.0 else 0.0
    score = (
        0.4 * breakout_strength
        + 0.3 * volume_spike
        + 0.2 * ema_distance
        + 0.1 * atr_quality
    )
    return round(score, 4)


def _compute_volatility_metrics(
    *,
    candles: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    tf: str,
) -> Dict[str, Any]:
    """
    Compute volatility metrics robustly for filters/diagnostics.
    Returns: atr_pct, vol_pct, data_ok, reason, n_bars, last_ts, atr14, price
    """
    n_bars = len(candles or [])
    last = (candles[-1] if candles else {}) or {}
    last_ts = str(last.get("timestamp_utc", last.get("ts", "")) or "")

    # price from snapshot first, then candles.
    price = float(snapshot.get("last_close", 0) or snapshot.get("close", 0) or last.get("close", 0) or 0)
    atr14 = float(snapshot.get("atr14", 0) or 0)
    if atr14 <= 0 and candles and len(candles) >= 15:
        atr14 = float(_atr14_from_candles(candles) or 0.0)

    if n_bars <= 0:
        return {
            "data_ok": False,
            "reason": "NO_BARS",
            "n_bars": n_bars,
            "last_ts": last_ts,
            "atr_pct": None,
            "vol_pct": None,
            "atr14": atr14,
            "price": price,
            "tf": str(tf),
        }
    if price <= 0:
        return {
            "data_ok": False,
            "reason": "INVALID_PRICE",
            "n_bars": n_bars,
            "last_ts": last_ts,
            "atr_pct": None,
            "vol_pct": None,
            "atr14": atr14,
            "price": price,
            "tf": str(tf),
        }
    if atr14 <= 0:
        return {
            "data_ok": False,
            "reason": "INVALID_ATR14",
            "n_bars": n_bars,
            "last_ts": last_ts,
            "atr_pct": None,
            "vol_pct": None,
            "atr14": atr14,
            "price": price,
            "tf": str(tf),
        }

    high = float(last.get("high", 0) or 0)
    low = float(last.get("low", 0) or 0)
    if high <= 0 or low < 0 or high < low:
        return {
            "data_ok": False,
            "reason": "INVALID_HIGH_LOW",
            "n_bars": n_bars,
            "last_ts": last_ts,
            "atr_pct": None,
            "vol_pct": None,
            "atr14": atr14,
            "price": price,
            "tf": str(tf),
        }

    atr_pct = (atr14 / max(price, 1e-10)) * 100.0
    vol_pct = ((high - low) / max(price, 1e-10)) * 100.0
    return {
        "data_ok": True,
        "reason": "",
        "n_bars": n_bars,
        "last_ts": last_ts,
        "atr_pct": float(atr_pct),
        "vol_pct": float(vol_pct),
        "atr14": atr14,
        "price": price,
        "tf": str(tf),
    }


def _self_check_volatility_metrics(metrics: Dict[str, Any]) -> None:
    """Basic self-check: vol_pct/atr_pct must be finite non-negative when data_ok."""
    if not metrics.get("data_ok"):
        return
    atr_pct = float(metrics.get("atr_pct", 0.0) or 0.0)
    vol_pct = float(metrics.get("vol_pct", 0.0) or 0.0)
    if atr_pct != atr_pct or vol_pct != vol_pct:  # NaN check
        raise ValueError("volatility metrics contain NaN")
    if atr_pct < 0 or vol_pct < 0:
        raise ValueError("volatility metrics contain negative values")


def _candidate_prefilter_reason(
    *,
    side: str,
    interval: str,
    vol_metrics: Dict[str, Any],
    snapshot: Dict[str, Any],
    bias_info: Dict[str, Any],
    config_module: Any,
) -> str:
    """Return prefilter block reason or empty string."""
    side_u = str(side or "").upper()
    if side_u not in {"LONG", "SHORT"}:
        return "INVALID_SIDE"

    # Volatility gate (candidate-level, never symbol-level pre-check).
    min_atr_pct = 0.35 if str(interval) == "5" else 0.5
    if not vol_metrics.get("data_ok"):
        return "VOL_DATA_MISSING"
    atr_pct = float(vol_metrics.get("atr_pct", 0.0) or 0.0)
    if atr_pct < min_atr_pct or atr_pct > 3.0:
        return "VOL_FILTER"

    # Spread gate.
    max_spread_bps = float(getattr(config_module, "WATCHLIST_MAX_SPREAD_BPS", 0.0) or 0.0)
    spread_bps_raw = snapshot.get("spread_bps")
    if max_spread_bps > 0 and spread_bps_raw is not None:
        try:
            spread_bps = float(spread_bps_raw)
            if spread_bps > max_spread_bps:
                return "SPREAD_FILTER"
        except (TypeError, ValueError):
            pass

    # Turnover gate.
    min_turnover = float(getattr(config_module, "WATCHLIST_MIN_TURNOVER_24H", 0.0) or 0.0)
    turnover_raw = snapshot.get("turnover_24h")
    if min_turnover > 0 and turnover_raw is not None:
        try:
            turnover = float(turnover_raw)
            if turnover < min_turnover:
                return "TURNOVER_FILTER"
        except (TypeError, ValueError):
            pass

    # Higher-TF context gate.
    require_align = bool(getattr(config_module, "REQUIRE_1H_EMA200_ALIGN", False))
    bias = str((bias_info or {}).get("bias", "NONE") or "NONE").upper()
    if require_align and bias in {"LONG", "SHORT"} and bias != side_u:
        return "HTF_FILTER"

    return ""


def _intent_fingerprint(symbol: str, strategy: str, side: str, bar_ts: str, profile: str = "") -> str:
    base = f"{symbol}|{strategy}|{side}|{bar_ts}"
    if profile:
        return f"{base}|{profile}"
    return base


def _fingerprint_group_key(symbol: str, strategy: str, side: str) -> str:
    return f"{symbol}|{strategy}|{side}"


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


def _codebase_conflict_check() -> Dict[str, Any]:
    root = Path.cwd()
    files: List[Path] = []
    for p in list((root / "scalper").rglob("*.py")):
        if ".venv" in str(p):
            continue
        files.append(p)
    for p in (root / "bot.py", root / "config.py", root / "signals.py", root / "storage.py", root / "bybit.py", root / "telegram_notify.py"):
        if p.exists():
            files.append(p)
    checked: List[str] = []
    bad: List[str] = []
    seen = set()
    for p in files:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        ok = True
        try:
            txt = p.read_text(encoding="utf-8", errors="replace")
            has_marker = False
            for line in txt.splitlines():
                stripped = line.strip()
                if stripped.startswith("<<<<<<<") or stripped.startswith(">>>>>>>") or stripped == "=======":
                    has_marker = True
                    break
            if has_marker:
                ok = False
                bad.append(str(p))
        except Exception:
            ok = False
            bad.append(str(p))
        logging.info("CODEBASE_CONFLICT_CHECK ok=%s file=%s", ok, str(p))
        checked.append(str(p))
    return {"ok": len(bad) == 0, "checked": checked, "bad": bad}


def run_doctor(config_module) -> int:
    from scalper.settings import sanitize_csv_env
    from scalper.strategies.registry import parse_strategies_enabled_diagnostics

    raw_watchlist = str(getattr(config_module, "_RAW_WATCHLIST", "") or "")
    watchlist = sanitize_csv_env(raw_watchlist, uppercase=True)
    print(f"WATCHLIST_SANITIZED raw_len={len(raw_watchlist)} parsed_len={len(watchlist)} symbols={watchlist}")

    raw_enabled = str(getattr(config_module, "STRATEGIES_ENABLED", "") or "")
    diag = parse_strategies_enabled_diagnostics(raw_enabled)
    print(f"STRATEGIES_ENABLED raw={raw_enabled}")
    print(f"STRATEGIES_ENABLED canonical={diag.get('canonical', [])}")
    for item in (diag.get("unknown", []) or []):
        print(f"UNKNOWN_STRATEGY raw={item.get('raw', '')} normalized={item.get('normalized', '')}")
    print(f"ACTIVE_STRATEGIES={diag.get('canonical', [])}")

    token_set = bool(str(getattr(config_module, "TELEGRAM_BOT_TOKEN", "") or "").strip())
    chat_set = bool(str(getattr(config_module, "TELEGRAM_CHAT_ID", "") or "").strip())
    print(f"TELEGRAM_CONFIG token_set={token_set} chat_set={chat_set}")

    conflict = _codebase_conflict_check()
    if conflict.get("ok", False):
        print("CODEBASE_CONFLICT_CHECK OK")
        print("DOCTOR_STATUS OK")
        return 0
    print(f"CODEBASE_CONFLICT_CHECK PROBLEMS files={conflict.get('bad', [])}")
    print("DOCTOR_STATUS PROBLEMS FOUND")
    return 2


def run_test_intent_notify(
    config_module,
    *,
    symbol: str,
    side: str,
    paper_mode: bool,
    dryrun_notify: bool,
    always_notify_intents: bool,
) -> int:
    from storage import load_paper_state
    from telegram_format import format_intent_allow

    sym = str(symbol or "").strip().upper()
    side_u = str(side or "").strip().upper()
    if not sym or side_u not in {"LONG", "SHORT"}:
        logging.error("--test-intent-notify requires SYMBOL and SIDE(LONG|SHORT)")
        return 2
    policy_now = str(getattr(config_module, "TELEGRAM_POLICY", "events") or "events")
    budget_now = int(getattr(config_module, "TELEGRAM_DAILY_BUDGET", 0) or 0)
    state_now = load_paper_state()
    msg = format_intent_allow(
        {
            "symbol": sym,
            "side": side_u,
            "strategy": "REV_SWEPT_RSI",
            "confidence": 0.68,
            "reason": "Synthetic approved intent pipeline test",
            "intent_id": f"TEST_NOTIFY_{sym}_{side_u}",
            "profile": "",
            "meta": {},
        },
        {"reason": "allowed"},
        {
            "tf": str(getattr(config_module, "INTERVAL", "15")),
            "entry": 100.0,
            "sl": 99.2 if side_u == "LONG" else 100.8,
            "tp": 101.2 if side_u == "LONG" else 98.8,
            "sl_pct": 0.8,
            "tp_pct": 1.2,
            "qty": 1.0,
            "notional": 100.0,
            "bar_ts_used": datetime.now(timezone.utc).isoformat(),
            "open_now": len(state_now.get("open_positions", []) or []),
            "open_max": int(getattr(config_module, "MAX_OPEN_POSITIONS", 0) or 0),
            "trades_today": int(state_now.get("trade_count_today", 0) or 0),
            "cooldown_until_utc": str(state_now.get("cooldown_until_utc", "") or ""),
            "telegram_format": "compact" if bool(getattr(config_module, "TELEGRAM_COMPACT", True)) else str(getattr(config_module, "TELEGRAM_FORMAT", "compact")),
            "telegram_max_chars_compact": int(getattr(config_module, "TELEGRAM_MAX_CHARS_COMPACT", 900)),
            "telegram_max_chars_verbose": int(getattr(config_module, "TELEGRAM_MAX_CHARS_VERBOSE", 2500)),
            "source": "test_intent",
        },
    )
    logging.info("NOTIFY_PIPELINE mode=test_intent")
    logging.info(
        "INTENT_APPROVED symbol=%s strategy=%s side=%s conf=%.3f entry=%s sl=%s tp=%s",
        sym,
        "REV_SWEPT_RSI",
        side_u,
        0.68,
        "100.0",
        "99.2" if side_u == "LONG" else "100.8",
        "101.2" if side_u == "LONG" else "98.8",
    )
    decision = _approved_intent_notify_decision(
        telegram_token=str(getattr(config_module, "TELEGRAM_BOT_TOKEN", "") or ""),
        telegram_chat_id=str(getattr(config_module, "TELEGRAM_CHAT_ID", "") or ""),
        policy=policy_now,
        budget=budget_now,
        paper_mode=paper_mode,
        dryrun_notify=dryrun_notify,
        always_notify_intents=always_notify_intents,
    )
    logging.info(
        "APPROVED_INTENT_DECISION symbol=%s should_notify=%s notify_reason=%s policy=%s budget=%s paper_mode=%s dryrun_notify=%s",
        sym,
        bool(decision.get("should_notify", False)),
        str(decision.get("notify_reason", "DISABLED")),
        str(decision.get("policy", "events")),
        int(decision.get("budget", 0) or 0),
        bool(decision.get("paper_mode", False)),
        bool(decision.get("dryrun_notify", False)),
    )
    status = _dispatch_intent_notification(
        symbol=sym,
        strategy="REV_SWEPT_RSI",
        side=side_u,
        kind="intent",
        text=msg,
        entry=100.0,
        sl=99.2 if side_u == "LONG" else 100.8,
        tp=101.2 if side_u == "LONG" else 98.8,
        conf=0.68,
        source="test_intent",
        telegram_token=str(getattr(config_module, "TELEGRAM_BOT_TOKEN", "") or ""),
        telegram_chat_id=str(getattr(config_module, "TELEGRAM_CHAT_ID", "") or ""),
        should_notify=bool(decision.get("should_notify", False)),
        notify_reason=str(decision.get("notify_reason", "DISABLED")),
    )
    return 0 if status in {"SEND_SUCCESS", "POLICY_SKIP", "BUDGET_BLOCK", "DISABLED"} else 1


def run_test_real_intent_format(config_module, symbol: str) -> int:
    from bybit import fetch_klines
    from paper_engine import compute_entry_sl_tp_for_display
    from telegram_format import format_intent_allow

    sym = str(symbol or "").strip().upper()
    if not sym:
        logging.error("--test-real-intent-format requires SYMBOL")
        return 2
    candles = fetch_klines(
        symbol=sym,
        interval=str(getattr(config_module, "INTERVAL", "15")),
        limit=max(50, int(getattr(config_module, "LOOKBACK", 300) or 300)),
    ) or []
    if not candles:
        logging.error("test-real-intent-format failed: no candles for %s", sym)
        return 2
    last = candles[-1] or {}
    close_px = float(last.get("close", 0.0) or 0.0)
    atr14 = 0.0
    try:
        atr14 = float(_atr14_from_candles(candles) or 0.0)
    except Exception:
        atr14 = 0.0
    snapshot = {"close": close_px, "last_close": close_px, "atr14": atr14}
    side = "LONG"
    trade_intent = {
        "symbol": sym,
        "side": side,
        "strategy": "REV_SWEPT_RSI",
        "setup": "REV_SWEPT_RSI",
        "direction": side,
        "entry_type": "market",
        "meta": {},
        "intent_id": f"REAL_FMT_{sym}",
    }
    display = compute_entry_sl_tp_for_display(
        trade_intent,
        candles,
        snapshot,
        sl_atr_mult=float(getattr(config_module, "PAPER_SL_ATR", 1.3) or 1.3),
        tp_atr_mult=float(getattr(config_module, "PAPER_TP_ATR", 2.0) or 2.0),
    )
    if not display:
        logging.error("test-real-intent-format failed: no display values for %s", sym)
        return 2
    ok_price, reason = _validate_real_intent_pricing(
        symbol=sym,
        entry=display.get("entry"),
        sl=display.get("sl"),
        tp=display.get("tp"),
        market_px=close_px,
    )
    if not ok_price:
        logging.error("INVALID_APPROVED_INTENT_PRICING symbol=%s reason=%s", sym, reason)
        return 2
    logging.info("NOTIFY_PIPELINE mode=real_intent")
    msg = format_intent_allow(
        {
            "symbol": sym,
            "side": side,
            "strategy": "REV_SWEPT_RSI",
            "confidence": 0.68,
            "reason": "Real formatter diagnostic path",
            "intent_id": f"REAL_FMT_{sym}",
            "meta": {},
        },
        {"reason": "allowed"},
        {
            "tf": str(getattr(config_module, "INTERVAL", "15")),
            "entry": display.get("entry"),
            "sl": display.get("sl"),
            "tp": display.get("tp"),
            "sl_pct": display.get("sl_pct"),
            "tp_pct": display.get("tp_pct"),
            "bar_ts_used": str(last.get("timestamp_utc", last.get("ts", "")) or ""),
            "open_now": 0,
            "open_max": int(getattr(config_module, "MAX_OPEN_POSITIONS", 0) or 0),
            "trades_today": 0,
            "cooldown_until_utc": "",
            "telegram_format": "compact",
            "telegram_max_chars_compact": int(getattr(config_module, "TELEGRAM_MAX_CHARS_COMPACT", 900)),
            "telegram_max_chars_verbose": int(getattr(config_module, "TELEGRAM_MAX_CHARS_VERBOSE", 2500)),
            "source": "real_intent",
        },
    )
    if not msg:
        return 2
    logging.info(
        "TELEGRAM_PAYLOAD symbol=%s side=%s strategy=%s entry=%s sl=%s tp=%s conf=%s source=real_intent",
        sym,
        side,
        "REV_SWEPT_RSI",
        str(display.get("entry")),
        str(display.get("sl")),
        str(display.get("tp")),
        "0.68",
    )
    print(msg)
    return 0


def run_reconcile(config_module, symbol: str) -> int:
    from bybit import fetch_klines
    from indicators_engine import precompute_tf_indicators
    from mtf import build_mtf_snapshot
    from scalper.settings import get_settings
    from scalper.strategies.liquidity_sweep_reversal import evaluate_last_bar as evaluate_lsr_last_bar
    from scalper.strategies.rev_swept_rsi import evaluate_reconcile as evaluate_rev_reconcile
    from signals import build_reconcile_report
    import pandas as pd

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
    # Reconcile extension for LSR strategy on 5m bars.
    lsr_lines: List[str] = []
    if candles_5m:
        try:
            indic_5m = precompute_tf_indicators(candles_5m)
            df_5m = pd.DataFrame(indic_5m)
            lsr = evaluate_lsr_last_bar(
                df_5m,
                {
                    "symbol": clean_symbol,
                    "tf": "5",
                    "settings": get_settings(),
                    "bar_ts_used": str(indic_5m.get("ts", [""])[-1] if indic_5m.get("ts") else ""),
                },
            )
            vals = dict(lsr.get("values", {}) or {})
            lsr_lines.append("LSR reconcile (5m):")
            lsr_lines.append(
                "  short_ok=%s long_ok=%s upper_wick=%.3f lower_wick=%.3f atr_pct=%.3f"
                % (
                    bool(lsr.get("ok_short", False)),
                    bool(lsr.get("ok_long", False)),
                    float(vals.get("upper_wick_ratio", 0.0) or 0.0),
                    float(vals.get("lower_wick_ratio", 0.0) or 0.0),
                    float(vals.get("atr_pct", 0.0) or 0.0),
                )
            )
            lsr_lines.append(
                "  prev_high=%.6f prev_low=%.6f ema200=%.6f slope=%.6f"
                % (
                    float(vals.get("prev_range_high", 0.0) or 0.0),
                    float(vals.get("prev_range_low", 0.0) or 0.0),
                    float(vals.get("ema200", 0.0) or 0.0),
                    float(vals.get("ema_slope", 0.0) or 0.0),
                )
            )
            lsr_lines.append("  short_reasons=" + ",".join(lsr.get("reasons_short", []) or ["none"]))
            lsr_lines.append("  long_reasons=" + ",".join(lsr.get("reasons_long", []) or ["none"]))
        except Exception as exc:
            lsr_lines.append(f"LSR reconcile unavailable: {exc}")
    else:
        lsr_lines.append("LSR reconcile unavailable: no 5m candles")
    if lsr_lines:
        report = f"{report}\n\n" + "\n".join(lsr_lines)
    # Reconcile extension for REV_SWEPT_RSI on 5m with 15m/1h context.
    rev_lines: List[str] = []
    if candles_5m:
        try:
            mtf_snap = build_mtf_snapshot(clean_symbol)
            mtf_snap = mtf_snap[0] if isinstance(mtf_snap, tuple) else mtf_snap
            indic_5m = precompute_tf_indicators(candles_5m)
            df_5m = pd.DataFrame(indic_5m)
            rev = evaluate_rev_reconcile(
                df_5m,
                {
                    "symbol": clean_symbol,
                    "tf": "5",
                    "settings": get_settings(),
                    "bar_ts_used": str(indic_5m.get("ts", [""])[-1] if indic_5m.get("ts") else ""),
                    "mtf_snapshot": mtf_snap or {},
                },
            )
            vals = dict(rev.get("values", {}) or {})
            rev_lines.append("REV_SWEPT_RSI reconcile (5m):")
            rev_lines.append(
                "  short_ok=%s long_ok=%s sweep_short=%s sweep_long=%s div_short=%s div_long=%s"
                % (
                    bool(rev.get("ok_short", False)),
                    bool(rev.get("ok_long", False)),
                    bool(vals.get("sweep_bear", False)),
                    bool(vals.get("sweep_bull", False)),
                    bool(vals.get("bear_div", False)),
                    bool(vals.get("bull_div", False)),
                )
            )
            rev_lines.append(
                "  ema200_dist=%.4f rsi=%.2f atr_pct_5m=%.4f entry_mode=%s"
                % (
                    float(vals.get("ema200_dist", 0.0) or 0.0),
                    float(vals.get("rsi", 0.0) or 0.0),
                    float(vals.get("atr_pct_5m", 0.0) or 0.0),
                    str(vals.get("entry_mode", "n/a") or "n/a"),
                )
            )
            rev_lines.append("  short_reasons=" + ",".join(rev.get("reasons_short", []) or ["none"]))
            rev_lines.append("  long_reasons=" + ",".join(rev.get("reasons_long", []) or ["none"]))
        except Exception as exc:
            rev_lines.append(f"REV_SWEPT_RSI reconcile unavailable: {exc}")
    else:
        rev_lines.append("REV_SWEPT_RSI reconcile unavailable: no 5m candles")
    if rev_lines:
        report = f"{report}\n\n" + "\n".join(rev_lines)
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
    from watchlist import get_watchlist

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
    dryrun_notify: bool = False,
    telegram_exit_alerts: bool = True,
) -> Dict[str, Any]:
    from bybit import fetch_klines
    from paper import PaperPosition, update_and_maybe_close
    from paper_engine import compute_entry_sl_tp_for_display, try_open_position
    from scalper.models import TradeRecord
    from scalper.risk_engine_core import RiskEngine
    from scalper.paper_broker import PaperBroker
    from scalper.settings import get_settings
    from signals import (
        evaluate_early_intents_from_5m,
        evaluate_symbol_intents,
    )
    from scalper.strategy_engine import StrategyEngine
    from scalper.strategies.registry import run_hybrid_strategies_for_symbol
    import storage as state_store
    from storage import (
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
    from telegram_format import (
        format_early_alert,
        format_exit_layer_event,
        format_intent_allow,
        format_intent_block,
        format_paper_open,
        format_paper_close,
    )

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
    hybrid_strategies_enabled = str(_cfg("STRATEGIES_ENABLED", "") or "").strip()
    hybrid_top_intents = int(_cfg("TOP_INTENTS_PER_SCAN", 3) or 3)
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

    # BTC regime filter: fetch BTCUSDT 5m volatility for signal reduction / breakout block.
    btc_atr_pct = 0.0
    try:
        btc_5m = fetch_klines(symbol="BTCUSDT", interval="5", limit=30)
        if btc_5m and len(btc_5m) >= 15:
            atr14_btc = _atr14_from_candles(btc_5m)
            last_close_btc = float((btc_5m[-1] or {}).get("close", 0) or 0)
            if last_close_btc > 0 and atr14_btc >= 0:
                btc_atr_pct = (atr14_btc / last_close_btc) * 100.0
                if btc_atr_pct > 1.8:
                    logging.warning("BTC_REGIME high_volatility")
    except Exception as _e:
        pass
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
    cycle_counts = {
        "total": 0,
        "blocked_after_candidate": 0,
        "blocked_by_vol": 0,
        "blocked_by_risk": 0,
        "no_candidates": 0,
    }

    set_defer_position_sync(True)
    for symbol in symbols_to_process:
        cycle_counts["total"] += 1
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
        decision_blocked_reason = "-"
        decision_intent = "NONE"
        decision_final_intent = "NONE"
        symbol_has_approved = False
        decision_candidate_count = 0
        decision_vol_pct: Optional[float] = None
        decision_atr_pct: Optional[float] = None
        decision_turnover_24h: Optional[float] = None
        decision_spread_bps: Optional[float] = None
        decision_candidates: List[str] = []
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
                    exit_params = {
                        "be_at_r": float(getattr(_config, "BE_AT_R", 0.0) or 0.0),
                        "partial_tp_at_r": float(getattr(_config, "PARTIAL_TP_AT_R", 0.0) or 0.0),
                        "partial_tp_pct": float(getattr(_config, "PARTIAL_TP_PCT", 0.0) or 0.0),
                        "time_stop_bars": int(getattr(_config, "TIME_STOP_BARS", 0) or 0),
                        "time_stop_min_r": float(getattr(_config, "TIME_STOP_MIN_R", 0.0) or 0.0),
                    }
                    updated, closed, pnl_usdt, close_reason, partial_trade = update_and_maybe_close(
                        position=position,
                        last_candle=latest_candle,
                        fees_bps=getattr(risk_autopilot, "paper_fees_bps", 6.0),
                        timeout_bars=getattr(risk_autopilot, "paper_timeout_bars", 12),
                        exit_params=exit_params,
                    )
                    intent_id = str(position.intent_id or "")
                    if intent_id and closed:
                        _EXIT_ALERTS_SENT.pop(intent_id, None)
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
                        if telegram_exit_alerts and telegram_token and telegram_chat_id:
                            sent = _EXIT_ALERTS_SENT.setdefault(intent_id, set())
                            if "PARTIAL_TP" not in sent:
                                sent.add("PARTIAL_TP")
                                evt_data = {
                                    "symbol": partial_trade.get("symbol", position.symbol),
                                    "side": partial_trade.get("side", position.side),
                                    "entry_price": partial_trade.get("entry_price"),
                                    "current_price": partial_trade.get("tp_price"),
                                    "reason": "Partial TP taken",
                                    "r_multiple": partial_trade.get("r_multiple"),
                                    "pnl_usdt": partial_trade.get("pnl_usdt"),
                                }
                                msg = format_exit_layer_event("PARTIAL_TP", evt_data, format_caps)
                                _send_telegram_with_logging(
                                    kind="intent",
                                    token=telegram_token,
                                    chat_id=telegram_chat_id,
                                    text=msg,
                                )
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
                        if telegram_exit_alerts and telegram_token and telegram_chat_id and close_reason == "TIME_STOP":
                            sent = _EXIT_ALERTS_SENT.setdefault(intent_id, set())
                            if "TIME_STOP" not in sent:
                                sent.add("TIME_STOP")
                                close_price = float(latest_candle.get("close", 0) or 0)
                                evt_data = {
                                    "symbol": updated.symbol,
                                    "side": updated.side,
                                    "entry_price": updated.entry_price,
                                    "current_price": close_price,
                                    "reason": "Time stop: bars_held >= threshold, R < min",
                                    "bars_held": updated.bars_held,
                                    "pnl_usdt": pnl_usdt,
                                }
                                msg = format_exit_layer_event("TIME_STOP", evt_data, format_caps)
                                _send_telegram_with_logging(
                                    kind="intent",
                                    token=telegram_token,
                                    chat_id=telegram_chat_id,
                                    text=msg,
                                )
                    else:
                        if telegram_exit_alerts and telegram_token and telegram_chat_id:
                            if updated.be_moved and not position.be_moved:
                                sent = _EXIT_ALERTS_SENT.setdefault(intent_id, set())
                                if "BE_MOVE" not in sent:
                                    sent.add("BE_MOVE")
                                    risk_per_unit = abs(position.entry_price - position.sl_price)
                                    mfp = updated.max_favorable_price or 0
                                    favorable_r = 0.0
                                    if risk_per_unit > 0:
                                        if position.side == "LONG":
                                            favorable_r = (mfp - position.entry_price) / risk_per_unit
                                        else:
                                            favorable_r = (position.entry_price - mfp) / risk_per_unit
                                    close_price = float(latest_candle.get("close", 0) or 0)
                                    evt_data = {
                                        "symbol": updated.symbol,
                                        "side": updated.side,
                                        "entry_price": updated.entry_price,
                                        "current_price": close_price,
                                        "reason": "SL moved to entry (BE)",
                                        "favorable_r": favorable_r,
                                    }
                                    msg = format_exit_layer_event("BE_MOVE", evt_data, format_caps)
                                    _send_telegram_with_logging(
                                        kind="intent",
                                        token=telegram_token,
                                        chat_id=telegram_chat_id,
                                        text=msg,
                                    )
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
                snap_15m = (mtf_snap or {}).get(15, {}) or {}
                bar_ts_v2 = str(
                    snap_15m.get("ts", "")
                    or (candles[-2].get("timestamp_utc", "") if len(candles) >= 2 else "")
                )
                # Hybrid strategy engine (DataFrame-based). When STRATEGIES_ENABLED is non-empty,
                # use the hybrid registry instead of the legacy v1/v2/v3 engine.
                if hybrid_strategies_enabled:
                    if symbol not in klines_5m_cache:
                        klines_5m_cache[symbol] = fetch_klines(
                            symbol=symbol,
                            interval=str(getattr(_config, "TF_TIMING", 5)),
                            limit=int(getattr(_config, "LOOKBACK_5M", 400)),
                        ) or []
                    evaluated = run_hybrid_strategies_for_symbol(
                        symbol=symbol,
                        candles_15m=candles or [],
                        candles_5m=klines_5m_cache.get(symbol) or [],
                        mtf_snapshot=mtf_snap or {},
                        bias_info=bias_info,
                        settings=settings_obj,
                        interval=str(interval),
                        bar_ts_used=bar_ts_v2,
                        strategies_enabled_raw=hybrid_strategies_enabled,
                        top_intents_per_scan=hybrid_top_intents,
                    )
                elif strategy_engine.has_enabled and (bias_info.get("bias") or "NONE") in ("LONG", "SHORT"):
                    if symbol not in klines_5m_cache:
                        klines_5m_cache[symbol] = fetch_klines(
                            symbol=symbol,
                            interval=str(getattr(_config, "TF_TIMING", 5)),
                            limit=int(getattr(_config, "LOOKBACK_5M", 400)),
                        ) or []
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
                    evaluated = evaluate_symbol_intents(
                        symbol=symbol,
                        candles=candles,
                        signal_debug=signal_debug,
                        early_min_conf=early_min_conf,
                        threshold_profile=active_profile,
                    )
                symbol_context["market_snapshot"] = dict(evaluated.get("market_snapshot", {}) or {})
                symbol_context["candidates_before"] = list(
                    evaluated.get("candidates_before", []) or []
                )
                decision_candidates = [
                    str(c.get("strategy", c.get("setup", "")) or "")
                    for c in (symbol_context.get("candidates_before", []) or [])
                    if isinstance(c, dict)
                ]
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

                # Candidate pipeline: strategies first, filters afterwards.
                snap = symbol_context.get("market_snapshot", {}) or {}
                vol_metrics = _compute_volatility_metrics(candles=candles or [], snapshot=snap, tf=str(interval))
                decision_atr_pct = vol_metrics.get("atr_pct")
                decision_vol_pct = vol_metrics.get("vol_pct")
                spread_bps_raw = snap.get("spread_bps")
                if spread_bps_raw is not None:
                    try:
                        decision_spread_bps = float(spread_bps_raw)
                    except (TypeError, ValueError):
                        decision_spread_bps = None
                elif decision_vol_pct is not None:
                    decision_spread_bps = float(decision_vol_pct) * 100.0
                if snap.get("turnover_24h") is not None:
                    try:
                        decision_turnover_24h = float(snap.get("turnover_24h"))
                    except (TypeError, ValueError):
                        decision_turnover_24h = None
                if not vol_metrics.get("data_ok"):
                    logging.warning(
                        "VOL_FILTER_DATA_MISSING symbol=%s tf=%s n_bars=%d last_ts=%s reason=%s",
                        symbol,
                        str(interval),
                        int(vol_metrics.get("n_bars", 0) or 0),
                        str(vol_metrics.get("last_ts", "") or ""),
                        str(vol_metrics.get("reason", "") or ""),
                    )
                else:
                    _self_check_volatility_metrics(vol_metrics)

                detected = list(evaluated.get("final_intents", []) or [])
                decision_candidate_count = len(detected)
                if not detected:
                    cycle_counts["no_candidates"] += 1
                    decision_blocked_reason = "NO_CANDIDATES"
                    logging.info("NO_CANDIDATES symbol=%s", symbol)
                blocked_reasons_local: Dict[str, int] = {}
                for signal in detected:
                    append_signal(signal)
                    intent_symbol = symbol
                    intent_side = str(signal.get("side", signal.get("direction", "")))
                    if decision_intent == "NONE" and intent_side:
                        decision_intent = str(intent_side).upper()
                    intent_strategy = str(signal.get("strategy", signal.get("setup", "")))
                    prefilter_reason = _candidate_prefilter_reason(
                        side=intent_side,
                        interval=str(interval),
                        vol_metrics=vol_metrics,
                        snapshot=snap,
                        bias_info=bias_info,
                        config_module=_config,
                    )
                    if not decision_candidates:
                        decision_candidates = [intent_strategy]
                    elif intent_strategy not in decision_candidates:
                        decision_candidates.append(intent_strategy)
                    intent_reason = str(signal.get("reason", ""))
                    intent_confidence = float(signal.get("confidence", 0.0))
                    intent_ts = str(signal.get("ts", signal.get("timestamp_utc", datetime.now(timezone.utc).isoformat())))
                    logging.info(
                        "CANDIDATE symbol=%s strategy=%s side=%s conf=%.3f entry=%s sl=%s tp=%s prefilter_block_reason=%s",
                        intent_symbol,
                        intent_strategy,
                        intent_side,
                        float(intent_confidence),
                        str(signal.get("entry", signal.get("close", ""))),
                        str(signal.get("sl", "")),
                        str(signal.get("tp", "")),
                        prefilter_reason or "-",
                    )
                    if prefilter_reason:
                        cycle_counts["blocked_after_candidate"] += 1
                        if prefilter_reason in {"VOL_FILTER", "VOL_DATA_MISSING"}:
                            cycle_counts["blocked_by_vol"] += 1
                        blocked_reasons_local[prefilter_reason] = blocked_reasons_local.get(prefilter_reason, 0) + 1
                        if decision_blocked_reason in ("-", "", "NO_CANDIDATES"):
                            decision_blocked_reason = prefilter_reason
                        logging.info(
                            "CANDIDATE_BLOCKED symbol=%s strategy=%s reason=%s",
                            intent_symbol,
                            intent_strategy,
                            prefilter_reason,
                        )
                        symbol_context["final_intents"].append(
                            {
                                "symbol": intent_symbol,
                                "side": intent_side,
                                "strategy": intent_strategy,
                                "reason": intent_reason,
                                "confidence": intent_confidence,
                                "ts": intent_ts,
                                "bar_ts_used": str(signal.get("bar_ts_used", "")),
                                "intent_id": str(signal.get("intent_id", "")),
                                "risk": {"allowed": False, "reason": prefilter_reason},
                            }
                        )
                        continue
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
                    profile = str((signal.get("meta") or {}).get("profile", "") or signal.get("profile", "") or "").strip()
                    fp = _intent_fingerprint(
                        intent_symbol,
                        intent_strategy,
                        intent_side,
                        bar_ts,
                        profile=profile,
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
                        },
                        snapshot=risk_snapshot,
                    )
                    allowed = bool(verdict.allowed)
                    gate_reason = str(verdict.reason or "")
                    if allowed and btc_atr_pct > 2.5 and _is_breakout_strategy(intent_strategy):
                        allowed = False
                        gate_reason = "BTC_REGIME_BREAKOUT_BLOCKED"
                    meta = dict(signal.get("meta") or {})
                    if "retest_level" not in meta and "failed_level" not in meta:
                        meta["retest_level"] = signal.get("level_ref")
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
                        "entry": signal.get("entry"),
                        "sl": signal.get("sl", signal.get("sl_hint")),
                        "tp": signal.get("tp", signal.get("tp_hint")),
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
                        symbol_has_approved = True
                        decision_final_intent = str(intent_side or "NONE").upper()
                        decision_blocked_reason = "-"
                        logging.info(
                            "CANDIDATE_APPROVED symbol=%s strategy=%s",
                            intent_symbol,
                            intent_strategy,
                        )
                        logging.info(
                            "INTENT_APPROVED symbol=%s strategy=%s side=%s conf=%.3f entry=%s sl=%s tp=%s",
                            intent_symbol,
                            intent_strategy,
                            str(intent_side).upper(),
                            float(intent_confidence),
                            str(signal.get("entry", signal.get("close", ""))),
                            str(signal.get("sl", signal.get("sl_hint", ""))),
                            str(signal.get("tp", signal.get("tp_hint", ""))),
                        )
                        _pending_open_msg: Optional[str] = None
                        snap = symbol_context.get("market_snapshot", {}) or {}
                        if paper_broker is not None:
                            pos_dict, skip_reason = paper_broker.open_from_intent(
                                intent=trade_intent,
                                candle=(candles[-1] if candles else {}),
                                strategy=intent_strategy,
                                fallback_atr=float(snap.get("atr14", 0.0) or 0.0),
                                intent_id=str(signal.get("intent_id", "")),
                                ts=str((candles[-1] if candles else {}).get("timestamp_utc", intent_ts)),
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
                            )
                        opened_position = (
                            PaperPosition.from_dict(pos_dict) if pos_dict else None
                        )
                        if pos_dict:
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
                                        "sl": opened_position.sl_price,
                                        "tp": opened_position.tp_price,
                                        "sl_pct": (
                                            abs(
                                                opened_position.entry_price
                                                - opened_position.sl_price
                                            )
                                            / max(
                                                opened_position.entry_price,
                                                1e-10,
                                            )
                                            * 100.0
                                        ),
                                        "tp_pct": (
                                            abs(
                                                opened_position.tp_price
                                                - opened_position.entry_price
                                            )
                                            / max(
                                                opened_position.entry_price,
                                                1e-10,
                                            )
                                            * 100.0
                                        ),
                                        "qty": opened_position.qty_est,
                                        "notional": opened_position.notional_usdt,
                                        "bar_ts_used": bar_ts_used,
                                        "intent_id": str(
                                            signal.get("intent_id", "")
                                        ),
                                        "risk_reason": gate_reason,
                                        "note": intent_reason,
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
                                _pending_open_msg = open_msg
                            else:
                                _warn_missing_telegram_once()
                        policy_now = str(getattr(_config, "TELEGRAM_POLICY", "events") or "events")
                        budget_now = int(getattr(_config, "TELEGRAM_DAILY_BUDGET", 0) or 0)
                        notify_decision = _approved_intent_notify_decision(
                            telegram_token=telegram_token,
                            telegram_chat_id=telegram_chat_id,
                            policy=policy_now,
                            budget=budget_now,
                            paper_mode=paper_mode,
                            dryrun_notify=dryrun_notify,
                            always_notify_intents=always_notify_intents,
                        )
                        logging.info("NOTIFY_PIPELINE mode=real_intent")
                        if telegram_token and telegram_chat_id:
                            meta = dict(signal.get("meta") or {})
                            profile = str(meta.get("profile", "") or signal.get("profile", "") or "").strip()
                            _allow_ctx_entry = _allow_ctx_sl = _allow_ctx_tp = None
                            _allow_ctx_sl_pct = _allow_ctx_tp_pct = None
                            _allow_ctx_qty = _allow_ctx_notional = None
                            if opened_position:
                                _allow_ctx_entry = opened_position.entry_price
                                _allow_ctx_sl = opened_position.sl_price
                                _allow_ctx_tp = opened_position.tp_price
                                _allow_ctx_sl_pct = (
                                    abs(opened_position.entry_price - opened_position.sl_price)
                                    / max(opened_position.entry_price, 1e-10) * 100.0
                                )
                                _allow_ctx_tp_pct = (
                                    abs(opened_position.tp_price - opened_position.entry_price)
                                    / max(opened_position.entry_price, 1e-10) * 100.0
                                )
                                _allow_ctx_qty = opened_position.qty_est
                                _allow_ctx_notional = opened_position.notional_usdt
                            else:
                                _display = compute_entry_sl_tp_for_display(
                                    {**trade_intent, "side": intent_side},
                                    candles,
                                    snap,
                                    sl_atr_mult=float(
                                        getattr(risk_autopilot, "paper_sl_atr", 1.0) or 1.0
                                    ),
                                    tp_atr_mult=float(
                                        getattr(risk_autopilot, "paper_tp_atr", 1.5) or 1.5
                                    ),
                                )
                                if _display:
                                    _allow_ctx_entry = _display["entry"]
                                    _allow_ctx_sl = _display["sl"]
                                    _allow_ctx_tp = _display["tp"]
                                    _allow_ctx_sl_pct = _display["sl_pct"]
                                    _allow_ctx_tp_pct = _display["tp_pct"]
                            approved_entry = _allow_ctx_entry
                            approved_sl = _allow_ctx_sl
                            approved_tp = _allow_ctx_tp
                            notifier_entry = _allow_ctx_entry
                            notifier_sl = _allow_ctx_sl
                            notifier_tp = _allow_ctx_tp
                            formatter_entry = _allow_ctx_entry
                            formatter_sl = _allow_ctx_sl
                            formatter_tp = _allow_ctx_tp
                            logging.info(
                                "INTENT_PRICE_LINEAGE symbol=%s strategy=%s side=%s candidate_entry=%s candidate_sl=%s candidate_tp=%s approved_entry=%s approved_sl=%s approved_tp=%s notifier_entry=%s notifier_sl=%s notifier_tp=%s formatter_entry=%s formatter_sl=%s formatter_tp=%s",
                                intent_symbol,
                                intent_strategy,
                                str(intent_side).upper(),
                                str(signal.get("entry", signal.get("close", ""))),
                                str(signal.get("sl", signal.get("sl_hint", ""))),
                                str(signal.get("tp", signal.get("tp_hint", ""))),
                                str(approved_entry),
                                str(approved_sl),
                                str(approved_tp),
                                str(notifier_entry),
                                str(notifier_sl),
                                str(notifier_tp),
                                str(formatter_entry),
                                str(formatter_sl),
                                str(formatter_tp),
                            )
                            market_px_ref = float(
                                snap.get("last_close", (candles[-1] if candles else {}).get("close", 0.0) or 0.0)
                                or 0.0
                            )
                            pricing_ok, pricing_reason = _validate_real_intent_pricing(
                                symbol=intent_symbol,
                                entry=approved_entry,
                                sl=approved_sl,
                                tp=approved_tp,
                                market_px=market_px_ref,
                            )
                            if not pricing_ok:
                                logging.error(
                                    "INVALID_APPROVED_INTENT_PRICING symbol=%s reason=%s",
                                    intent_symbol,
                                    pricing_reason,
                                )
                                notify_decision["should_notify"] = False
                                notify_decision["notify_reason"] = "INVALID_APPROVED_INTENT_PRICING"
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
                                    "profile": profile,
                                    "meta": meta,
                                },
                                {"reason": gate_reason},
                                {
                                    "tf": str(interval),
                                    "profile": profile,
                                    "conf_raw": meta.get("conf_raw"),
                                    "conf_hq": meta.get("conf_hq"),
                                    "entry": _allow_ctx_entry,
                                    "sl": _allow_ctx_sl,
                                    "tp": _allow_ctx_tp,
                                    "sl_pct": _allow_ctx_sl_pct,
                                    "tp_pct": _allow_ctx_tp_pct,
                                    "qty": _allow_ctx_qty,
                                    "notional": _allow_ctx_notional,
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
                                    "source": "real_intent",
                                    **format_caps,
                                },
                            )
                            if not msg:
                                notify_decision["should_notify"] = False
                                notify_decision["notify_reason"] = "FORMATTER_MISSING_PRICE_FIELDS"
                            logging.info(
                                "APPROVED_INTENT_DECISION symbol=%s should_notify=%s notify_reason=%s policy=%s budget=%s paper_mode=%s dryrun_notify=%s",
                                intent_symbol,
                                bool(notify_decision.get("should_notify", False)),
                                str(notify_decision.get("notify_reason", "DISABLED")),
                                str(notify_decision.get("policy", "events")),
                                int(notify_decision.get("budget", 0) or 0),
                                bool(notify_decision.get("paper_mode", False)),
                                bool(notify_decision.get("dryrun_notify", False)),
                            )
                            if always_notify_intents:
                                msg = f"[ALLOW] {msg}"
                            score = _compute_signal_score(signal, snap, candles, atr_pct)
                            logging.info("SIGNAL_RANK %s %s", score, symbol)
                            _dispatch_intent_notification(
                                symbol=intent_symbol,
                                strategy=intent_strategy,
                                side=str(intent_side).upper(),
                                kind="intent",
                                text=msg,
                                entry=_allow_ctx_entry,
                                sl=_allow_ctx_sl,
                                tp=_allow_ctx_tp,
                                conf=float(intent_confidence),
                                source="real_intent",
                                telegram_token=telegram_token,
                                telegram_chat_id=telegram_chat_id,
                                should_notify=bool(notify_decision.get("should_notify", False)),
                                notify_reason=str(notify_decision.get("notify_reason", "DISABLED")),
                            )
                            if _pending_open_msg:
                                _dispatch_intent_notification(
                                    symbol=intent_symbol,
                                    strategy=intent_strategy,
                                    side=str(intent_side).upper(),
                                    kind="intent",
                                    text=_pending_open_msg,
                                    entry=_allow_ctx_entry,
                                    sl=_allow_ctx_sl,
                                    tp=_allow_ctx_tp,
                                    conf=float(intent_confidence),
                                    source="real_intent",
                                    telegram_token=telegram_token,
                                    telegram_chat_id=telegram_chat_id,
                                    should_notify=bool(notify_decision.get("should_notify", False)),
                                    notify_reason=str(notify_decision.get("notify_reason", "DISABLED")),
                                )
                        else:
                            logging.info(
                                "APPROVED_INTENT_DECISION symbol=%s should_notify=%s notify_reason=%s policy=%s budget=%s paper_mode=%s dryrun_notify=%s",
                                intent_symbol,
                                bool(notify_decision.get("should_notify", False)),
                                str(notify_decision.get("notify_reason", "DISABLED")),
                                str(notify_decision.get("policy", "events")),
                                int(notify_decision.get("budget", 0) or 0),
                                bool(notify_decision.get("paper_mode", False)),
                                bool(notify_decision.get("dryrun_notify", False)),
                            )
                            _dispatch_intent_notification(
                                symbol=intent_symbol,
                                strategy=intent_strategy,
                                side=str(intent_side).upper(),
                                kind="intent",
                                text="",
                                entry=signal.get("entry", signal.get("close", "")),
                                sl=signal.get("sl", signal.get("sl_hint", "")),
                                tp=signal.get("tp", signal.get("tp_hint", "")),
                                conf=float(intent_confidence),
                                source="real_intent",
                                telegram_token=telegram_token,
                                telegram_chat_id=telegram_chat_id,
                                should_notify=bool(notify_decision.get("should_notify", False)),
                                notify_reason=str(notify_decision.get("notify_reason", "DISABLED")),
                            )
                    else:
                        logging.warning(
                            "Risk gate blocked intent (%s | %s | bar_ts_used=%s): %s",
                            intent_symbol,
                            intent_strategy,
                            bar_ts_used,
                            gate_reason,
                        )
                        logging.info(
                            "BLOCKED reason=%s symbol=%s strategy=%s",
                            gate_reason,
                            intent_symbol,
                            intent_strategy,
                        )
                        cycle_counts["blocked_by_risk"] += 1
                        cycle_counts["blocked_after_candidate"] += 1
                        decision_blocked_reason = str(gate_reason or "RISK_BLOCK")
                        blocked_reasons_local["RISK_FILTER"] = blocked_reasons_local.get("RISK_FILTER", 0) + 1
                        logging.info(
                            "CANDIDATE_BLOCKED symbol=%s strategy=%s reason=RISK_FILTER",
                            intent_symbol,
                            intent_strategy,
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
                if not symbol_has_approved and decision_candidate_count > 0 and blocked_reasons_local:
                    reasons_txt = ",".join(f"{k}:{v}" for k, v in sorted(blocked_reasons_local.items()))
                    logging.info(
                        "TOP_CANDIDATES_BLOCKED count=%d reasons={%s}",
                        int(decision_candidate_count),
                        reasons_txt,
                    )
        except Exception as exc:
            symbol_context["error"] = str(exc)
            logging.exception("Scan failed for %s: %s", symbol, exc)
        finally:
            if decision_intent == "NONE":
                # Try to infer from final intents if available.
                finals_for_symbol = list(symbol_context.get("final_intents", []) or [])
                if finals_for_symbol:
                    decision_intent = str(finals_for_symbol[0].get("side", "NONE") or "NONE").upper()
            logging.info(
                "SYMBOL_DECISION symbol=%s tf=%sm candidate_count=%d final_intent=%s blocked_reason=%s vol_pct=%s atr_pct_15m=%s turnover_24h=%s spread_bps=%s strategy_candidates=%s",
                symbol,
                str(interval),
                int(decision_candidate_count),
                decision_final_intent or "NONE",
                str(decision_blocked_reason or "-"),
                ("%.4f" % float(decision_vol_pct)) if decision_vol_pct is not None else "n/a",
                ("%.4f" % float(decision_atr_pct)) if decision_atr_pct is not None else "n/a",
                ("%.2f" % float(decision_turnover_24h)) if decision_turnover_24h is not None else "n/a",
                ("%.2f" % float(decision_spread_bps)) if decision_spread_bps is not None else "n/a",
                "[" + ",".join([c for c in decision_candidates if c]) + "]" if decision_candidates else "[]",
            )
            run_context["symbols"].append(symbol_context)

    if always_notify_intents and not has_any_allow:
        blocked_summary = (
            f"SCAN_BLOCKED_SUMMARY total={int(cycle_counts.get('total', 0))} "
            f"blocked_after_candidate={int(cycle_counts.get('blocked_after_candidate', 0))} "
            f"blocked_by_vol={int(cycle_counts.get('blocked_by_vol', 0))} "
            f"blocked_by_risk={int(cycle_counts.get('blocked_by_risk', 0))} "
            f"no_candidates={int(cycle_counts.get('no_candidates', 0))}"
        )
        logging.info(blocked_summary)
        if telegram_token and telegram_chat_id:
            _send_telegram_with_logging(
                kind="intent",
                token=telegram_token,
                chat_id=telegram_chat_id,
                text=blocked_summary,
            )
        else:
            _warn_missing_telegram_once()

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
    Send scan summary or heartbeat per TELEGRAM_POLICY.
    - events: no scan_summary, heartbeat when idle
    - periodic: scan_summary at most once per SCAN_SUMMARY_MINUTES (only when NOTIFY_SCAN_SUMMARY and not DISABLE_SCAN_SUMMARY)
    - off: no scan_summary, no heartbeat
    Scan summary requires ALL: TELEGRAM_POLICY==periodic, NOTIFY_SCAN_SUMMARY, DISABLE_SCAN_SUMMARY==False.
    Heartbeat: not sent on startup (requires at least 2 completed scans), includes run_mode, watchlist count, last_scan_ts.
    """
    from scalper.notifier import get_last_telegram_sent_at

    global _LAST_SCAN_SUMMARY_AT, _SCANS_COMPLETED
    _SCANS_COMPLETED += 1
    if not telegram_token or not telegram_chat_id:
        return
    policy = str(getattr(config_module, "TELEGRAM_POLICY", "events") or "events").strip().lower()
    if policy not in {"events", "periodic", "off"}:
        policy = "events"
    notify_summary = bool(getattr(config_module, "NOTIFY_SCAN_SUMMARY", False))
    disable_summary = bool(getattr(config_module, "DISABLE_SCAN_SUMMARY", True))
    heartbeat_min = int(getattr(config_module, "HEARTBEAT_MINUTES", 10) or 10)
    summary_min = int(getattr(config_module, "SCAN_SUMMARY_MINUTES", 30) or 30)
    now = time.time()
    idle_sec = now - get_last_telegram_sent_at()

    if policy == "off":
        return

    may_send_scan_summary = (
        policy == "periodic"
        and notify_summary
        and not disable_summary
    )
    if policy == "periodic" and not may_send_scan_summary:
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
        if policy == "periodic":
            return

    threshold_sec = heartbeat_min * 60
    may_send_heartbeat = (
        policy in ("events", "periodic")
        and heartbeat_min > 0
        and idle_sec >= threshold_sec
        and _SCANS_COMPLETED >= 2
    )
    if not may_send_heartbeat and policy in ("events", "periodic") and heartbeat_min > 0:
        logging.info(
            "HEARTBEAT_SKIP elapsed=%.0f threshold=%.0f scans=%d",
            idle_sec,
            threshold_sec,
            _SCANS_COMPLETED,
        )
    if may_send_heartbeat:
        from storage import get_last_scan_ts

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
    from dashboard import build_dashboard_report
    from storage import load_paper_state, get_paper_performance_summary, append_paper_performance_row
    from telegram_format import format_dashboard_compact

    ctx = dict(run_context)
    ctx["top_n"] = config_module.DASHBOARD_TOP_N
    ctx["include_blocked"] = config_module.DASHBOARD_INCLUDE_BLOCKED
    ctx["include_market_snapshot"] = config_module.DASHBOARD_INCLUDE_MARKET_SNAPSHOT
    ctx["include_debug_why_none"] = bool(config_module.DASHBOARD_INCLUDE_DEBUG_WHY_NONE)
    ctx["max_open_positions"] = max_open_positions
    paper_state = load_paper_state()
    ctx["paper_state"] = paper_state
    paper_performance_summary = get_paper_performance_summary(paper_state)
    ctx["paper_performance_summary"] = paper_performance_summary

    report = build_dashboard_report(ctx)
    logging.info("\n%s", report)
    scan_ts = run_context.get("ts") or None
    append_paper_performance_row(
        paper_performance_summary,
        scan_ts_utc=str(scan_ts) if scan_ts else None,
    )

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
    from storage import set_last_scan_started_ts, set_last_scan_ts

    set_last_scan_started_ts(int(time.time()))
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
        dryrun_notify=bool(dryrun_notify_always),
        telegram_exit_alerts=bool(getattr(config_module, "TELEGRAM_EXIT_ALERTS", True)),
    )
    from storage import set_selected_watchlist, set_stall_alerted

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
    """If stalled (no completed scan for threshold), send Telegram alert once per episode.
    Success = completed scan cycle (not telegram). Threshold default: 2*SCAN_SECONDS+60."""
    if not telegram_token or not telegram_chat_id:
        return
    from storage import (
        get_last_scan_error,
        get_last_scan_started_ts,
        get_last_scan_ts,
        get_stall_alerted,
        set_stall_alerted,
    )

    scan_sec = max(1, int(getattr(config_module, "SCAN_SECONDS", 60)))
    configured = int(getattr(config_module, "STALL_THRESHOLD_SECONDS", 0) or 0)
    stall_threshold = configured if configured > 0 else (2 * scan_sec + 60)
    last_completed = get_last_scan_ts() or 0
    if last_completed <= 0:
        return
    now = int(time.time())
    if now - last_completed <= stall_threshold:
        return
    if get_stall_alerted():
        return
    last_error = get_last_scan_error()
    last_started = get_last_scan_started_ts()

    def _ts_str(ts: Optional[int]) -> str:
        if not ts:
            return "-"
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except (TypeError, ValueError, OSError):
            return str(ts)

    msg = (
        f"STALL DETECTED | last_scan_started={_ts_str(last_started)} | "
        f"last_scan_completed={_ts_str(last_completed)} | last_error={last_error}"
    )
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
    from storage import set_last_scan_error

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


def _apply_debug_relax_filters(config_module) -> None:
    """
    Debug mode for easier signal diagnostics without editing .env.
    Keeps values bounded/safe but loosens strict filters.
    """
    enabled = bool(getattr(config_module, "DEBUG_RELAX_FILTERS", False))
    if not enabled:
        return
    # Preserve old values for observability.
    old = {
        "WATCHLIST_MIN_TURNOVER_24H": float(getattr(config_module, "WATCHLIST_MIN_TURNOVER_24H", 0.0) or 0.0),
        "MIN_TURNOVER_USDT": float(getattr(config_module, "MIN_TURNOVER_USDT", 0.0) or 0.0),
        "MIN_VOL_PCT": float(getattr(config_module, "MIN_VOL_PCT", 0.0) or 0.0),
        "MAX_VOL_PCT": float(getattr(config_module, "MAX_VOL_PCT", 0.0) or 0.0),
        "MIN_ATR_PCT": float(getattr(config_module, "MIN_ATR_PCT", 0.0) or 0.0),
    }
    # Relax thresholds.
    config_module.WATCHLIST_MIN_TURNOVER_24H = min(old["WATCHLIST_MIN_TURNOVER_24H"], 5_000_000.0) if old["WATCHLIST_MIN_TURNOVER_24H"] > 0 else 5_000_000.0
    config_module.MIN_TURNOVER_USDT = min(old["MIN_TURNOVER_USDT"], 5_000_000.0) if old["MIN_TURNOVER_USDT"] > 0 else 5_000_000.0
    config_module.MIN_VOL_PCT = min(old["MIN_VOL_PCT"], 0.2)
    config_module.MAX_VOL_PCT = max(old["MAX_VOL_PCT"], 20.0)
    config_module.MIN_ATR_PCT = min(old["MIN_ATR_PCT"], 0.001)
    logging.warning(
        "DEBUG_RELAX_FILTERS=ON effective thresholds: min_turnover_24h=%.0f min_turnover_usdt=%.0f min_vol_pct=%.3f max_vol_pct=%.3f min_atr_pct=%.6f",
        float(config_module.WATCHLIST_MIN_TURNOVER_24H),
        float(config_module.MIN_TURNOVER_USDT),
        float(config_module.MIN_VOL_PCT),
        float(config_module.MAX_VOL_PCT),
        float(config_module.MIN_ATR_PCT),
    )


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
    _apply_debug_relax_filters(config)
    logging.info("PIPELINE_MODE=strategies_first_filters_after")

    if getattr(args, "enable_scan_summary", False):
        config.DISABLE_SCAN_SUMMARY = False
        config.NOTIFY_SCAN_SUMMARY = True

    if args.log_level.upper() == "DEBUG":
        config.debug_env(logger)
        config.debug_risk_config(logger)
        logging.debug(
            "UNIVERSE_MODE_set=%s",
            bool(os.getenv("UNIVERSE_MODE") or getattr(config, "UNIVERSE_MODE", "")),
        )

    symbols_override = _parse_symbols_override(getattr(args, "symbols", ""))
    if symbols_override:
        logging.info("CLI symbols override active: %s", ",".join(symbols_override))
    paper_mode = bool(getattr(args, "paper", False))
    dryrun_notify = bool(getattr(args, "dryrun_notify_always", False))
    always_notify_intents = bool(dryrun_notify or getattr(config, "ALWAYS_NOTIFY_INTENTS", False))
    if paper_mode:
        logging.info("PAPER mode enabled (--paper). No exchange private endpoints are used.")
    logging.info(
        "NOTIFY_CONFIG telegram_enabled=%s policy=%s budget=%s paper_mode=%s dryrun_notify=%s always_notify_intents=%s disable_scan_summary=%s notify_scan_summary=%s",
        bool(str(getattr(config, "TELEGRAM_BOT_TOKEN", "") or "").strip())
        and bool(str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").strip()),
        str(getattr(config, "TELEGRAM_POLICY", "events") or "events"),
        int(getattr(config, "TELEGRAM_DAILY_BUDGET", 0) or 0),
        bool(paper_mode),
        bool(dryrun_notify),
        bool(always_notify_intents),
        bool(getattr(config, "DISABLE_SCAN_SUMMARY", True)),
        bool(getattr(config, "NOTIFY_SCAN_SUMMARY", False)),
    )

    if args.cooldown_minutes <= 0:
        logging.error("--cooldown-minutes must be a positive integer.")
        return 2
    if args.force_intents < 0:
        logging.error("--force-intents must be >= 0.")
        return 2
    if args.test_telegram_formats:
        return run_test_telegram_formats(config)
    if args.doctor:
        return run_doctor(config)
    if args.test_intent_notify:
        return run_test_intent_notify(
            config,
            symbol=str(args.test_intent_notify[0]),
            side=str(args.test_intent_notify[1]),
            paper_mode=paper_mode,
            dryrun_notify=dryrun_notify,
            always_notify_intents=always_notify_intents,
        )
    if str(getattr(args, "test_real_intent_format", "") or "").strip():
        return run_test_real_intent_format(config, str(args.test_real_intent_format))
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
        from scalper.notifier import get_last_telegram_status

        ok = _send_telegram_with_logging(
            kind="test",
            token=config.TELEGRAM_BOT_TOKEN,
            chat_id=config.TELEGRAM_CHAT_ID,
            text="Telegram OK (test)",
            strict=True,
        )
        if ok:
            logging.info("Telegram test message sent. Exiting --test-telegram mode.")
            return 0
        status = get_last_telegram_status()
        if status == "POLICY_SKIP":
            logging.warning("Telegram test skipped by policy.")
            return 0
        logging.error("Telegram test failed due to send failure. Check token/chat_id/network.")
        return 1

    if config.WATCHLIST_MODE == "static" and not config.WATCHLIST:
        logging.error("WATCHLIST is empty. Set WATCHLIST in .env for static mode.")
        return 2

    risk_autopilot = build_risk_autopilot(config)

    # Resolve watchlist once for startup log and --once path
    watchlist, watchlist_mode = resolve_watchlist(config, symbols_override=symbols_override)
    if not watchlist:
        logging.error("Resolved watchlist is empty. Check WATCHLIST or WATCHLIST_MODE settings.")
        return 2

    trans = {}
    try:
        from storage import get_watchlist_transparency

        trans = get_watchlist_transparency() or {}
    except Exception:
        pass
    universe_n = trans.get("watchlist_universe_size") or getattr(config, "WATCHLIST_UNIVERSE_N", None) or len(config.WATCHLIST or [])
    batch_n = trans.get("watchlist_batch_size") or getattr(config, "WATCHLIST_BATCH_N", None) or len(watchlist)
    offset = trans.get("watchlist_rotation_offset")
    if offset is None:
        try:
            from storage import get_watchlist_rotation_offset

            offset = get_watchlist_rotation_offset()
        except Exception:
            offset = 0
    universe_mode = str(getattr(config, "WATCHLIST_MODE", None) or getattr(config, "UNIVERSE_MODE", None) or "static")
    logging.info(
        "Startup mode_once=%s mode_loop=%s mode_paper=%s tf=%s universe_mode=%s universe_n=%s batch_n=%s offset=%s cwd=%s",
        bool(getattr(args, "once", False)),
        bool(getattr(args, "loop", False)),
        paper_mode,
        str(getattr(config, "INTERVAL", "15")),
        universe_mode,
        universe_n,
        batch_n,
        offset,
        os.getcwd(),
    )
    logging.info(
        "WATCHLIST mode=%s universe_n=%s batch_n=%s effective=%s offset=%s source=%s",
        watchlist_mode,
        universe_n,
        batch_n,
        len(watchlist),
        offset,
        str(trans.get("watchlist_source", "unknown")),
    )

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
            from storage import set_last_scan_ts, set_selected_watchlist, set_stall_alerted

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
            logging.info("Completed FORCE_TEST run. Exiting.")
            return 0
        # watchlist already resolved above (non-empty)
        from storage import set_last_scan_started_ts

        set_last_scan_started_ts(int(time.time()))
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
            always_notify_intents=bool(always_notify_intents),
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
            dryrun_notify=bool(dryrun_notify),
            telegram_exit_alerts=bool(getattr(config, "TELEGRAM_EXIT_ALERTS", True)),
        )
        from storage import set_last_scan_ts, set_selected_watchlist, set_stall_alerted

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
        logging.info("Completed one scan cycle. Exiting.")
        return 0

    if args.force_intents > 0:
        logging.warning("--force-intents is only applied with --once; ignoring in loop mode.")

    from storage import set_last_scan_error, set_last_scan_ts, set_selected_watchlist, set_stall_alerted

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
                logging.info("LOOP_SLEEP seconds=%d", scan_seconds)
                time.sleep(scan_seconds)
                continue
            from storage import set_last_scan_started_ts

            set_last_scan_started_ts(int(time.time()))
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
                    always_notify_intents=bool(always_notify_intents),
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
                    dryrun_notify=bool(dryrun_notify),
                    telegram_exit_alerts=bool(getattr(config, "TELEGRAM_EXIT_ALERTS", True)),
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
            logging.info("LOOP_SLEEP seconds=%d", scan_seconds)
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


if __name__ == "__main__":
    raise SystemExit(main())

