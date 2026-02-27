from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from scalper.repositories import get_storage_repo

sqlite_store = get_storage_repo()

_defer_position_sync = threading.local()


def set_defer_position_sync(defer: bool) -> None:
    """When True, save_paper_state skips sync_positions_and_fills. Use to batch sync at end of scan cycle."""
    _defer_position_sync.value = defer


def _is_defer_position_sync() -> bool:
    return bool(getattr(_defer_position_sync, "value", False))



def _default_paper_state() -> Dict[str, Any]:
    day_utc = datetime.now(timezone.utc).date().isoformat()
    return {
        "day_utc": day_utc,
        "trade_count_today": 0,
        "daily_pnl_realized": 0.0,
        "daily_pnl_sim": 0.0,
        "equity_peak": 0.0,
        "consecutive_losses": 0,
        "cooldown_until_ts": 0,
        "pause_until_ts": 0,
        "pause_reason": "",
        "kill_reason": "",
        "last_trade_ts": 0,
        "last_trade_symbol_ts": {},
        "cooldown_until_utc": "",
        "open_positions": [],
        "closed_trades": [],
        "intent_fingerprints": [],
        "early_alert_keys": [],
        "last_scan_ts": None,
        "last_block_reason": "-",
        "signal_hashes": [],
        "trade_intents": [],
        "risk_events": [],
        "risk_metrics": {
            "day_utc": day_utc,
            "trade_count_today": 0,
            "daily_pnl_realized": 0.0,
            "daily_pnl_sim": 0.0,
            "equity_peak": 0.0,
            "consecutive_losses": 0,
            "cooldown_until_ts": 0,
            "pause_until_ts": 0,
            "pause_reason": "",
            "kill_reason": "",
            "cooldown_until_utc": "",
        },
    }



def _normalize_paper_state(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _default_paper_state()
    normalized = {**defaults, **(raw_state or {})}

    risk_blob = normalized.get("risk")
    if isinstance(risk_blob, dict):
        for key in (
            "day_utc",
            "state_date",
            "trade_count_today",
            "daily_pnl_sim",
            "consecutive_losses",
            "cooldown_until_utc",
        ):
            if key in risk_blob and key not in normalized:
                normalized[key] = risk_blob.get(key)

    day_utc = normalized.get("day_utc", normalized.get("state_date", defaults["day_utc"]))
    normalized["day_utc"] = str(day_utc)
    normalized.pop("state_date", None)

    normalized["trade_count_today"] = int(normalized.get("trade_count_today", 0) or 0)
    normalized["daily_pnl_realized"] = float(normalized.get("daily_pnl_realized", 0.0) or 0.0)
    normalized["daily_pnl_sim"] = float(normalized.get("daily_pnl_sim", 0.0) or 0.0)
    normalized["equity_peak"] = float(normalized.get("equity_peak", 0.0) or 0.0)
    normalized["consecutive_losses"] = int(normalized.get("consecutive_losses", 0) or 0)
    normalized["cooldown_until_ts"] = int(normalized.get("cooldown_until_ts", 0) or 0)
    normalized["pause_until_ts"] = int(normalized.get("pause_until_ts", 0) or 0)
    normalized["pause_reason"] = str(normalized.get("pause_reason", "") or "")
    normalized["kill_reason"] = str(normalized.get("kill_reason", "") or "")
    normalized["last_trade_ts"] = int(normalized.get("last_trade_ts", 0) or 0)
    last_trade_symbol_ts = normalized.get("last_trade_symbol_ts", {})
    if isinstance(last_trade_symbol_ts, dict):
        normalized["last_trade_symbol_ts"] = {
            str(k).upper(): int(v or 0) for k, v in last_trade_symbol_ts.items() if str(k).strip()
        }
    else:
        normalized["last_trade_symbol_ts"] = {}
    normalized["cooldown_until_utc"] = str(normalized.get("cooldown_until_utc", "") or "")

    open_positions = normalized.get("open_positions")
    if isinstance(open_positions, list):
        normalized["open_positions"] = [p for p in open_positions if isinstance(p, dict)]
    else:
        legacy_open_position = normalized.get("open_paper_position")
        normalized["open_positions"] = [legacy_open_position] if isinstance(legacy_open_position, dict) else []
    normalized.pop("open_paper_position", None)

    closed_trades = normalized.get("closed_trades")
    if isinstance(closed_trades, list):
        normalized["closed_trades"] = [t for t in closed_trades if isinstance(t, dict)][-2000:]
    else:
        normalized["closed_trades"] = []

    fingerprints = normalized.get("intent_fingerprints")
    if isinstance(fingerprints, list):
        normalized["intent_fingerprints"] = [
            f for f in fingerprints if isinstance(f, dict) and str(f.get("fingerprint", "")).strip()
        ][-500:]
    else:
        normalized["intent_fingerprints"] = []

    early_alert_keys = normalized.get("early_alert_keys")
    if isinstance(early_alert_keys, list):
        normalized["early_alert_keys"] = [
            e for e in early_alert_keys if isinstance(e, dict) and str(e.get("key", "")).strip()
        ][-1000:]
    else:
        normalized["early_alert_keys"] = []

    signal_hashes = normalized.get("signal_hashes")
    if isinstance(signal_hashes, list):
        normalized["signal_hashes"] = [str(h).strip() for h in signal_hashes if str(h).strip()][-10000:]
    else:
        normalized["signal_hashes"] = []

    last_scan_ts = normalized.get("last_scan_ts")
    try:
        normalized["last_scan_ts"] = int(last_scan_ts) if last_scan_ts is not None else None
    except (TypeError, ValueError):
        normalized["last_scan_ts"] = None

    normalized["last_block_reason"] = str(normalized.get("last_block_reason", "-") or "-")
    normalized["risk_metrics"] = {
        "day_utc": normalized["day_utc"],
        "trade_count_today": normalized["trade_count_today"],
        "daily_pnl_realized": normalized["daily_pnl_realized"],
        "daily_pnl_sim": normalized["daily_pnl_sim"],
        "equity_peak": normalized["equity_peak"],
        "consecutive_losses": normalized["consecutive_losses"],
        "cooldown_until_ts": normalized["cooldown_until_ts"],
        "pause_until_ts": normalized["pause_until_ts"],
        "pause_reason": normalized["pause_reason"],
        "kill_reason": normalized["kill_reason"],
        "cooldown_until_utc": normalized["cooldown_until_utc"],
    }
    return normalized



def load_paper_state() -> Dict[str, Any]:
    raw = sqlite_store.kv_get("paper_state_json")
    if not raw:
        return _default_paper_state()
    try:
        parsed = json.loads(raw)
    except Exception:
        return _default_paper_state()
    if not isinstance(parsed, dict):
        return _default_paper_state()
    return _normalize_paper_state(parsed)



def save_paper_state(state: Dict[str, Any]) -> None:
    normalized = _normalize_paper_state(state)
    sqlite_store.kv_set("paper_state_json", json.dumps(normalized, ensure_ascii=True))
    # Maintain key counters for quick inspection and keep position/fill persistence in normalized tables.
    sqlite_store.kv_set("counter.trade_count_today", str(int(normalized.get("trade_count_today", 0) or 0)))
    sqlite_store.kv_set("counter.daily_pnl_sim", str(float(normalized.get("daily_pnl_sim", 0.0) or 0.0)))
    sqlite_store.kv_set("counter.consecutive_losses", str(int(normalized.get("consecutive_losses", 0) or 0)))
    sqlite_store.kv_set("counter.day_utc", str(normalized.get("day_utc", "")))
    if not _is_defer_position_sync():
        sqlite_store.sync_positions_and_fills(
            open_positions=list(normalized.get("open_positions", []) or []),
            closed_trades=list(normalized.get("closed_trades", []) or []),
        )


def sync_paper_positions_from_state() -> None:
    """Sync positions table from current paper state. Call once at end of scan cycle when defer was used."""
    state = load_paper_state()
    open_pos = list(state.get("open_positions", []) or [])
    closed = list(state.get("closed_trades", []) or [])
    sqlite_store.sync_positions_and_fills(open_positions=open_pos, closed_trades=closed)
    n = len(open_pos) + len(closed)
    if n > 0:
        import logging
        logging.info("positions_upserted=%d (end of cycle)", n)



def append_signal(signal: Dict[str, Any]) -> None:
    store_signal(signal)



def store_signal(signal: Dict[str, Any]) -> bool:
    return sqlite_store.store_signal(signal)



def get_recent_signals(limit: int = 50) -> List[Dict[str, Any]]:
    return sqlite_store.get_recent_signals(limit=limit)



def get_signals_since(ts: int) -> int:
    return sqlite_store.count_signals_since(int(ts))



def store_trade_intent(intent: Dict[str, Any]) -> None:
    sqlite_store.store_trade_intent(intent)



def upsert_paper_position(position: Dict[str, Any]) -> None:
    sqlite_store.upsert_paper_position(position)


def delete_paper_position(position_id: str) -> None:
    sqlite_store.delete_paper_position(position_id)


def insert_paper_trade(trade: Dict[str, Any]) -> None:
    sqlite_store.insert_paper_trade(trade)


def get_recent_trade_intents(limit: int = 50) -> List[Dict[str, Any]]:
    return sqlite_store.get_recent_trade_intents(limit=limit)


def get_block_stats_last_24h() -> List[Dict[str, Any]]:
    """Return block_reason counts for trade_intents with status=BLOCK over last 24h."""
    return sqlite_store.get_block_stats_last_24h()



def store_risk_event(event: Dict[str, Any]) -> None:
    sqlite_store.store_risk_event(event)



def get_recent_risk_events(limit: int = 50) -> List[Dict[str, Any]]:
    return sqlite_store.get_recent_risk_events(limit=limit)



def get_open_positions() -> list:
    state = load_paper_state()
    return list(state.get("open_positions", []) or [])



def set_open_positions(open_positions: list) -> None:
    state = load_paper_state()
    state["open_positions"] = [p for p in (open_positions or []) if isinstance(p, dict)]
    save_paper_state(state)



def add_closed_trade(closed_trade: Dict[str, Any]) -> None:
    state = load_paper_state()
    closed_trades = list(state.get("closed_trades", []) or [])
    closed_trades.append(dict(closed_trade))
    state["closed_trades"] = closed_trades[-2000:]
    save_paper_state(state)


def _compute_r_for_trade(t: Dict[str, Any]) -> tuple[Optional[float], Optional[str]]:
    """Compute r_multiple for a trade if possible. Returns (r_multiple, r_reason)."""
    r = t.get("r_multiple")
    if r is not None:
        try:
            return float(r), t.get("r_reason")
        except (TypeError, ValueError):
            pass
    entry = float(t.get("entry_price", 0) or t.get("entry", 0) or 0)
    sl = float(t.get("sl_price", 0) or t.get("sl", 0) or 0)
    pnl = float(t.get("pnl_usdt", 0) or t.get("pnl", 0) or 0)
    qty_est = t.get("qty_est")
    notional = float(t.get("notional_usdt", 0) or t.get("notional", 0) or 0)
    qty = (
        float(qty_est)
        if (qty_est is not None and float(qty_est or 0) > 0)
        else (notional / max(entry, 1e-10) if entry > 0 else 0.0)
    )
    risk_usdt = abs(entry - sl) * max(0.0, qty)
    if risk_usdt <= 0:
        return None, "RISK_ZERO"
    return pnl / risk_usdt, None


def compute_paper_kpis(
    closed_trades: List[Dict[str, Any]],
    paper_equity_usdt: float = 200.0,
) -> Dict[str, Any]:
    """
    Compute Paper KPIs from closed_trades.
    Returns {kpi: {...}, kpi_by_setup: {setup_name: {...}}}.
    """
    trades = [t for t in (closed_trades or []) if isinstance(t, dict)]
    wins = [t for t in trades if float(t.get("pnl_usdt", 0) or 0) > 0]
    losses = [t for t in trades if float(t.get("pnl_usdt", 0) or 0) < 0]
    r_values = []
    for t in trades:
        r, _ = _compute_r_for_trade(t)
        if r is not None:
            r_values.append(r)
    win_rs = [r for r in r_values if r > 0]
    avg_win_r = sum(win_rs) / len(win_rs) if win_rs else 0.0
    loss_rs = [r for r in r_values if r < 0]
    avg_loss_r = sum(loss_rs) / len(loss_rs) if loss_rs else 0.0
    expectancy_r = sum(r_values) / len(r_values) if r_values else 0.0
    sum_pos = sum(float(t.get("pnl_usdt", 0) or 0) for t in trades if float(t.get("pnl_usdt", 0) or 0) > 0)
    sum_neg = sum(float(t.get("pnl_usdt", 0) or 0) for t in trades if float(t.get("pnl_usdt", 0) or 0) < 0)
    profit_factor = sum_pos / abs(sum_neg) if sum_neg != 0 else (float("inf") if sum_pos > 0 else 0.0)
    equity = float(paper_equity_usdt)
    peak = equity
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: str(x.get("close_ts", x.get("entry_ts", "")))):
        pnl = float(t.get("pnl_usdt", 0) or 0)
        equity += pnl
        peak = max(peak, equity)
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    kpi = {
        "expectancy_R": round(expectancy_r, 4),
        "winrate": len(wins) / len(trades) if trades else 0.0,
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 999.0,
        "max_dd_usdt": round(max_dd, 2),
        "trades_total": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "avg_win_R": round(avg_win_r, 4),
        "avg_loss_R": round(avg_loss_r, 4),
    }
    by_setup: Dict[str, Dict[str, Any]] = {}
    for t in trades:
        setup = str(t.get("setup", t.get("strategy", "")) or "").strip() or "unknown"
        if setup not in by_setup:
            by_setup[setup] = {"trades": [], "wins": 0, "losses": 0, "pnl_sum": 0.0, "r_values": []}
        by_setup[setup]["trades"].append(t)
        pnl = float(t.get("pnl_usdt", 0) or 0)
        by_setup[setup]["pnl_sum"] += pnl
        if pnl > 0:
            by_setup[setup]["wins"] += 1
        elif pnl < 0:
            by_setup[setup]["losses"] += 1
        r_val, _ = _compute_r_for_trade(t)
        if r_val is not None:
            by_setup[setup]["r_values"].append(r_val)
    kpi_by_setup = {}
    for setup, data in by_setup.items():
        tr = data["trades"]
        wr = data["wins"] / len(tr) if tr else 0.0
        rv = data["r_values"]
        exp_r = sum(rv) / len(rv) if rv else 0.0
        win_rs_s = [r for r in rv if r > 0]
        loss_rs_s = [r for r in rv if r < 0]
        avg_win_r_s = sum(win_rs_s) / len(win_rs_s) if win_rs_s else 0.0
        avg_loss_r_s = sum(loss_rs_s) / len(loss_rs_s) if loss_rs_s else 0.0
        sp = sum(float(x.get("pnl_usdt", 0) or 0) for x in tr if float(x.get("pnl_usdt", 0) or 0) > 0)
        sn = sum(float(x.get("pnl_usdt", 0) or 0) for x in tr if float(x.get("pnl_usdt", 0) or 0) < 0)
        pf = sp / abs(sn) if sn != 0 else (999.0 if sp > 0 else 0.0)
        eq_s = float(paper_equity_usdt)
        peak_s = eq_s
        max_dd_s = 0.0
        for t in sorted(tr, key=lambda x: str(x.get("close_ts", x.get("entry_ts", "")))):
            pnl = float(t.get("pnl_usdt", 0) or 0)
            eq_s += pnl
            peak_s = max(peak_s, eq_s)
            dd = peak_s - eq_s
            if dd > max_dd_s:
                max_dd_s = dd
        kpi_by_setup[setup] = {
            "expectancy_R": round(exp_r, 4),
            "winrate": round(wr, 4),
            "profit_factor": round(pf, 4) if pf != 999.0 else 999.0,
            "trades_total": len(tr),
            "wins": data["wins"],
            "losses": data["losses"],
            "avg_win_R": round(avg_win_r_s, 4),
            "avg_loss_R": round(avg_loss_r_s, 4),
            "max_dd_usdt": round(max_dd_s, 2),
        }
    if os.getenv("KPI_DEBUG", "0") == "1" and trades:
        import logging
        sample = trades[-5:]
        for t in sample:
            r_val, r_reason = _compute_r_for_trade(t)
            logging.info(
                "KPI_DEBUG trade: setup=%s pnl_usdt=%s risk_usdt=%s R=%s r_reason=%s",
                t.get("setup", t.get("strategy", "?")),
                t.get("pnl_usdt"),
                t.get("risk_usdt"),
                r_val,
                r_reason or t.get("r_reason"),
            )
    return {"kpi": kpi, "kpi_by_setup": kpi_by_setup}



def get_last_scan_ts() -> Optional[int]:
    state = load_paper_state()
    raw_ts = state.get("last_scan_ts")
    try:
        return int(raw_ts) if raw_ts is not None else None
    except (TypeError, ValueError):
        return None


def get_risk_metrics(state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    src = _normalize_paper_state(state or load_paper_state())
    risk_metrics = src.get("risk_metrics")
    if isinstance(risk_metrics, dict):
        return {
            "day_utc": str(risk_metrics.get("day_utc", src.get("day_utc", "")) or ""),
            "trade_count_today": int(risk_metrics.get("trade_count_today", src.get("trade_count_today", 0)) or 0),
            "daily_pnl_realized": float(
                risk_metrics.get("daily_pnl_realized", src.get("daily_pnl_realized", 0.0)) or 0.0
            ),
            "daily_pnl_sim": float(risk_metrics.get("daily_pnl_sim", src.get("daily_pnl_sim", 0.0)) or 0.0),
            "equity_peak": float(risk_metrics.get("equity_peak", src.get("equity_peak", 0.0)) or 0.0),
            "consecutive_losses": int(risk_metrics.get("consecutive_losses", src.get("consecutive_losses", 0)) or 0),
            "cooldown_until_ts": int(risk_metrics.get("cooldown_until_ts", src.get("cooldown_until_ts", 0)) or 0),
            "pause_until_ts": int(risk_metrics.get("pause_until_ts", src.get("pause_until_ts", 0)) or 0),
            "pause_reason": str(risk_metrics.get("pause_reason", src.get("pause_reason", "")) or ""),
            "kill_reason": str(risk_metrics.get("kill_reason", src.get("kill_reason", "")) or ""),
            "cooldown_until_utc": str(risk_metrics.get("cooldown_until_utc", src.get("cooldown_until_utc", "")) or ""),
        }
    return {
        "day_utc": str(src.get("day_utc", "") or ""),
        "trade_count_today": int(src.get("trade_count_today", 0) or 0),
        "daily_pnl_realized": float(src.get("daily_pnl_realized", 0.0) or 0.0),
        "daily_pnl_sim": float(src.get("daily_pnl_sim", 0.0) or 0.0),
        "equity_peak": float(src.get("equity_peak", 0.0) or 0.0),
        "consecutive_losses": int(src.get("consecutive_losses", 0) or 0),
        "cooldown_until_ts": int(src.get("cooldown_until_ts", 0) or 0),
        "pause_until_ts": int(src.get("pause_until_ts", 0) or 0),
        "pause_reason": str(src.get("pause_reason", "") or ""),
        "kill_reason": str(src.get("kill_reason", "") or ""),
        "cooldown_until_utc": str(src.get("cooldown_until_utc", "") or ""),
    }



def set_last_scan_ts(ts: Optional[int]) -> None:
    state = load_paper_state()
    state["last_scan_ts"] = None if ts is None else int(ts)
    save_paper_state(state)



def set_last_block_reason(reason: str) -> None:
    state = load_paper_state()
    state["last_block_reason"] = str(reason or "-")
    save_paper_state(state)



def get_last_block_reason() -> str:
    state = load_paper_state()
    return str(state.get("last_block_reason", "-") or "-")


def set_symbols_v3(symbols: Dict[str, Dict[str, Any]]) -> None:
    """Persist per-symbol V3 status for dashboard. symbols[symbol][\"v3\"] = {ok, side, reason, breakout_level}."""
    sqlite_store.kv_set("symbols_v3_json", json.dumps(dict(symbols or {}), ensure_ascii=True))


def get_symbols_v3() -> Dict[str, Dict[str, Any]]:
    """Return per-symbol state including v3 status for /api/summary."""
    raw = sqlite_store.kv_get("symbols_v3_json")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def set_last_scan_error(msg: str) -> None:
    sqlite_store.kv_set("last_scan_error", str(msg or "-")[:500])


def get_last_scan_error() -> str:
    return str(sqlite_store.kv_get("last_scan_error", "-") or "-")


def get_stall_alerted() -> bool:
    return sqlite_store.kv_get("stall_alerted", "0") == "1"


def set_stall_alerted(alerted: bool) -> None:
    sqlite_store.kv_set("stall_alerted", "1" if alerted else "0")


def set_watchlist_transparency(
    source: str,
    symbols: List[str],
    *,
    candidates_count: Optional[int] = None,
    cached_until_ts: Optional[int] = None,
) -> None:
    """Persist watchlist transparency meta. No secrets."""
    ts = int(datetime.now(timezone.utc).timestamp())
    sqlite_store.kv_set("watchlist_source", str(source or "static"))
    sqlite_store.kv_set("watchlist_updated_ts", str(ts))
    sqlite_store.kv_set("selected_symbols_json", json.dumps(list(symbols or [])))
    if candidates_count is not None:
        sqlite_store.kv_set("watchlist_candidates_count", str(int(candidates_count)))
    if cached_until_ts is not None:
        sqlite_store.kv_set("watchlist_cached_until_ts", str(int(cached_until_ts)))


def get_watchlist_transparency() -> Dict[str, Any]:
    """Return watchlist transparency dict for /api/summary."""
    source = str(sqlite_store.kv_get("watchlist_source") or sqlite_store.kv_get("last_watchlist_source", "static") or "static")
    raw_ts = sqlite_store.kv_get("watchlist_updated_ts") or sqlite_store.kv_get("last_watchlist_updated_ts")
    raw_cached = sqlite_store.kv_get("watchlist_cached_until_ts")
    raw_count = sqlite_store.kv_get("watchlist_candidates_count")
    raw_symbols = sqlite_store.kv_get("selected_symbols_json")
    try:
        updated_ts = int(raw_ts) if raw_ts and str(raw_ts).strip().isdigit() else None
    except (TypeError, ValueError):
        updated_ts = None
    try:
        cached_until_ts = int(raw_cached) if raw_cached and str(raw_cached).strip().isdigit() else None
    except (TypeError, ValueError):
        cached_until_ts = None
    try:
        candidates_count = int(raw_count) if raw_count else None
    except (TypeError, ValueError):
        candidates_count = None
    symbols: List[str] = []
    if raw_symbols:
        try:
            parsed = json.loads(raw_symbols)
            symbols = list(parsed) if isinstance(parsed, list) else []
        except Exception:
            pass
    if not symbols:
        raw_legacy = sqlite_store.kv_get("selected_watchlist_json")
        if raw_legacy:
            try:
                data = json.loads(raw_legacy)
                symbols = list(data.get("symbols", []) or [])
            except Exception:
                pass
    return {
        "watchlist_source": source,
        "watchlist_updated_ts": updated_ts,
        "watchlist_cached_until_ts": cached_until_ts,
        "watchlist_candidates_count": candidates_count,
        "selected_symbols": symbols,
    }


def set_last_watchlist_meta(source: str, symbols: List[str]) -> None:
    """Legacy: persist watchlist source and updated ts. Use set_watchlist_transparency."""
    set_watchlist_transparency(source, symbols)


def get_last_watchlist_meta() -> tuple:
    """Legacy: return (source, updated_ts). Use get_watchlist_transparency."""
    d = get_watchlist_transparency()
    return d["watchlist_source"], d["watchlist_updated_ts"]


def set_selected_watchlist(symbols: List[str], mode: str) -> None:
    """Persist last computed watchlist (for dashboard). No secrets."""
    sqlite_store.kv_set("selected_watchlist_json", json.dumps({"symbols": list(symbols or []), "mode": str(mode or "static")}))


def get_selected_watchlist() -> tuple:
    """Return (symbols, mode) from storage. Empty if never set."""
    raw = sqlite_store.kv_get("selected_watchlist_json")
    if not raw:
        return [], "static"
    try:
        data = json.loads(raw)
        symbols = list(data.get("symbols", []) or [])
        mode = str(data.get("mode", "static") or "static")
        return symbols, mode
    except Exception:
        return [], "static"


def set_last_bias_json(bias_list: List[Dict[str, Any]]) -> None:
    """Persist per-scan bias map for dashboard. No secrets."""
    sqlite_store.kv_set("last_bias_json", json.dumps(bias_list or [], ensure_ascii=True))


def get_last_bias_json() -> List[Dict[str, Any]]:
    """Return last bias list for /api/summary."""
    raw = sqlite_store.kv_get("last_bias_json")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return list(parsed) if isinstance(parsed, list) else []
    except Exception:
        return []


def set_near_misses(near_misses: List[Dict[str, Any]]) -> None:
    """Persist top near-miss candidates for dashboard. No secrets."""
    sqlite_store.kv_set("near_misses_json", json.dumps(near_misses or [], ensure_ascii=True))


def get_near_misses() -> List[Dict[str, Any]]:
    """Return last near_misses for /api/summary."""
    raw = sqlite_store.kv_get("near_misses_json")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return list(parsed) if isinstance(parsed, list) else []
    except Exception:
        return []
