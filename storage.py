from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sqlite_store



def _default_paper_state() -> Dict[str, Any]:
    return {
        "day_utc": datetime.now(timezone.utc).date().isoformat(),
        "trade_count_today": 0,
        "daily_pnl_sim": 0.0,
        "consecutive_losses": 0,
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
    normalized["daily_pnl_sim"] = float(normalized.get("daily_pnl_sim", 0.0) or 0.0)
    normalized["consecutive_losses"] = int(normalized.get("consecutive_losses", 0) or 0)
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
    sqlite_store.sync_positions_and_fills(
        open_positions=list(normalized.get("open_positions", []) or []),
        closed_trades=list(normalized.get("closed_trades", []) or []),
    )



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



def get_recent_trade_intents(limit: int = 50) -> List[Dict[str, Any]]:
    return sqlite_store.get_recent_trade_intents(limit=limit)



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



def get_last_scan_ts() -> Optional[int]:
    state = load_paper_state()
    raw_ts = state.get("last_scan_ts")
    try:
        return int(raw_ts) if raw_ts is not None else None
    except (TypeError, ValueError):
        return None



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
