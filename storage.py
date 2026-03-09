from __future__ import annotations

import csv
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import sqlite_store

_log = logging.getLogger(__name__)

CSV_PATH = Path("signals_log.csv")
CSV_ENRICHED_PATH = Path("signals_enriched_log.csv")
CSV_HEADERS = ["timestamp_utc", "symbol", "setup", "direction", "close", "reason"]
PAPER_STATE_PATH = Path("paper_state.json")


def _default_paper_state() -> Dict[str, Any]:
    return {
        "open_positions": [],
        "closed_trades": [],
        "daily_pnl_sim": 0.0,
        "consecutive_losses": 0,
        "trades_today_count": 0,
        "last_scan_ts": 0,
        "selected_watchlist": [],
        "selected_watchlist_mode": "static",
    }


def _normalize_paper_state(state: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(_default_paper_state())
    out.update(state or {})
    out["open_positions"] = list(out.get("open_positions", []) or [])
    out["closed_trades"] = list(out.get("closed_trades", []) or [])
    return out


def load_paper_state() -> Dict[str, Any]:
    if PAPER_STATE_PATH.exists():
        try:
            data = json.loads(PAPER_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return _normalize_paper_state(data)
        except Exception:
            pass
    return _default_paper_state()


def save_paper_state(state: Dict[str, Any]) -> None:
    payload = _normalize_paper_state(state if isinstance(state, dict) else {})
    PAPER_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _csv_write(path: Path, headers: List[str], row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not exists:
            writer.writeheader()
        writer.writerow({h: row.get(h, "") for h in headers})


def _has_valid_levels(signal: Dict[str, Any]) -> bool:
    entry = signal.get("entry")
    sl = signal.get("sl")
    tp = signal.get("tp")
    if entry is None or sl is None or tp is None:
        return False
    try:
        e, s, t = float(entry), float(sl), float(tp)
    except (TypeError, ValueError):
        return False
    return e > 0 and s > 0 and t > 0


def append_signal(signal: Dict[str, Any]) -> None:
    row = {
        "timestamp_utc": signal.get("timestamp_utc", ""),
        "symbol": signal.get("symbol", ""),
        "setup": signal.get("setup", signal.get("strategy", "")),
        "direction": signal.get("direction", signal.get("side", "")),
        "close": signal.get("close", ""),
        "reason": signal.get("reason", ""),
    }
    _csv_write(CSV_PATH, CSV_HEADERS, row)

    if not _has_valid_levels(signal):
        reason = str(signal.get("reason", "") or "").strip() or "PREVIEW_BUILD_FAILED"
        _log.warning(
            "PREVIEW_BUILD_FAILED symbol=%s setup=%s direction=%s reason=%s (missing or invalid entry/sl/tp)",
            row.get("symbol", ""),
            row.get("setup", ""),
            row.get("direction", ""),
            reason,
        )
        _csv_write(
            CSV_ENRICHED_PATH,
            [
                "timestamp_utc",
                "symbol",
                "setup",
                "direction",
                "close",
                "reason",
                "confidence",
                "entry",
                "sl",
                "tp",
            ],
            {
                **row,
                "reason": reason,
                "confidence": signal.get("confidence", ""),
                "entry": "",
                "sl": "",
                "tp": "",
            },
        )
    else:
        _csv_write(
            CSV_ENRICHED_PATH,
            [
                "timestamp_utc",
                "symbol",
                "setup",
                "direction",
                "close",
                "reason",
                "confidence",
                "entry",
                "sl",
                "tp",
            ],
            {
                **row,
                "confidence": signal.get("confidence", ""),
                "entry": signal.get("entry", ""),
                "sl": signal.get("sl", ""),
                "tp": signal.get("tp", ""),
            },
        )
    try:
        sqlite_store.store_signal(dict(signal))
    except Exception:
        pass


def store_trade_intent(intent: Dict[str, Any]) -> None:
    try:
        sqlite_store.store_trade_intent(intent)
    except Exception:
        pass


def upsert_paper_position(position: Dict[str, Any]) -> None:
    try:
        sqlite_store.upsert_paper_position(position)
    except Exception:
        state = load_paper_state()
        positions = [p for p in state["open_positions"] if p.get("position_id") != position.get("position_id")]
        positions.append(position)
        state["open_positions"] = positions
        save_paper_state(state)


def delete_paper_position(position_id: str) -> None:
    try:
        sqlite_store.delete_paper_position(position_id)
    except Exception:
        state = load_paper_state()
        state["open_positions"] = [p for p in state["open_positions"] if str(p.get("position_id", "")) != str(position_id)]
        save_paper_state(state)


def insert_paper_trade(trade: Dict[str, Any]) -> None:
    try:
        sqlite_store.insert_paper_trade(trade)
    except Exception:
        state = load_paper_state()
        state["closed_trades"].append(trade)
        save_paper_state(state)


def sync_paper_positions_from_state(state: Dict[str, Any] | None = None) -> None:
    if state is None:
        state = load_paper_state()
    open_positions = list((state or {}).get("open_positions", []) or [])
    closed_trades = list((state or {}).get("closed_trades", []) or [])
    try:
        sqlite_store.sync_positions_and_fills(open_positions, closed_trades)
    except Exception:
        pass


def _kv_get(key: str, default: Any = None) -> Any:
    val = sqlite_store.kv_get(key, None)
    if val is None:
        return default
    try:
        return json.loads(val)
    except Exception:
        return val


def _kv_set(key: str, value: Any) -> None:
    try:
        sqlite_store.kv_set(key, json.dumps(value, ensure_ascii=True))
    except Exception:
        pass


def set_last_scan_ts(ts: int) -> None:
    _kv_set("last_scan_ts", int(ts))


def get_last_scan_ts() -> int:
    return int(_kv_get("last_scan_ts", 0) or 0)


def set_last_scan_error(msg: str) -> None:
    _kv_set("last_scan_error", str(msg or ""))


def get_last_scan_error() -> str:
    return str(_kv_get("last_scan_error", "") or "")


def set_stall_alerted(flag: bool) -> None:
    _kv_set("stall_alerted", bool(flag))


def get_stall_alerted() -> bool:
    return bool(_kv_get("stall_alerted", False))


def set_selected_watchlist(symbols: List[str], mode: str) -> None:
    _kv_set("selected_watchlist", list(symbols or []))
    _kv_set("selected_watchlist_mode", str(mode or "static"))


def get_selected_watchlist() -> Tuple[List[str], str]:
    return list(_kv_get("selected_watchlist", []) or []), str(_kv_get("selected_watchlist_mode", "static") or "static")


def set_last_block_reason(reason: str) -> None:
    _kv_set("last_block_reason", str(reason or ""))


def get_last_block_reason() -> str:
    return str(_kv_get("last_block_reason", "") or "")


def set_last_bias_json(items: List[Dict[str, Any]]) -> None:
    _kv_set("last_bias_json", list(items or []))


def get_last_bias_json() -> List[Dict[str, Any]]:
    return list(_kv_get("last_bias_json", []) or [])


def set_near_misses(items: List[Dict[str, Any]]) -> None:
    _kv_set("near_misses", list(items or []))


def get_near_misses() -> List[Dict[str, Any]]:
    return list(_kv_get("near_misses", []) or [])


def set_symbols_v3(items: Dict[str, Any]) -> None:
    _kv_set("symbols_v3", dict(items or {}))


def get_symbols_v3() -> Dict[str, Any]:
    return dict(_kv_get("symbols_v3", {}) or {})


def set_defer_position_sync(flag: bool) -> None:
    _kv_set("defer_position_sync", bool(flag))


def get_defer_position_sync() -> bool:
    return bool(_kv_get("defer_position_sync", False))


def set_watchlist_transparency(source: str, selected_symbols: List[str], candidates_count: int = 0, cached_until_ts: int = 0) -> None:
    _kv_set(
        "watchlist_transparency",
        {
            "source": str(source or ""),
            "selected_symbols": list(selected_symbols or []),
            "candidates_count": int(candidates_count or 0),
            "cached_until_ts": int(cached_until_ts or 0),
            "updated_ts": int(time.time()),
        },
    )


def get_watchlist_transparency() -> Dict[str, Any]:
    return dict(_kv_get("watchlist_transparency", {}) or {})


def get_watchlist_rotation_offset() -> int:
    return int(_kv_get("watchlist_rotation_offset", 0) or 0)


def set_watchlist_rotation_offset(offset: int) -> None:
    _kv_set("watchlist_rotation_offset", int(offset))


def get_recent_signals(limit: int = 50) -> List[Dict[str, Any]]:
    try:
        return sqlite_store.get_recent_signals(limit=limit)
    except Exception:
        return []


def get_signals_since(ts: int) -> int:
    try:
        return int(sqlite_store.count_signals_since(int(ts)))
    except Exception:
        return 0


def get_recent_trade_intents(limit: int = 50) -> List[Dict[str, Any]]:
    try:
        return sqlite_store.get_recent_trade_intents(limit=limit)
    except Exception:
        return []


def get_recent_risk_events(limit: int = 50) -> List[Dict[str, Any]]:
    try:
        return sqlite_store.get_recent_risk_events(limit=limit)
    except Exception:
        return []


def get_block_stats_last_24h() -> List[Dict[str, Any]]:
    try:
        return sqlite_store.get_block_stats_last_24h()
    except Exception:
        return []


def get_risk_metrics(state: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if state is None:
        state = load_paper_state()
    return {
        "open_positions": len(list(state.get("open_positions", []) or [])),
        "closed_trades": len(list(state.get("closed_trades", []) or [])),
        "daily_pnl_sim": float(state.get("daily_pnl_sim", 0.0) or 0.0),
        "consecutive_losses": int(state.get("consecutive_losses", 0) or 0),
    }


def compute_paper_kpis() -> Dict[str, Any]:
    state = load_paper_state()
    closed = list(state.get("closed_trades", []) or [])
    pnl = [float(t.get("pnl_usdt", 0.0) or 0.0) for t in closed if isinstance(t, dict)]
    wins = sum(1 for x in pnl if x > 0)
    losses = sum(1 for x in pnl if x < 0)
    total = len(pnl)
    win_rate = (wins / total) if total else 0.0
    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "pnl_total": float(sum(pnl)),
    }
