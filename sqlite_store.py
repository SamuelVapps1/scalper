from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_DB_LOCK = threading.RLock()
_DB_READY = False



def _resolve_db_path() -> Path:
    raw = str(os.getenv("DB_PATH", "./data/scalper.db") or "./data/scalper.db").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path



def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_resolve_db_path()), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn



def _ensure_db() -> None:
    global _DB_READY
    if _DB_READY:
        return
    with _DB_LOCK:
        if _DB_READY:
            return
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    hash TEXT PRIMARY KEY,
                    ts INTEGER,
                    symbol TEXT,
                    setup TEXT,
                    direction TEXT,
                    timeframe TEXT,
                    score REAL,
                    notes TEXT,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS trade_intents (
                    id TEXT PRIMARY KEY,
                    ts INTEGER,
                    symbol TEXT,
                    setup TEXT,
                    direction TEXT,
                    timeframe TEXT,
                    status TEXT,
                    risk_verdict TEXT,
                    block_reason TEXT,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS risk_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER,
                    type TEXT,
                    status TEXT,
                    reason TEXT,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS positions (
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    direction TEXT,
                    status TEXT,
                    opened_ts INTEGER,
                    closed_ts INTEGER,
                    entry REAL,
                    sl REAL,
                    tp REAL,
                    exit REAL,
                    pnl REAL,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS kv (
                    k TEXT PRIMARY KEY,
                    v TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_signals_hash ON signals(hash);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_ts ON trade_intents(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON risk_events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
                """
            )
            _migrate_schema(conn)
        _DB_READY = True


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add columns for existing DBs; tolerate missing columns."""
    migrations = [
        "ALTER TABLE signals ADD COLUMN json TEXT",
        "ALTER TABLE trade_intents ADD COLUMN json TEXT",
        "ALTER TABLE risk_events ADD COLUMN json TEXT",
        "ALTER TABLE positions ADD COLUMN json TEXT",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass



def _to_epoch(value: Any) -> int:
    if value is None:
        return int(time.time())
    raw = str(value).strip()
    if not raw:
        return int(time.time())
    if raw.isdigit():
        try:
            return int(raw)
        except ValueError:
            return int(time.time())
    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return int(time.time())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())



def _epoch_to_iso(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""



def _signal_hash(signal: Dict[str, Any]) -> str:
    symbol = str(signal.get("symbol", "") or "").strip().upper()
    setup = str(signal.get("setup") or signal.get("strategy") or "").strip()
    direction = str(signal.get("direction") or signal.get("side") or "").strip().upper()
    timeframe = str(signal.get("timeframe") or signal.get("interval") or "").strip()
    candle_ts = str(
        signal.get("candle_ts")
        or signal.get("bar_ts_used")
        or signal.get("timestamp_utc")
        or signal.get("ts")
        or ""
    ).strip()
    payload = "|".join([symbol, setup, direction, timeframe, candle_ts])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()



def store_signal(signal: Dict[str, Any]) -> bool:
    _ensure_db()
    ts = _to_epoch(signal.get("timestamp_utc") or signal.get("ts") or signal.get("candle_ts"))
    symbol = str(signal.get("symbol", "") or "")
    setup = str(signal.get("setup") or signal.get("strategy") or "")
    direction = str(signal.get("direction") or signal.get("side") or "")
    timeframe = str(signal.get("timeframe") or signal.get("interval") or "")
    score_raw = signal.get("score", signal.get("confidence", None))
    try:
        score = float(score_raw) if score_raw is not None and str(score_raw) != "" else None
    except (TypeError, ValueError):
        score = None
    notes = str(signal.get("notes") or signal.get("reason") or "")
    sig_hash = _signal_hash(signal)

    json_payload = json.dumps(signal, ensure_ascii=True)
    with _DB_LOCK:
        with _connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO signals (hash, ts, symbol, setup, direction, timeframe, score, notes, json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (sig_hash, ts, symbol, setup, direction, timeframe, score, notes, json_payload),
            )
            return int(cur.rowcount or 0) > 0



def get_recent_signals(limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db()
    max_rows = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, symbol, setup, direction, timeframe, score, notes
            FROM signals
            ORDER BY ts DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "ts": _epoch_to_iso(row["ts"]),
                "symbol": str(row["symbol"] or ""),
                "setup": str(row["setup"] or ""),
                "direction": str(row["direction"] or ""),
                "score": "" if row["score"] is None else float(row["score"]),
                "timeframe": str(row["timeframe"] or ""),
                "notes": str(row["notes"] or ""),
            }
        )
    return out



def count_signals_since(ts: int) -> int:
    _ensure_db()
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(1) AS c FROM signals WHERE ts >= ?", (int(ts),)).fetchone()
    return int((row["c"] if row else 0) or 0)



def _intent_id(intent: Dict[str, Any]) -> str:
    base = str(intent.get("id") or "").strip()
    if not base:
        base = f"intent:{int(time.time() * 1000)}:{int(time.time_ns() % 1000000)}"
    return base



def store_trade_intent(intent: Dict[str, Any]) -> None:
    _ensure_db()
    payload = dict(intent or {})
    ts = _to_epoch(payload.get("ts") or payload.get("timestamp_utc"))
    iid = _intent_id(payload)

    with _DB_LOCK:
        with _connect() as conn:
            payload_json = json.dumps(payload, ensure_ascii=True)
            try:
                conn.execute(
                    """
                    INSERT INTO trade_intents (id, ts, symbol, setup, direction, timeframe, status, risk_verdict, block_reason, json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        iid,
                        ts,
                        str(payload.get("symbol", "") or ""),
                        str(payload.get("setup") or payload.get("strategy") or ""),
                        str(payload.get("direction") or payload.get("side") or ""),
                        str(payload.get("timeframe") or payload.get("interval") or ""),
                        str(payload.get("status", "") or ""),
                        str(payload.get("risk_verdict", "") or ""),
                        str(payload.get("block_reason", "") or ""),
                        payload_json,
                    ),
                )
            except sqlite3.IntegrityError:
                iid = f"{iid}:{ts}:{int(time.time_ns() % 1000000)}"
                conn.execute(
                    """
                    INSERT INTO trade_intents (id, ts, symbol, setup, direction, timeframe, status, risk_verdict, block_reason, json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        iid,
                        ts,
                        str(payload.get("symbol", "") or ""),
                        str(payload.get("setup") or payload.get("strategy") or ""),
                        str(payload.get("direction") or payload.get("side") or ""),
                        str(payload.get("timeframe") or payload.get("interval") or ""),
                        str(payload.get("status", "") or ""),
                        str(payload.get("risk_verdict", "") or ""),
                        str(payload.get("block_reason", "") or ""),
                        payload_json,
                    ),
                )



def get_recent_trade_intents(limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db()
    max_rows = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, symbol, setup, direction, timeframe, status, risk_verdict, block_reason, json
            FROM trade_intents
            ORDER BY ts DESC, rowid DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        raw = row["json"] if row["json"] is not None else "{}"
        try:
            payload = json.loads(str(raw or "{}"))
        except Exception:
            payload = {}
        merged = dict(payload)
        merged.setdefault("id", str(row["id"] or ""))
        merged.setdefault("ts", _epoch_to_iso(row["ts"]))
        merged.setdefault("symbol", str(row["symbol"] or ""))
        merged.setdefault("setup", str(row["setup"] or ""))
        merged.setdefault("direction", str(row["direction"] or ""))
        merged.setdefault("timeframe", str(row["timeframe"] or ""))
        merged.setdefault("status", str(row["status"] or ""))
        merged.setdefault("risk_verdict", str(row["risk_verdict"] or ""))
        merged.setdefault("block_reason", str(row["block_reason"] or ""))
        out.append(merged)
    return out



def store_risk_event(event: Dict[str, Any]) -> None:
    _ensure_db()
    payload = dict(event or {})
    ts = _to_epoch(payload.get("ts") or payload.get("timestamp_utc"))
    with _DB_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO risk_events (ts, type, status, reason, json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(payload.get("type", "") or ""),
                    str(payload.get("status", "") or ""),
                    str(payload.get("reason", "") or ""),
                    json.dumps(payload, ensure_ascii=True),
                ),
            )



def get_recent_risk_events(limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db()
    max_rows = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, type, status, reason, json
            FROM risk_events
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row["json"] or "{}"))
        except Exception:
            payload = {}
        merged = dict(payload)
        merged.setdefault("ts", _epoch_to_iso(row["ts"]))
        merged.setdefault("type", str(row["type"] or ""))
        merged.setdefault("status", str(row["status"] or ""))
        merged.setdefault("reason", str(row["reason"] or ""))
        out.append(merged)
    return out



def kv_get(key: str, default: Optional[str] = None) -> Optional[str]:
    _ensure_db()
    with _connect() as conn:
        row = conn.execute("SELECT v FROM kv WHERE k = ?", (str(key),)).fetchone()
    if not row:
        return default
    return str(row["v"])



def kv_set(key: str, value: str) -> None:
    _ensure_db()
    with _DB_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO kv(k, v) VALUES (?, ?)
                ON CONFLICT(k) DO UPDATE SET v=excluded.v
                """,
                (str(key), str(value)),
            )



def _to_epoch_stable(value: Any) -> int:
    """Stable epoch for id generation: use 0 if missing (no time.time())."""
    if value is None:
        return 0
    raw = str(value).strip()
    if not raw:
        return 0
    if raw.isdigit():
        try:
            return int(raw)
        except ValueError:
            return 0
    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _position_id(pos: Dict[str, Any], *, prefix: str = "pos") -> str:
    """Deterministic id: prefer intent_id, else sha1(symbol|opened_ts|direction). opened_ts uses ts or 0 if missing."""
    raw = str(pos.get("intent_id") or pos.get("id") or "").strip()
    if raw:
        return raw
    symbol = str(pos.get("symbol", "") or "")
    opened_ts = _to_epoch_stable(pos.get("entry_ts") or pos.get("ts"))
    direction = str(pos.get("side") or pos.get("direction") or "")
    payload = f"{symbol}|{opened_ts}|{direction}"
    return f"{prefix}:{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:16]}"


def _dedupe_by_id(items: List[Dict[str, Any]], id_fn, log_prefix: str) -> List[Dict[str, Any]]:
    """Deduplicate by id, keep last occurrence. Skip empty id, log warning."""
    seen: Dict[str, Dict[str, Any]] = {}
    skipped_empty = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        pid = id_fn(item)
        if not pid or not str(pid).strip():
            skipped_empty += 1
            continue
        seen[str(pid)] = item
    if skipped_empty:
        logging.warning("%s: skipped %d entries with missing/empty id", log_prefix, skipped_empty)
    return list(seen.values())


def sync_positions_and_fills(open_positions: List[Dict[str, Any]], closed_trades: List[Dict[str, Any]]) -> None:
    _ensure_db()
    raw_open = list(open_positions or [])
    raw_closed = list(closed_trades or [])
    open_list = _dedupe_by_id(
        raw_open,
        lambda p: _position_id(p, prefix="pos"),
        "sync_positions(open)",
    )
    closed_list = _dedupe_by_id(
        raw_closed,
        lambda t: _position_id(t, prefix="closed"),
        "sync_positions(closed)",
    )
    positions_upserted = len(open_list) + len(closed_list)
    generated = sum(
        1
        for p in open_list + closed_list
        if not str(p.get("intent_id") or p.get("id") or "").strip()
    )
    logging.info("positions_upserted=%d positions_generated_ids=%d", positions_upserted, generated)

    with _DB_LOCK:
        with _connect() as conn:
            open_ids = [_position_id(p, prefix="pos") for p in open_list]
            closed_ids = [_position_id(t, prefix="closed") for t in closed_list]
            keep_ids = set(open_ids) | set(closed_ids)

            for pos in open_list:
                pid = _position_id(pos, prefix="pos")
                symbol = str(pos.get("symbol", "") or "")
                direction = str(pos.get("side") or pos.get("direction") or "")
                opened_ts = _to_epoch(pos.get("entry_ts") or pos.get("ts"))
                entry = float(pos.get("entry_price", 0.0) or 0.0)
                sl = float(pos.get("sl_price", 0.0) or 0.0)
                tp = float(pos.get("tp_price", 0.0) or 0.0)
                conn.execute(
                    """
                    INSERT INTO positions (
                        id, symbol, direction, status, opened_ts, closed_ts, entry, sl, tp, exit, pnl, json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        symbol=excluded.symbol,
                        direction=excluded.direction,
                        status=excluded.status,
                        opened_ts=excluded.opened_ts,
                        closed_ts=excluded.closed_ts,
                        entry=excluded.entry,
                        sl=excluded.sl,
                        tp=excluded.tp,
                        exit=excluded.exit,
                        pnl=excluded.pnl,
                        json=excluded.json
                    """,
                    (pid, symbol, direction, "OPEN", opened_ts, None, entry, sl, tp, None, None, json.dumps(pos, ensure_ascii=True)),
                )

            for trade in closed_list:
                tid = _position_id(trade, prefix="closed")
                symbol = str(trade.get("symbol", "") or "")
                direction = str(trade.get("side") or trade.get("direction") or "")
                opened_ts = _to_epoch(trade.get("entry_ts") or trade.get("ts"))
                closed_ts = _to_epoch(trade.get("close_ts") or trade.get("timestamp_utc"))
                entry = float(trade.get("entry_price", 0.0) or 0.0)
                sl = float(trade.get("sl_price", 0.0) or 0.0)
                tp = float(trade.get("tp_price", 0.0) or 0.0)
                exit_price = float(trade.get("exit_price", 0.0) or 0.0)
                pnl = float(trade.get("pnl_usdt", 0.0) or trade.get("pnl", 0.0) or 0.0)
                conn.execute(
                    """
                    INSERT INTO positions (
                        id, symbol, direction, status, opened_ts, closed_ts, entry, sl, tp, exit, pnl, json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        symbol=excluded.symbol,
                        direction=excluded.direction,
                        status=excluded.status,
                        opened_ts=excluded.opened_ts,
                        closed_ts=excluded.closed_ts,
                        entry=excluded.entry,
                        sl=excluded.sl,
                        tp=excluded.tp,
                        exit=excluded.exit,
                        pnl=excluded.pnl,
                        json=excluded.json
                    """,
                    (tid, symbol, direction, "CLOSED", opened_ts, closed_ts, entry, sl, tp, exit_price, pnl, json.dumps(trade, ensure_ascii=True)),
                )

            if keep_ids:
                placeholders = ",".join("?" * len(keep_ids))
                conn.execute(
                    f"DELETE FROM positions WHERE id NOT IN ({placeholders})",
                    tuple(keep_ids),
                )
            else:
                conn.execute("DELETE FROM positions")
