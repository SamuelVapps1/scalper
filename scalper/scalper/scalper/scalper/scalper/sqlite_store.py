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

_INIT_LOCK = threading.Lock()
_WRITE_LOCK = threading.Lock()
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
    with _INIT_LOCK:
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
                    side TEXT,
                    setup TEXT,
                    strategy_id TEXT,
                    direction TEXT,
                    timeframe TEXT,
                    status TEXT,
                    verdict TEXT,
                    risk_verdict TEXT,
                    reason_code TEXT,
                    block_reason TEXT,
                    details_json TEXT,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS risk_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER,
                    event_type TEXT,
                    type TEXT,
                    status TEXT,
                    reason_code TEXT,
                    reason TEXT,
                    details_json TEXT,
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
                CREATE TABLE IF NOT EXISTS paper_positions (
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    side TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_price REAL,
                    sl_price REAL,
                    tp_price REAL,
                    qty REAL,
                    notional_usdt REAL,
                    opened_ts INTEGER,
                    updated_ts INTEGER,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT,
                    symbol TEXT,
                    side TEXT,
                    strategy TEXT,
                    entry_ts INTEGER,
                    exit_ts INTEGER,
                    entry_price REAL,
                    exit_price REAL,
                    qty REAL,
                    notional_usdt REAL,
                    pnl_usdt REAL,
                    r_multiple REAL,
                    exit_reason TEXT,
                    fee_usdt REAL,
                    json TEXT
                );
                CREATE TABLE IF NOT EXISTS trade_records (
                    intent_id TEXT PRIMARY KEY,
                    ts_open INTEGER,
                    symbol TEXT,
                    strategy TEXT,
                    side TEXT,
                    tf TEXT,
                    entry REAL,
                    sl REAL,
                    tp REAL,
                    sl_pct REAL,
                    tp_pct REAL,
                    confidence REAL,
                    atr_pct REAL,
                    spread_bps REAL,
                    bias_flags TEXT,
                    ts_close INTEGER,
                    close_price REAL,
                    close_reason TEXT,
                    pnl_usdt REAL,
                    pnl_r REAL,
                    bars_held INTEGER,
                    mfe REAL,
                    mae REAL,
                    json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_signals_hash ON signals(hash);
                CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_signals_setup_ts ON signals(setup, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_signals_symbol_setup_ts ON signals(symbol, setup, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_ts ON trade_intents(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_symbol_ts ON trade_intents(symbol, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_setup_ts ON trade_intents(setup, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_symbol_setup_ts ON trade_intents(symbol, setup, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_intents_status_ts ON trade_intents(status, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON risk_events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_risk_events_type_ts ON risk_events(type, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_risk_events_reason_ts ON risk_events(reason, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
                CREATE INDEX IF NOT EXISTS idx_positions_symbol_status ON positions(symbol, status);
                CREATE INDEX IF NOT EXISTS idx_positions_symbol_opened_ts ON positions(symbol, opened_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_paper_positions_symbol_status ON paper_positions(symbol, status);
                CREATE INDEX IF NOT EXISTS idx_paper_positions_opened_ts ON paper_positions(opened_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol_exit_ts ON paper_trades(symbol, exit_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_records_ts_close ON trade_records(ts_close DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_records_strategy ON trade_records(strategy);
                CREATE INDEX IF NOT EXISTS idx_trade_records_symbol ON trade_records(symbol);
                """
            )
            _migrate_schema(conn)
        _DB_READY = True


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add columns for existing DBs; tolerate missing columns."""
    migrations = [
        "ALTER TABLE signals ADD COLUMN json TEXT",
        "ALTER TABLE trade_intents ADD COLUMN side TEXT",
        "ALTER TABLE trade_intents ADD COLUMN strategy_id TEXT",
        "ALTER TABLE trade_intents ADD COLUMN verdict TEXT",
        "ALTER TABLE trade_intents ADD COLUMN reason_code TEXT",
        "ALTER TABLE trade_intents ADD COLUMN details_json TEXT",
        "ALTER TABLE trade_intents ADD COLUMN json TEXT",
        "ALTER TABLE risk_events ADD COLUMN event_type TEXT",
        "ALTER TABLE risk_events ADD COLUMN reason_code TEXT",
        "ALTER TABLE risk_events ADD COLUMN details_json TEXT",
        "ALTER TABLE risk_events ADD COLUMN json TEXT",
        "ALTER TABLE positions ADD COLUMN json TEXT",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_trade_intents_verdict_ts ON trade_intents(verdict, ts DESC)",
        "CREATE INDEX IF NOT EXISTS idx_trade_intents_reason_code_ts ON trade_intents(reason_code, ts DESC)",
        "CREATE INDEX IF NOT EXISTS idx_risk_events_event_type_ts ON risk_events(event_type, ts DESC)",
        "CREATE INDEX IF NOT EXISTS idx_risk_events_reason_code_ts ON risk_events(reason_code, ts DESC)",
    ):
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
    with _WRITE_LOCK:
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

    side = str(payload.get("side") or payload.get("direction") or "")
    setup = str(payload.get("setup") or payload.get("strategy") or "")
    strategy_id = str(payload.get("strategy_id") or setup)
    verdict = str(payload.get("verdict") or payload.get("risk_verdict") or "")
    reason_code = str(payload.get("reason_code") or payload.get("block_reason") or "")
    details_json = payload.get("details_json")
    if details_json is None:
        details_json = json.dumps(payload.get("details", {}), ensure_ascii=True)
    else:
        details_json = str(details_json)

    with _WRITE_LOCK:
        with _connect() as conn:
            payload_json = json.dumps(payload, ensure_ascii=True)
            try:
                conn.execute(
                    """
                    INSERT INTO trade_intents (
                        id, ts, symbol, side, setup, strategy_id, direction, timeframe,
                        status, verdict, risk_verdict, reason_code, block_reason, details_json, json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        iid,
                        ts,
                        str(payload.get("symbol", "") or ""),
                        side,
                        setup,
                        strategy_id,
                        str(payload.get("direction") or side),
                        str(payload.get("timeframe") or payload.get("interval") or ""),
                        str(payload.get("status", "") or ""),
                        verdict,
                        str(payload.get("risk_verdict", "") or ""),
                        reason_code,
                        str(payload.get("block_reason", "") or ""),
                        details_json,
                        payload_json,
                    ),
                )
            except sqlite3.IntegrityError:
                iid = f"{iid}:{ts}:{int(time.time_ns() % 1000000)}"
                conn.execute(
                    """
                    INSERT INTO trade_intents (
                        id, ts, symbol, side, setup, strategy_id, direction, timeframe,
                        status, verdict, risk_verdict, reason_code, block_reason, details_json, json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        iid,
                        ts,
                        str(payload.get("symbol", "") or ""),
                        side,
                        setup,
                        strategy_id,
                        str(payload.get("direction") or side),
                        str(payload.get("timeframe") or payload.get("interval") or ""),
                        str(payload.get("status", "") or ""),
                        verdict,
                        str(payload.get("risk_verdict", "") or ""),
                        reason_code,
                        str(payload.get("block_reason", "") or ""),
                        details_json,
                        payload_json,
                    ),
                )



def get_block_stats_last_24h() -> List[Dict[str, Any]]:
    """Aggregate trade_intents where status=BLOCK over last 24h. Returns [{block_reason, count}] sorted by count desc."""
    _ensure_db()
    since_ts = int(time.time()) - 24 * 60 * 60
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT block_reason, COUNT(*) AS cnt
            FROM trade_intents
            WHERE status = 'BLOCK' AND ts >= ?
            GROUP BY block_reason
            ORDER BY cnt DESC
            """,
            (since_ts,),
        ).fetchall()
    return [
        {"block_reason": str(row["block_reason"] or "").strip() or "(empty)", "count": int(row["cnt"])}
        for row in rows
    ]


def get_recent_trade_intents(limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db()
    max_rows = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                id, ts, symbol, side, setup, strategy_id, direction, timeframe, status,
                verdict, risk_verdict, reason_code, block_reason, details_json, json
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
        merged.setdefault("side", str(row["side"] or ""))
        merged.setdefault("setup", str(row["setup"] or ""))
        merged.setdefault("strategy_id", str(row["strategy_id"] or ""))
        merged.setdefault("direction", str(row["direction"] or ""))
        merged.setdefault("timeframe", str(row["timeframe"] or ""))
        merged.setdefault("status", str(row["status"] or ""))
        merged.setdefault("verdict", str(row["verdict"] or ""))
        merged.setdefault("risk_verdict", str(row["risk_verdict"] or ""))
        merged.setdefault("reason_code", str(row["reason_code"] or ""))
        merged.setdefault("block_reason", str(row["block_reason"] or ""))
        merged.setdefault("details_json", str(row["details_json"] or ""))
        out.append(merged)
    return out



def store_risk_event(event: Dict[str, Any]) -> None:
    _ensure_db()
    payload = dict(event or {})
    ts = _to_epoch(payload.get("ts") or payload.get("timestamp_utc"))
    event_type = str(payload.get("event_type") or payload.get("type") or "")
    reason_code = str(payload.get("reason_code") or payload.get("reason") or "")
    details_json = payload.get("details_json")
    if details_json is None:
        details_json = json.dumps(payload.get("details", {}), ensure_ascii=True)
    else:
        details_json = str(details_json)
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO risk_events (ts, event_type, type, status, reason_code, reason, details_json, json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    event_type,
                    str(payload.get("type", "") or ""),
                    str(payload.get("status", "") or ""),
                    reason_code,
                    str(payload.get("reason", "") or ""),
                    details_json,
                    json.dumps(payload, ensure_ascii=True),
                ),
            )



def get_recent_risk_events(limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db()
    max_rows = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, event_type, type, status, reason_code, reason, details_json, json
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
        merged.setdefault("event_type", str(row["event_type"] or ""))
        merged.setdefault("type", str(row["type"] or ""))
        merged.setdefault("status", str(row["status"] or ""))
        merged.setdefault("reason_code", str(row["reason_code"] or ""))
        merged.setdefault("reason", str(row["reason"] or ""))
        merged.setdefault("details_json", str(row["details_json"] or ""))
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
    with _WRITE_LOCK:
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

    with _WRITE_LOCK:
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


def upsert_paper_position(position: Dict[str, Any]) -> None:
    _ensure_db()
    row = dict(position or {})
    pid = str(row.get("intent_id") or row.get("id") or "").strip() or _position_id(row, prefix="paper")
    symbol = str(row.get("symbol", "") or "").upper()
    side = str(row.get("side", "") or row.get("direction", "") or "").upper()
    strategy = str(row.get("strategy", "") or row.get("setup", "") or "")
    status = str(row.get("status", "OPEN") or "OPEN").upper()
    entry = float(row.get("entry_price", 0.0) or 0.0)
    sl = float(row.get("sl_price", 0.0) or 0.0)
    tp = float(row.get("tp_price", 0.0) or 0.0)
    qty = float(row.get("qty_est", row.get("qty", 0.0)) or 0.0)
    notional = float(row.get("notional_usdt", 0.0) or 0.0)
    opened_ts = _to_epoch(row.get("entry_ts") or row.get("ts"))
    updated_ts = _to_epoch(row.get("last_ts") or row.get("timestamp_utc") or row.get("close_ts") or row.get("entry_ts"))
    row["id"] = pid
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO paper_positions (
                    id, symbol, side, strategy, status, entry_price, sl_price, tp_price, qty, notional_usdt, opened_ts, updated_ts, json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    symbol=excluded.symbol,
                    side=excluded.side,
                    strategy=excluded.strategy,
                    status=excluded.status,
                    entry_price=excluded.entry_price,
                    sl_price=excluded.sl_price,
                    tp_price=excluded.tp_price,
                    qty=excluded.qty,
                    notional_usdt=excluded.notional_usdt,
                    opened_ts=excluded.opened_ts,
                    updated_ts=excluded.updated_ts,
                    json=excluded.json
                """,
                (
                    pid,
                    symbol,
                    side,
                    strategy,
                    status,
                    entry,
                    sl,
                    tp,
                    qty,
                    notional,
                    opened_ts,
                    updated_ts,
                    json.dumps(row, ensure_ascii=True),
                ),
            )


def delete_paper_position(position_id: str) -> None:
    _ensure_db()
    pid = str(position_id or "").strip()
    if not pid:
        return
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute("DELETE FROM paper_positions WHERE id = ?", (pid,))


def insert_paper_trade(trade: Dict[str, Any]) -> None:
    _ensure_db()
    row = dict(trade or {})
    position_id = str(row.get("intent_id") or row.get("position_id") or "").strip()
    symbol = str(row.get("symbol", "") or "").upper()
    side = str(row.get("side", "") or row.get("direction", "") or "").upper()
    strategy = str(row.get("strategy", "") or row.get("setup", "") or "")
    entry_ts = _to_epoch(row.get("entry_ts") or row.get("ts"))
    exit_ts = _to_epoch(row.get("close_ts") or row.get("exit_ts") or row.get("timestamp_utc"))
    entry_price = float(row.get("entry_price", 0.0) or 0.0)
    exit_price = float(row.get("exit_price", row.get("close_price", 0.0)) or 0.0)
    qty = float(row.get("qty_est", row.get("qty", 0.0)) or 0.0)
    notional = float(row.get("notional_usdt", 0.0) or 0.0)
    pnl_usdt = float(row.get("pnl_usdt", row.get("pnl", 0.0)) or 0.0)
    r_multiple_raw = row.get("r_multiple")
    r_multiple = float(r_multiple_raw) if isinstance(r_multiple_raw, (int, float)) else None
    exit_reason = str(row.get("close_reason", row.get("exit_reason", "")) or "")
    fee_usdt = float(row.get("fee_usdt", 0.0) or 0.0)
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO paper_trades (
                    position_id, symbol, side, strategy, entry_ts, exit_ts, entry_price, exit_price, qty, notional_usdt, pnl_usdt, r_multiple, exit_reason, fee_usdt, json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    symbol,
                    side,
                    strategy,
                    entry_ts,
                    exit_ts,
                    entry_price,
                    exit_price,
                    qty,
                    notional,
                    pnl_usdt,
                    r_multiple,
                    exit_reason,
                    fee_usdt,
                    json.dumps(row, ensure_ascii=True),
                ),
            )


def insert_trade_record(record: Dict[str, Any]) -> None:
    """Insert a strategy validation TradeRecord on paper ALLOW (open). intent_id is primary key."""
    _ensure_db()
    r = dict(record or {})
    intent_id = str(r.get("intent_id", "") or "").strip()
    if not intent_id:
        return
    ts_open = _to_epoch(r.get("ts_open") or r.get("entry_ts") or r.get("ts"))
    symbol = str(r.get("symbol", "") or "").upper()
    strategy = str(r.get("strategy", "") or r.get("setup", "") or "")
    side = str(r.get("side", "") or r.get("direction", "") or "").upper()
    tf = str(r.get("tf") or r.get("timeframe", "") or "")
    entry = float(r.get("entry") or r.get("entry_price", 0.0) or 0.0)
    sl = float(r.get("sl") or r.get("sl_price", 0.0) or 0.0)
    tp = float(r.get("tp") or r.get("tp_price", 0.0) or 0.0)
    sl_pct = _float_or_none(r.get("sl_pct"))
    tp_pct = _float_or_none(r.get("tp_pct"))
    confidence = _float_or_none(r.get("confidence") or r.get("score"))
    atr_pct = _float_or_none(r.get("atr_pct"))
    spread_bps = _float_or_none(r.get("spread_bps"))
    bias_flags = str(r.get("bias_flags", "") or "")[:500]
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_records (
                    intent_id, ts_open, symbol, strategy, side, tf, entry, sl, tp,
                    sl_pct, tp_pct, confidence, atr_pct, spread_bps, bias_flags, json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(intent_id) DO NOTHING
                """,
                (
                    intent_id,
                    ts_open,
                    symbol,
                    strategy,
                    side,
                    tf,
                    entry,
                    sl,
                    tp,
                    sl_pct,
                    tp_pct,
                    confidence,
                    atr_pct,
                    spread_bps,
                    bias_flags,
                    json.dumps(r, ensure_ascii=True),
                ),
            )


def _float_or_none(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def update_trade_record_on_close(intent_id: str, close_data: Dict[str, Any]) -> None:
    """Update trade_records row on paper close (TP/SL/timeout)."""
    _ensure_db()
    iid = str(intent_id or "").strip()
    if not iid:
        return
    c = dict(close_data or {})
    ts_close = _to_epoch(c.get("close_ts") or c.get("exit_ts") or c.get("ts"))
    close_price = _float_or_none(c.get("close_price") or c.get("exit_price"))
    close_reason = str(c.get("close_reason") or c.get("exit_reason", "") or "")
    pnl_usdt = _float_or_none(c.get("pnl_usdt") or c.get("pnl"))
    pnl_r = _float_or_none(c.get("pnl_r") or c.get("pnl_R") or c.get("r_multiple"))
    bars_held = c.get("bars_held")
    if bars_held is not None:
        try:
            bars_held = int(bars_held)
        except (TypeError, ValueError):
            bars_held = None
    mfe = _float_or_none(c.get("mfe"))
    mae = _float_or_none(c.get("mae"))
    with _WRITE_LOCK:
        with _connect() as conn:
            conn.execute(
                """
                UPDATE trade_records SET
                    ts_close = ?, close_price = ?, close_reason = ?,
                    pnl_usdt = ?, pnl_r = ?, bars_held = ?, mfe = ?, mae = ?
                WHERE intent_id = ?
                """,
                (ts_close, close_price, close_reason, pnl_usdt, pnl_r, bars_held, mfe, mae, iid),
            )


def get_trade_records_closed() -> List[Dict[str, Any]]:
    """Return all trade_records that have been closed (ts_close not null) for reporting."""
    _ensure_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT intent_id, ts_open, symbol, strategy, side, tf, entry, sl, tp,
                   sl_pct, tp_pct, confidence, atr_pct, spread_bps, bias_flags,
                   ts_close, close_price, close_reason, pnl_usdt, pnl_r, bars_held, mfe, mae
            FROM trade_records WHERE ts_close IS NOT NULL ORDER BY ts_close DESC
            """
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({
            "intent_id": str(row["intent_id"] or ""),
            "ts_open": int(row["ts_open"] or 0),
            "symbol": str(row["symbol"] or ""),
            "strategy": str(row["strategy"] or ""),
            "side": str(row["side"] or ""),
            "tf": str(row["tf"] or ""),
            "entry": float(row["entry"] or 0),
            "sl": float(row["sl"] or 0),
            "tp": float(row["tp"] or 0),
            "sl_pct": _float_or_none(row["sl_pct"]),
            "tp_pct": _float_or_none(row["tp_pct"]),
            "confidence": _float_or_none(row["confidence"]),
            "atr_pct": _float_or_none(row["atr_pct"]),
            "spread_bps": _float_or_none(row["spread_bps"]),
            "bias_flags": str(row["bias_flags"] or ""),
            "ts_close": int(row["ts_close"] or 0),
            "close_price": _float_or_none(row["close_price"]),
            "close_reason": str(row["close_reason"] or ""),
            "pnl_usdt": _float_or_none(row["pnl_usdt"]),
            "pnl_r": _float_or_none(row["pnl_r"]),
            "bars_held": int(row["bars_held"]) if row["bars_held"] is not None else None,
            "mfe": _float_or_none(row["mfe"]),
            "mae": _float_or_none(row["mae"]),
        })
    return out


def get_block_reasons_top_n(limit: int = 5) -> List[Dict[str, Any]]:
    """Top N block reasons by count (from trade_intents)."""
    _ensure_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT block_reason, COUNT(*) AS cnt FROM trade_intents
            WHERE (risk_verdict = 'BLOCK' OR status = 'BLOCKED') AND ts >= ?
            GROUP BY block_reason ORDER BY cnt DESC LIMIT ?
            """,
            (int(time.time()) - 7 * 24 * 60 * 60, max(1, int(limit))),
        ).fetchall()
    return [
        {"block_reason": str(row["block_reason"] or "").strip() or "(empty)", "count": int(row["cnt"])}
        for row in rows
    ]
