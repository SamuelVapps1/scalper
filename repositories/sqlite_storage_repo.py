from __future__ import annotations

from typing import Any, Dict, List, Optional

import sqlite_store


class SQLiteStorageRepository:
    def store_signal(self, signal: Dict[str, Any]) -> bool:
        return sqlite_store.store_signal(signal)

    def get_recent_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        return sqlite_store.get_recent_signals(limit=limit)

    def count_signals_since(self, ts: int) -> int:
        return sqlite_store.count_signals_since(int(ts))

    def store_trade_intent(self, intent: Dict[str, Any]) -> None:
        sqlite_store.store_trade_intent(intent)

    def get_recent_trade_intents(self, limit: int = 50) -> List[Dict[str, Any]]:
        return sqlite_store.get_recent_trade_intents(limit=limit)

    def get_block_stats_last_24h(self) -> List[Dict[str, Any]]:
        return sqlite_store.get_block_stats_last_24h()

    def store_risk_event(self, event: Dict[str, Any]) -> None:
        sqlite_store.store_risk_event(event)

    def get_recent_risk_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        return sqlite_store.get_recent_risk_events(limit=limit)

    def kv_get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return sqlite_store.kv_get(key, default)

    def kv_set(self, key: str, value: str) -> None:
        sqlite_store.kv_set(key, value)

    def sync_positions_and_fills(
        self,
        open_positions: List[Dict[str, Any]],
        closed_trades: List[Dict[str, Any]],
    ) -> None:
        sqlite_store.sync_positions_and_fills(
            open_positions=open_positions,
            closed_trades=closed_trades,
        )

    def upsert_paper_position(self, position: Dict[str, Any]) -> None:
        sqlite_store.upsert_paper_position(position)

    def delete_paper_position(self, position_id: str) -> None:
        sqlite_store.delete_paper_position(position_id)

    def insert_paper_trade(self, trade: Dict[str, Any]) -> None:
        sqlite_store.insert_paper_trade(trade)

    def insert_trade_record(self, record: Dict[str, Any]) -> None:
        sqlite_store.insert_trade_record(record)

    def update_trade_record_on_close(self, intent_id: str, close_data: Dict[str, Any]) -> None:
        sqlite_store.update_trade_record_on_close(intent_id, close_data)

    def get_trade_records_closed(self) -> List[Dict[str, Any]]:
        return sqlite_store.get_trade_records_closed()

    def get_block_reasons_top_n(self, limit: int = 5) -> List[Dict[str, Any]]:
        return sqlite_store.get_block_reasons_top_n(limit=limit)


_REPO = SQLiteStorageRepository()


def get_storage_repo() -> SQLiteStorageRepository:
    return _REPO

