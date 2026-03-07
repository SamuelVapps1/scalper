from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class StorageRepository(Protocol):
    def store_signal(self, signal: Dict[str, Any]) -> bool:
        ...

    def get_recent_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        ...

    def count_signals_since(self, ts: int) -> int:
        ...

    def store_trade_intent(self, intent: Dict[str, Any]) -> None:
        ...

    def get_recent_trade_intents(self, limit: int = 50) -> List[Dict[str, Any]]:
        ...

    def get_block_stats_last_24h(self) -> List[Dict[str, Any]]:
        ...

    def store_risk_event(self, event: Dict[str, Any]) -> None:
        ...

    def get_recent_risk_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        ...

    def kv_get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        ...

    def kv_set(self, key: str, value: str) -> None:
        ...

    def sync_positions_and_fills(
        self,
        open_positions: List[Dict[str, Any]],
        closed_trades: List[Dict[str, Any]],
    ) -> None:
        ...

    def upsert_paper_position(self, position: Dict[str, Any]) -> None:
        ...

    def delete_paper_position(self, position_id: str) -> None:
        ...

    def insert_paper_trade(self, trade: Dict[str, Any]) -> None:
        ...

