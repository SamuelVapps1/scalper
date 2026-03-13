from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class PaperBroker:
    store: Any
    risk_settings: Any

    def _slippage_pct(self) -> float:
        return max(0.0, float(getattr(self.risk_settings, "paper_slippage_pct", 0.01) or 0.01))

    def _fee_pct(self) -> float:
        return max(0.0, float(getattr(self.risk_settings, "paper_fee_pct", 0.055) or 0.055))

    def _start_equity(self) -> float:
        return max(0.0, float(getattr(self.risk_settings, "paper_start_equity_usdt", 1000.0) or 1000.0))

    def current_equity(self) -> float:
        state = self.store.load_paper_state()
        realized = float(state.get("daily_pnl_realized", 0.0) or 0.0)
        peak = float(state.get("equity_peak", 0.0) or 0.0)
        base = peak if peak > 0 else self._start_equity()
        return max(0.0, base + realized)

    def _entry_price_with_slippage(self, close_price: float, side: str) -> float:
        slip = self._slippage_pct() / 100.0
        if str(side).upper() == "SHORT":
            return float(close_price) * (1.0 - slip)
        return float(close_price) * (1.0 + slip)

    def open_from_intent(
        self,
        *,
        intent: Dict[str, Any],
        candle: Dict[str, Any],
        strategy: str,
        fallback_atr: float,
        intent_id: str,
        ts: str,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        symbol = str(intent.get("symbol", "") or "").upper()
        side = str(intent.get("side", "") or "").upper()
        if not symbol or side not in {"LONG", "SHORT"}:
            return (None, "invalid_intent")
        close_price = float(candle.get("close", 0.0) or 0.0)
        if close_price <= 0:
            return (None, "invalid_price")
        meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
        sl_hint = meta.get("sl_hint")
        entry = self._entry_price_with_slippage(close_price, side)

        if isinstance(sl_hint, (int, float)):
            sl_price = float(sl_hint)
        else:
            atr = max(0.0, float(fallback_atr))
            sl_mult = max(0.1, float(getattr(self.risk_settings, "paper_sl_atr", 1.0) or 1.0))
            sl_price = entry - (sl_mult * atr) if side == "LONG" else entry + (sl_mult * atr)

        sl_distance = abs(entry - sl_price)
        if sl_distance <= 0:
            return (None, "sl_distance_zero")

        risk_pct = max(0.0, float(getattr(self.risk_settings, "risk_per_trade_pct", 0.15) or 0.15))
        risk_usdt = self.current_equity() * (risk_pct / 100.0)
        qty = risk_usdt / sl_distance if sl_distance > 0 else 0.0
        if qty <= 0:
            return (None, "qty_zero")
        notional_usdt = qty * entry

        tp_r_mult = meta.get("tp_r_mult")
        if isinstance(tp_r_mult, (int, float)):
            tp_price = entry + (float(tp_r_mult) * sl_distance if side == "LONG" else -float(tp_r_mult) * sl_distance)
        else:
            tp_mult = max(0.1, float(getattr(self.risk_settings, "paper_tp_atr", 1.5) or 1.5))
            tp_price = entry + (tp_mult * sl_distance if side == "LONG" else -tp_mult * sl_distance)

        pos = {
            "intent_id": str(intent_id or ""),
            "symbol": symbol,
            "side": side,
            "strategy": str(strategy or ""),
            "entry_price": float(entry),
            "notional_usdt": float(notional_usdt),
            "qty_est": float(qty),
            "atr_at_entry": float(max(0.0, fallback_atr)),
            "sl_price": float(sl_price),
            "tp_price": float(tp_price),
            "entry_ts": str(ts or ""),
            "last_ts": str(ts or ""),
            "bars_held": 0,
            "max_favorable_price": 0.0,
            "be_moved": False,
            "partial_taken": False,
            "status": "OPEN",
            "fee_pct": self._fee_pct(),
        }
        return (pos, None)

    def persist_open(self, position: Dict[str, Any]) -> None:
        self.store.upsert_paper_position(position)

    def persist_close(self, trade: Dict[str, Any]) -> None:
        self.store.insert_paper_trade(trade)
        position_id = str(trade.get("intent_id", "") or trade.get("position_id", "")).strip()
        if position_id:
            self.store.delete_paper_position(position_id)
