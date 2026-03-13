from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from scalper.trade_preview import build_trade_preview


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

    def open_from_preview(
        self,
        *,
        preview: Dict[str, Any],
        intent_id: str,
        ts: str,
        strategy: str,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not preview or not bool(preview.get("ok")):
            return (None, str((preview or {}).get("reason") or "PREVIEW_BUILD_FAILED"))
        if not bool(preview.get("executable")):
            return (None, str(preview.get("reason") or "PREVIEW_NOT_EXECUTABLE"))

        symbol = str(preview.get("symbol", "") or "").upper()
        side = str(preview.get("side", "") or "").upper()
        entry_preview = float(preview.get("entry", 0.0) or 0.0)
        if not symbol or side not in {"LONG", "SHORT"} or entry_preview <= 0:
            return (None, "INVALID_PREVIEW")

        fill_entry = self._entry_price_with_slippage(entry_preview, side)
        sl_price = float(preview.get("sl", 0.0) or 0.0)
        tp_price = float(preview.get("tp", 0.0) or 0.0)
        if sl_price <= 0 or tp_price <= 0:
            return (None, "INVALID_PREVIEW_LEVELS")
        if side == "LONG" and not (sl_price < fill_entry < tp_price):
            return (None, "FILL_GEOMETRY_INVALID_LONG")
        if side == "SHORT" and not (tp_price < fill_entry < sl_price):
            return (None, "FILL_GEOMETRY_INVALID_SHORT")

        sl_distance = abs(fill_entry - sl_price)
        if sl_distance <= 0:
            return (None, "SL_DISTANCE_ZERO")

        risk_pct = max(0.0, float(getattr(self.risk_settings, "risk_per_trade_pct", 0.15) or 0.15))
        risk_usdt = self.current_equity() * (risk_pct / 100.0)
        qty = risk_usdt / sl_distance if sl_distance > 0 else 0.0
        if qty <= 0:
            return (None, "QTY_ZERO")
        notional_usdt = qty * fill_entry

        pos = {
            "intent_id": str(intent_id or ""),
            "symbol": symbol,
            "side": side,
            "strategy": str(strategy or ""),
            "entry_price": float(fill_entry),
            "preview_entry_price": float(entry_preview),
            "notional_usdt": float(notional_usdt),
            "qty_est": float(qty),
            "atr_at_entry": float(preview.get("atr_used", 0.0) or 0.0),
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
            "preview_reason": str(preview.get("reason", "")),
            "atr_source": str(preview.get("atr_source", "")),
            "bar_ts_used": str(preview.get("bar_ts_used", "")),
            # Planned vs actual (from canonical trade plan, if present on preview).
            "planned_entry_price": float(preview.get("planned_entry", entry_preview)),
            "planned_sl_price": float(preview.get("planned_sl", sl_price)),
            "planned_tp_price": float(preview.get("planned_tp", tp_price)),
            "planned_rr": float(preview.get("plan_rr", 0.0) or 0.0),
            "leverage_recommended": float(preview.get("plan_leverage", 0.0) or 0.0),
            "margin_mode": str(preview.get("margin_mode", "isolated") or "isolated"),
        }
        return (pos, None)

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
        market_snapshot = {"close": float(candle.get("close", 0.0) or 0.0), "atr14": float(fallback_atr or 0.0)}
        preview = build_trade_preview(
            signal={
                **dict(intent or {}),
                "strategy": str(strategy or intent.get("strategy", intent.get("setup", ""))),
            },
            market_snapshot=market_snapshot,
            candles=[dict(candle or {})] if candle else [],
            risk_settings=self.risk_settings,
            equity_usdt=self.current_equity(),
            for_execution=True,
        )
        return self.open_from_preview(
            preview=preview,
            intent_id=intent_id,
            ts=ts,
            strategy=str(strategy or intent.get("strategy", intent.get("setup", ""))),
        )

    def persist_open(self, position: Dict[str, Any]) -> None:
        self.store.upsert_paper_position(position)

    def persist_close(self, trade: Dict[str, Any]) -> None:
        self.store.insert_paper_trade(trade)
        position_id = str(trade.get("intent_id", "") or trade.get("position_id", "")).strip()
        if position_id:
            self.store.delete_paper_position(position_id)
