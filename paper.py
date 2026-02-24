from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Tuple


@dataclass
class PaperPosition:
    intent_id: str
    symbol: str
    side: str
    strategy: str
    entry_price: float
    notional_usdt: float
    qty_est: float
    atr_at_entry: float
    sl_price: float
    tp_price: float
    entry_ts: str
    last_ts: str
    bars_held: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "PaperPosition":
        return cls(
            intent_id=str(raw.get("intent_id", "")),
            symbol=str(raw.get("symbol", "")),
            side=str(raw.get("side", "")),
            strategy=str(raw.get("strategy", "")),
            entry_price=float(raw.get("entry_price", 0.0)),
            notional_usdt=float(raw.get("notional_usdt", 0.0)),
            qty_est=float(raw.get("qty_est", 0.0)),
            atr_at_entry=float(raw.get("atr_at_entry", 0.0)),
            sl_price=float(raw.get("sl_price", 0.0)),
            tp_price=float(raw.get("tp_price", 0.0)),
            entry_ts=str(raw.get("entry_ts", "")),
            last_ts=str(raw.get("last_ts", "")),
            bars_held=int(raw.get("bars_held", 0)),
        )


def open_paper_position(
    intent,
    price: float,
    atr: float,
    ts: str,
    *,
    sl_atr_mult: float = 1.0,
    tp_atr_mult: float = 1.5,
    paper_equity_usdt: float = 200.0,
    risk_per_trade_pct: float = 0.25,
    max_notional_usdt: float = 50.0,
) -> PaperPosition:
    entry = float(price)
    atr_value = max(0.0, float(atr))

    side = str(getattr(intent, "side", "")).upper()
    if side == "SHORT":
        sl_price = entry + (sl_atr_mult * atr_value)
        tp_price = entry - (tp_atr_mult * atr_value)
    else:
        side = "LONG"
        sl_price = entry - (sl_atr_mult * atr_value)
        tp_price = entry + (tp_atr_mult * atr_value)

    sl_distance = abs(entry - float(sl_price))
    equity = max(0.0, float(paper_equity_usdt))
    risk_pct = max(0.0, float(risk_per_trade_pct))
    max_notional = max(0.0, float(max_notional_usdt))
    risk_usdt = equity * (risk_pct / 100.0)
    sl_pct = sl_distance / max(entry, 1e-10)
    if sl_pct > 0:
        risk_based_notional = risk_usdt / sl_pct
    else:
        risk_based_notional = max_notional
    notional = min(max_notional, risk_based_notional)
    notional = max(0.0, notional)
    qty_est = notional / max(entry, 1e-10)

    return PaperPosition(
        intent_id=str(getattr(intent, "intent_id", "")),
        symbol=str(getattr(intent, "symbol", "")),
        side=side,
        strategy=str(getattr(intent, "strategy", "")),
        entry_price=entry,
        notional_usdt=notional,
        qty_est=qty_est,
        atr_at_entry=atr_value,
        sl_price=float(sl_price),
        tp_price=float(tp_price),
        entry_ts=str(ts),
        last_ts=str(ts),
        bars_held=0,
    )


def update_and_maybe_close(
    position: PaperPosition,
    last_candle: Dict[str, Any],
    fees_bps: float,
    timeout_bars: int,
) -> Tuple[PaperPosition, bool, float, str]:
    high = float(last_candle.get("high", 0.0))
    low = float(last_candle.get("low", 0.0))
    close = float(last_candle.get("close", 0.0))
    ts = str(last_candle.get("timestamp_utc", position.last_ts))

    updated = PaperPosition(
        intent_id=position.intent_id,
        symbol=position.symbol,
        side=position.side,
        strategy=position.strategy,
        entry_price=position.entry_price,
        notional_usdt=position.notional_usdt,
        qty_est=position.qty_est,
        atr_at_entry=position.atr_at_entry,
        sl_price=position.sl_price,
        tp_price=position.tp_price,
        entry_ts=position.entry_ts,
        last_ts=ts,
        bars_held=position.bars_held + 1,
    )

    side = updated.side.upper()
    if side == "SHORT":
        sl_hit = high >= updated.sl_price
        tp_hit = low <= updated.tp_price
    else:
        sl_hit = low <= updated.sl_price
        tp_hit = high >= updated.tp_price

    close_reason = ""
    exit_price = close
    closed = False
    if sl_hit:
        closed = True
        close_reason = "SL"
        exit_price = updated.sl_price
    elif tp_hit:
        closed = True
        close_reason = "TP"
        exit_price = updated.tp_price
    elif timeout_bars > 0 and updated.bars_held >= timeout_bars:
        closed = True
        close_reason = "TIMEOUT"
        exit_price = close

    if not closed:
        return updated, False, 0.0, ""

    qty = updated.qty_est
    if side == "SHORT":
        gross_pnl = (updated.entry_price - exit_price) * qty
    else:
        gross_pnl = (exit_price - updated.entry_price) * qty
    fees_usdt = updated.notional_usdt * (float(fees_bps) / 10000.0) * 2.0
    pnl_usdt = gross_pnl - fees_usdt
    return updated, True, float(pnl_usdt), close_reason
