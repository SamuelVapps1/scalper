from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Tuple


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
    max_favorable_price: float = 0.0
    min_adverse_price: float = 0.0
    be_moved: bool = False
    partial_taken: bool = False
    leverage_recommended: float = 0.0
    margin_mode: str = "isolated"
    planned_entry_price: float = 0.0
    planned_sl_price: float = 0.0
    planned_tp_price: float = 0.0
    planned_rr: float = 0.0

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
            max_favorable_price=float(raw.get("max_favorable_price", 0.0) or 0.0),
            min_adverse_price=float(raw.get("min_adverse_price", 0.0) or 0.0),
            be_moved=bool(raw.get("be_moved", False)),
            partial_taken=bool(raw.get("partial_taken", False)),
            leverage_recommended=float(raw.get("leverage_recommended", 0.0) or 0.0),
            margin_mode=str(raw.get("margin_mode", "isolated") or "isolated"),
            planned_entry_price=float(raw.get("planned_entry_price", 0.0) or 0.0),
            planned_sl_price=float(raw.get("planned_sl_price", 0.0) or 0.0),
            planned_tp_price=float(raw.get("planned_tp_price", 0.0) or 0.0),
            planned_rr=float(raw.get("planned_rr", 0.0) or 0.0),
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
    sl_price_override: Optional[float] = None,
    tp_r_mult_override: Optional[float] = None,
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

    if sl_price_override is not None:
        sl_price = float(sl_price_override)
    if tp_r_mult_override is not None:
        r = abs(entry - sl_price)
        if side == "LONG":
            tp_price = entry + float(tp_r_mult_override) * r
        else:
            tp_price = entry - float(tp_r_mult_override) * r

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
        planned_entry_price=entry,
        planned_sl_price=float(sl_price),
        planned_tp_price=float(tp_price),
    )


def _get_paper_exit_config() -> Dict[str, float]:
    """Lazy load paper exit config to avoid circular imports."""
    try:
        import config as _cfg
        return {
            "be_at_r": float(getattr(_cfg, "BE_AT_R", 1.0)),
            "partial_tp_at_r": float(getattr(_cfg, "PARTIAL_TP_AT_R", 0.0)),
            "partial_tp_pct": float(getattr(_cfg, "PARTIAL_TP_PCT", 0.5)),
            "trail_after_r": float(getattr(_cfg, "TRAIL_AFTER_R", 0.0)),
            "trail_atr_mult": float(getattr(_cfg, "TRAIL_ATR_MULT", 1.0)),
        }
    except Exception:
        return {"be_at_r": 1.0, "partial_tp_at_r": 0.0, "partial_tp_pct": 0.5, "trail_after_r": 0.0, "trail_atr_mult": 1.0}


def update_and_maybe_close(
    position: PaperPosition,
    last_candle: Dict[str, Any],
    fees_bps: float,
    timeout_bars: int,
    *,
    replay_strict_exit: bool = False,
) -> Tuple[PaperPosition, bool, float, str, Optional[Dict[str, Any]]]:
    """
    Update position and maybe close. Returns (updated, closed, pnl, reason, partial_trade).
    partial_trade is a dict for a partial TP fill when applicable; caller adds to closed_trades.
    """
    high = float(last_candle.get("high", 0.0))
    low = float(last_candle.get("low", 0.0))
    close = float(last_candle.get("close", 0.0))
    ts = str(last_candle.get("timestamp_utc", position.last_ts))

    cfg = _get_paper_exit_config()
    side = position.side.upper()
    entry = position.entry_price
    sl = position.sl_price
    risk_per_unit = abs(entry - sl)
    atr_val = max(0.0, position.atr_at_entry)

    # Update max_favorable_price and min_adverse_price:
    # - LONG: favorable = higher prices, adverse = lower prices
    # - SHORT: favorable = lower prices, adverse = higher prices
    mfp = position.max_favorable_price
    min_adv = position.min_adverse_price
    if side == "LONG":
        mfp = max(mfp, high) if mfp > 0 else high
        min_adv = min(min_adv, low) if min_adv > 0 else low
    else:
        mfp = min(mfp, low) if (mfp > 0 and low > 0) else (low if low > 0 else mfp)
        min_adv = max(min_adv, high) if min_adv > 0 else high

    # Current R using close (or high/low for intra-bar)
    if side == "LONG":
        current_r = (close - entry) / risk_per_unit if risk_per_unit > 0 else 0.0
        favorable_r = (mfp - entry) / risk_per_unit if risk_per_unit > 0 else 0.0
    else:
        current_r = (entry - close) / risk_per_unit if risk_per_unit > 0 else 0.0
        favorable_r = (entry - mfp) / risk_per_unit if risk_per_unit > 0 else 0.0

    be_moved = position.be_moved
    partial_taken = position.partial_taken
    partial_trade: Optional[Dict[str, Any]] = None

    # 1. BE move once when R >= BE_AT_R
    if not be_moved and favorable_r >= cfg["be_at_r"] and cfg["be_at_r"] > 0:
        sl = entry
        be_moved = True
        risk_per_unit = abs(entry - sl)

    # 2. Partial TP once when R >= PARTIAL_TP_AT_R (skipped in replay strict/hard mode)
    if not replay_strict_exit and not partial_taken and cfg["partial_tp_at_r"] > 0 and favorable_r >= cfg["partial_tp_at_r"]:
        pct = min(1.0, max(0.01, cfg["partial_tp_pct"]))
        partial_qty = position.qty_est * pct
        partial_notional = position.notional_usdt * pct
        # Exit at PARTIAL_TP_AT_R level
        if side == "LONG":
            partial_exit_price = entry + cfg["partial_tp_at_r"] * risk_per_unit
            partial_pnl = (partial_exit_price - entry) * partial_qty
        else:
            partial_exit_price = entry - cfg["partial_tp_at_r"] * risk_per_unit
            partial_pnl = (entry - partial_exit_price) * partial_qty
        partial_fees = partial_notional * (float(fees_bps) / 10000.0) * 2.0
        partial_pnl_net = partial_pnl - partial_fees
        partial_risk = risk_per_unit * partial_qty
        partial_r = (partial_pnl_net / partial_risk) if partial_risk > 0 else None
        partial_trade = {
            "symbol": position.symbol,
            "side": position.side,
            "setup": position.strategy,
            "entry_ts": position.entry_ts,
            "close_ts": ts,
            "pnl_usdt": float(partial_pnl_net),
            "close_reason": "PARTIAL_TP",
            "entry_price": entry,
            "sl_price": sl,
            "tp_price": partial_exit_price,
            "notional_usdt": partial_notional,
            "qty_est": partial_qty,
            "risk_usdt": partial_risk,
            "r_multiple": partial_r,
            "partial": 1,
        }
        partial_taken = True
        # Reduce position
        position = PaperPosition(
            intent_id=position.intent_id,
            symbol=position.symbol,
            side=position.side,
            strategy=position.strategy,
            entry_price=position.entry_price,
            notional_usdt=position.notional_usdt * (1 - pct),
            qty_est=position.qty_est * (1 - pct),
            atr_at_entry=position.atr_at_entry,
            sl_price=sl,
            tp_price=position.tp_price,
            entry_ts=position.entry_ts,
            last_ts=ts,
            bars_held=position.bars_held + 1,
            max_favorable_price=mfp,
            min_adverse_price=min_adv,
            be_moved=be_moved,
            partial_taken=partial_taken,
        )
        # Continue with reduced position; don't close yet
        return position, False, 0.0, "", partial_trade

    # 3. Trail SL when R >= TRAIL_AFTER_R
    if cfg["trail_after_r"] > 0 and favorable_r >= cfg["trail_after_r"] and atr_val > 0:
        trail_dist = cfg["trail_atr_mult"] * atr_val
        if side == "LONG":
            trail_sl = mfp - trail_dist
            if trail_sl > sl:
                sl = trail_sl
        else:
            trail_sl = mfp + trail_dist
            if trail_sl < sl:
                sl = trail_sl

    updated = PaperPosition(
        intent_id=position.intent_id,
        symbol=position.symbol,
        side=position.side,
        strategy=position.strategy,
        entry_price=position.entry_price,
        notional_usdt=position.notional_usdt,
        qty_est=position.qty_est,
        atr_at_entry=position.atr_at_entry,
        sl_price=sl,
        tp_price=position.tp_price,
        entry_ts=position.entry_ts,
        last_ts=ts,
        bars_held=position.bars_held + 1,
        max_favorable_price=mfp,
        min_adverse_price=min_adv,
        be_moved=be_moved,
        partial_taken=partial_taken,
    )

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
    elif not replay_strict_exit and timeout_bars > 0 and updated.bars_held >= timeout_bars:
        closed = True
        close_reason = "TIMEOUT"
        exit_price = close

    if not closed:
        return updated, False, 0.0, "", None

    qty = updated.qty_est
    if side == "SHORT":
        gross_pnl = (updated.entry_price - exit_price) * qty
    else:
        gross_pnl = (exit_price - updated.entry_price) * qty
    fees_usdt = updated.notional_usdt * (float(fees_bps) / 10000.0) * 2.0
    pnl_usdt = gross_pnl - fees_usdt
    return updated, True, float(pnl_usdt), close_reason, None
