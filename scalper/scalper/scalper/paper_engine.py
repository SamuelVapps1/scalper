"""
DRY RUN PaperEngine: simulates fills and closes from candles only.
No exchange private endpoints; deterministic.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from paper import PaperPosition, open_paper_position
from scalper.trade_preview import build_trade_preview


def _get_spread_bps() -> float:
    try:
        import config
        return max(0.0, float(getattr(config, "SPREAD_BPS", 2.0)))
    except Exception:
        return 2.0


def _get_slippage_bps() -> float:
    try:
        import config
        return max(0.0, float(getattr(config, "SLIPPAGE_BPS", 3.0)))
    except Exception:
        return 3.0


def _apply_spread_slippage(level: float, side: str) -> float:
    """effective fill price = level +/- (spread+slip) in bps."""
    spread = _get_spread_bps()
    slip = _get_slippage_bps()
    adj_bps = spread + slip
    adj = level * (adj_bps / 10000.0)
    if side == "LONG":
        return level + adj
    return level - adj


def _candle_close(candle: Dict[str, Any]) -> float:
    return float(candle.get("close", 0.0) or 0.0)


def _candle_low(candle: Dict[str, Any]) -> float:
    return float(candle.get("low", 0.0) or 0.0)


def _candle_high(candle: Dict[str, Any]) -> float:
    return float(candle.get("high", 0.0) or 0.0)


def _candle_ts(candle: Dict[str, Any], fallback: str = "") -> str:
    ts = str(candle.get("timestamp_utc", "") or candle.get("ts", "") or fallback).strip()
    return ts or fallback


def open_from_preview(
    preview: Dict[str, Any],
    *,
    intent_id: str,
    ts: str,
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not preview or not bool(preview.get("ok")):
        return (None, str((preview or {}).get("reason") or "PREVIEW_BUILD_FAILED"))
    if not bool(preview.get("executable")):
        return (None, str(preview.get("reason") or "PREVIEW_NOT_EXECUTABLE"))

    symbol = str(preview.get("symbol", "") or "").strip().upper()
    side = str(preview.get("side", "") or "").strip().upper()
    strategy = str(preview.get("strategy", "") or "").strip()
    entry_price = float(preview.get("entry", 0.0) or 0.0)
    sl_price = float(preview.get("sl", 0.0) or 0.0)
    tp_price = float(preview.get("tp", 0.0) or 0.0)
    if not symbol or side not in ("LONG", "SHORT"):
        return (None, "invalid_preview_intent")
    if entry_price <= 0 or sl_price <= 0 or tp_price <= 0:
        return (None, "invalid_preview_levels")

    # Legacy paper engine path still applies spread/slippage to entry fill only.
    fill_entry = _apply_spread_slippage(entry_price, side)
    if side == "LONG" and not (sl_price < fill_entry < tp_price):
        return (None, "fill_geometry_invalid_long")
    if side == "SHORT" and not (tp_price < fill_entry < sl_price):
        return (None, "fill_geometry_invalid_short")

    notional_override = float(preview.get("notional", 0.0) or 0.0)
    if notional_override <= 0:
        return (None, "preview_notional_zero")
    qty_override = notional_override / max(fill_entry, 1e-10)
    atr_value = float(preview.get("atr_used", 0.0) or 0.0)

    intent_obj = type(
        "Intent",
        (),
        {
            "symbol": symbol,
            "side": side,
            "strategy": strategy,
            "intent_id": intent_id or f"{symbol}|{strategy}|{side}|{ts}",
        },
    )()
    position = open_paper_position(
        intent=intent_obj,
        price=fill_entry,
        atr=max(0.0, atr_value),
        ts=ts,
        sl_price_override=sl_price,
        tp_r_mult_override=None,
        risk_per_trade_pct=0.0,
        max_notional_usdt=max(0.0, notional_override),
        paper_equity_usdt=max(0.0, notional_override),
    )
    pos_dict = position.to_dict()
    pos_dict["notional_usdt"] = notional_override
    pos_dict["qty_est"] = qty_override
    pos_dict["intent_id"] = intent_id or pos_dict.get("intent_id", "")
    pos_dict["status"] = "OPEN"
    pos_dict["symbol"] = symbol
    pos_dict["fill_price"] = float(position.entry_price)
    pos_dict["preview_entry_price"] = entry_price
    pos_dict["tp_price"] = tp_price
    pos_dict["atr_source"] = str(preview.get("atr_source", ""))
    return (pos_dict, None)


def try_open_position(
    trade_intent: Dict[str, Any],
    candles: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    *,
    paper_position_usdt: float,
    sl_atr_mult: float,
    tp_atr_mult: float,
    intent_id: str = "",
    preview: Optional[Dict[str, Any]] = None,
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    If intent.entry_type == "market": open at latest candle close.
    If entry_type == "retest": open when a candle low/high touches retest level (from intent meta).
    With RETEST_CONFIRM_MODE=bos for RANGE_BREAKOUT_RETEST_GO: require BOS within EARLY_LOOKBACK_5M after retest.
    Returns (position_dict, skip_reason). skip_reason is "bos_not_confirmed" when BOS not met, else None.
    """
    if preview is None:
        preview = build_trade_preview(
            signal=trade_intent,
            market_snapshot=snapshot,
            candles=candles,
            risk_settings=type(
                "RiskSettings",
                (),
                {
                    "paper_sl_atr": sl_atr_mult,
                    "paper_tp_atr": tp_atr_mult,
                    "risk_per_trade_pct": 0.0,
                    "paper_start_equity_usdt": max(0.0, paper_position_usdt),
                    "preview_min_rr": 1.0,
                    "preview_min_atr_pct": 0.0,
                    "preview_max_atr_pct": 100.0,
                    "preview_max_retest_drift_pct": 999.0,
                },
            )(),
            equity_usdt=max(0.0, paper_position_usdt),
            for_execution=True,
        )
        if preview.get("ok"):
            preview["qty"] = max(0.0, paper_position_usdt) / max(float(preview.get("entry", 0.0) or 0.0), 1e-10)
            preview["notional"] = max(0.0, paper_position_usdt)
    if not preview.get("ok"):
        logging.debug("PaperEngine skip open: preview_failed reason=%s", preview.get("reason"))
        return (None, str(preview.get("reason") or "preview_failed"))
    ts_open = str(
        snapshot.get("ts")
        or snapshot.get("bar_ts_used")
        or (candles[-1] if candles else {}).get("timestamp_utc", "")
    )
    return open_from_preview(
        preview=preview,
        intent_id=intent_id,
        ts=ts_open or str((candles[-1] if candles else {}).get("timestamp_utc", "")),
    )
