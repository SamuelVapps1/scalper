"""
DRY RUN PaperEngine: simulates fills and closes from candles only.
No exchange private endpoints; deterministic.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from paper import PaperPosition, open_paper_position


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


def try_open_position(
    trade_intent: Dict[str, Any],
    candles: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    *,
    paper_position_usdt: float,
    sl_atr_mult: float,
    tp_atr_mult: float,
    intent_id: str = "",
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    If intent.entry_type == "market": open at latest candle close.
    If entry_type == "retest": open when a candle low/high touches retest level (from intent meta).
    With RETEST_CONFIRM_MODE=bos for RANGE_BREAKOUT_RETEST_GO: require BOS within EARLY_LOOKBACK_5M after retest.
    Returns (position_dict, skip_reason). skip_reason is "bos_not_confirmed" when BOS not met, else None.
    """
    if not candles:
        return (None, None)
    symbol = str(trade_intent.get("symbol", "") or "").strip().upper()
    side = str(trade_intent.get("side", "") or trade_intent.get("direction", "") or "").strip().upper()
    strategy = str(trade_intent.get("setup", "") or trade_intent.get("strategy", "") or "").strip()
    if not symbol or side not in ("LONG", "SHORT"):
        return (None, None)
    raw_entry = str(trade_intent.get("entry_type", "market") or "market").strip().lower()
    if raw_entry in ("market", "market_sim") or (raw_entry and "market" in raw_entry):
        entry_type = "market"
    elif raw_entry and "retest" in raw_entry:
        entry_type = "retest"
    else:
        entry_type = raw_entry or "market"
    meta = trade_intent.get("meta") if isinstance(trade_intent.get("meta"), dict) else {}
    sl_hint = meta.get("sl_hint")
    tp_r_mult = meta.get("tp_r_mult")
    sl_price_override = float(sl_hint) if sl_hint is not None and isinstance(sl_hint, (int, float)) else None
    try:
        tp_r_mult_override = float(tp_r_mult) if tp_r_mult is not None else None
    except (TypeError, ValueError):
        tp_r_mult_override = None
    retest_level = None
    if "retest_level" in meta:
        try:
            retest_level = float(meta["retest_level"])
        except (TypeError, ValueError):
            pass
    if retest_level is None and "level_ref" in trade_intent:
        try:
            retest_level = float(trade_intent["level_ref"])
        except (TypeError, ValueError):
            pass

    entry_price: Optional[float] = None
    entry_ts: Optional[str] = None

    if entry_type == "market":
        # market_sim: fill at first bar (immediate); market: fill at last bar
        fill_candle = candles[0] if raw_entry == "market_sim" else candles[-1]
        raw_level = _candle_close(fill_candle)
        entry_price = _apply_spread_slippage(raw_level, side)
        entry_ts = _candle_ts(fill_candle)
    elif entry_type == "retest" and retest_level is not None:
        try:
            import config
            retest_confirm = getattr(config, "RETEST_CONFIRM_MODE", "bos")
            bos_lookback = int(getattr(config, "BOS_LOOKBACK_5M", 20))
            early_lookback = int(getattr(config, "EARLY_LOOKBACK_5M", 180))
        except Exception:
            retest_confirm = "bos"
            bos_lookback = 20
            early_lookback = 180

        retest_idx: Optional[int] = None
        for i, c in enumerate(candles):
            low = _candle_low(c)
            high = _candle_high(c)
            if side == "LONG" and low <= retest_level:
                retest_idx = i
                break
            if side == "SHORT" and high >= retest_level:
                retest_idx = i
                break

        if retest_idx is None:
            pass
        elif retest_confirm == "bos" and strategy == "RANGE_BREAKOUT_RETEST_GO":
            end_idx = min(retest_idx + early_lookback, len(candles) - 1)
            bos_confirmed = False
            for i in range(retest_idx + 1, end_idx + 1):
                if i < bos_lookback:
                    continue
                prev_highs = [_candle_high(candles[j]) for j in range(i - bos_lookback, i)]
                prev_lows = [_candle_low(candles[j]) for j in range(i - bos_lookback, i)]
                close_i = _candle_close(candles[i])
                if side == "LONG" and close_i > max(prev_highs):
                    bos_confirmed = True
                    raw_level = retest_level
                    entry_price = _apply_spread_slippage(raw_level, side)
                    entry_ts = _candle_ts(candles[i])
                    break
                if side == "SHORT" and close_i < min(prev_lows):
                    bos_confirmed = True
                    raw_level = retest_level
                    entry_price = _apply_spread_slippage(raw_level, side)
                    entry_ts = _candle_ts(candles[i])
                    break
            if not bos_confirmed:
                return (None, "bos_not_confirmed")
        else:
            c = candles[retest_idx]
            raw_level = retest_level
            entry_price = _apply_spread_slippage(raw_level, side)
            entry_ts = _candle_ts(c)
    else:
        logging.debug(
            "PaperEngine skip open: entry_type=%s retest_level=%s",
            entry_type,
            retest_level,
        )
        return (None, None)

    if entry_price is None or entry_price <= 0 or not entry_ts:
        return (None, None)

    atr_value = float(snapshot.get("atr14", 0.0) or 0.0)
    if atr_value <= 0:
        logging.debug("PaperEngine skip open: atr14 not ready")
        return (None, None)

    intent_obj = type("Intent", (), {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "intent_id": intent_id or f"{symbol}|{strategy}|{side}|{entry_ts}",
    })()
    position = open_paper_position(
        intent=intent_obj,
        price=entry_price,
        atr=atr_value,
        ts=entry_ts,
        sl_atr_mult=sl_atr_mult,
        tp_atr_mult=tp_atr_mult,
        paper_equity_usdt=max(0.0, paper_position_usdt),
        risk_per_trade_pct=0.0,
        max_notional_usdt=max(0.0, paper_position_usdt),
        sl_price_override=sl_price_override,
        tp_r_mult_override=tp_r_mult_override,
    )
    notional_override = max(0.0, paper_position_usdt)
    qty_override = notional_override / max(position.entry_price, 1e-10)
    pos_dict = position.to_dict()
    pos_dict["notional_usdt"] = notional_override
    pos_dict["qty_est"] = qty_override
    pos_dict["intent_id"] = intent_id or pos_dict.get("intent_id", "")
    pos_dict["status"] = "OPEN"
    pos_dict["symbol"] = symbol
    pos_dict["fill_price"] = float(position.entry_price)
    return (pos_dict, None)
