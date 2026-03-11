from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class TradePlan:
    ok: bool
    reason: str
    symbol: str
    side: str
    strategy: str
    timeframe: str
    confidence: float
    entry: float
    stop: float
    tp: float
    rr: float
    risk_pct: float
    stop_distance_pct: float
    leverage_recommended: float
    leverage_cap_applied: bool
    position_value_usdt: float
    qty_est: float
    notional_est: float
    resistance_1: Optional[float]
    support_1: Optional[float]
    atr14_used: float
    atr_source: str
    degraded: bool
    execution_ready: bool
    bar_ts_used: str
    notes: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "symbol": self.symbol,
            "side": self.side,
            "strategy": self.strategy,
            "timeframe": self.timeframe,
            "confidence": self.confidence,
            "entry": self.entry,
            "stop": self.stop,
            "tp": self.tp,
            "rr": self.rr,
            "risk_pct": self.risk_pct,
            "stop_distance_pct": self.stop_distance_pct,
            "leverage_recommended": self.leverage_recommended,
            "leverage_cap_applied": self.leverage_cap_applied,
            "position_value_usdt": self.position_value_usdt,
            "qty_est": self.qty_est,
            "notional_est": self.notional_est,
            "resistance_1": self.resistance_1,
            "support_1": self.support_1,
            "atr14_used": self.atr14_used,
            "atr_source": self.atr_source,
            "degraded": self.degraded,
            "execution_ready": self.execution_ready,
            "bar_ts_used": self.bar_ts_used,
            "notes": list(self.notes),
        }


def _as_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _swing_highs_lows(candles: List[Dict[str, Any]], lookback: int = 50) -> Tuple[List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []
    window = candles[-max(1, int(lookback)) :]
    n = len(window)
    for i in range(1, n - 1):
        high_prev = _as_float(window[i - 1].get("high"))
        high_curr = _as_float(window[i].get("high"))
        high_next = _as_float(window[i + 1].get("high"))
        low_prev = _as_float(window[i - 1].get("low"))
        low_curr = _as_float(window[i].get("low"))
        low_next = _as_float(window[i + 1].get("low"))
        if None in (high_prev, high_curr, high_next, low_prev, low_curr, low_next):
            continue
        if high_curr > high_prev and high_curr > high_next:
            highs.append(high_curr)  # type: ignore[arg-type]
        if low_curr < low_prev and low_curr < low_next:
            lows.append(low_curr)  # type: ignore[arg-type]
    return highs, lows


def _first_resistance_support(
    side: str,
    entry: float,
    candles: List[Dict[str, Any]],
    *,
    buffer_pct: float = 0.15,
) -> Tuple[Optional[float], Optional[float], bool]:
    """Return (resistance_1, support_1, degraded_from_structure)."""
    highs, lows = _swing_highs_lows(candles)
    if not highs and not lows:
        return None, None, True

    side_norm = str(side or "").upper()
    resistance_1: Optional[float] = None
    support_1: Optional[float] = None

    if highs:
        above_entry = [h for h in highs if h > entry]
        resistance_1 = min(above_entry) if above_entry else max(highs)
    if lows:
        below_entry = [l for l in lows if l < entry]
        support_1 = max(below_entry) if below_entry else min(lows)

    degraded = False
    if side_norm == "LONG":
        if resistance_1 is None or resistance_1 <= entry:
            degraded = True
        else:
            res_buf = resistance_1 * (1.0 - buffer_pct / 100.0)
            resistance_1 = max(entry, res_buf)
    elif side_norm == "SHORT":
        if support_1 is None or support_1 >= entry:
            degraded = True
        else:
            sup_buf = support_1 * (1.0 + buffer_pct / 100.0)
            support_1 = min(entry, sup_buf)
    else:
        degraded = True

    return resistance_1, support_1, degraded


def _derive_leverage(
    *,
    stop_distance_pct: float,
    max_leverage: float,
    min_leverage: float = 1.0,
) -> Tuple[float, bool, List[str]]:
    """
    Pick isolated margin leverage so liquidation distance is in the same ballpark as stop distance.
    This is an approximation: 1 / stop_distance_pct (in decimal) with sane caps.
    """
    notes: List[str] = []
    if stop_distance_pct <= 0 or not max_leverage or max_leverage <= 0:
        return 1.0, False, ["invalid_stop_or_max_leverage"]

    stop_frac = stop_distance_pct / 100.0
    if stop_frac <= 0:
        return 1.0, False, ["invalid_stop_or_max_leverage"]

    base_leverage = 1.0 / stop_frac
    applied = base_leverage
    cap_applied = False
    if applied > max_leverage:
        applied = max_leverage
        cap_applied = True
        notes.append("leverage_capped_by_config")
    if applied < min_leverage:
        applied = min_leverage

    if applied >= max(5.0, 0.5 * max_leverage):
        notes.append("high_leverage_warning")
    return float(applied), cap_applied, notes


def build_trade_plan(
    *,
    signal: Dict[str, Any],
    preview: Dict[str, Any],
    candles: List[Dict[str, Any]],
    risk_settings: Any,
    equity_usdt: float,
    max_leverage: Optional[float] = None,
) -> TradePlan:
    """
    Canonical trade plan builder.

    This is a pure function: same inputs => same plan.
    It wraps an existing preview (levels & ATR) and enriches it with sizing, leverage, and structure-based TP.
    """
    symbol = str(signal.get("symbol", "") or "").upper()
    side = str(signal.get("side", signal.get("direction", "")) or "").upper()
    strategy = str(signal.get("strategy", signal.get("setup", "")) or "")
    timeframe = str(signal.get("timeframe", signal.get("tf", "")) or "").strip() or "15"
    confidence = float(signal.get("confidence", 0.0) or 0.0)
    bar_ts_used = str(
        signal.get("bar_ts_used")
        or preview.get("bar_ts_used")
        or signal.get("timestamp_utc")
        or signal.get("ts")
        or ""
    )

    # Basic validation before we trust the preview.
    if not symbol or side not in {"LONG", "SHORT"}:
        return TradePlan(
            ok=False,
            reason="INVALID_SIGNAL",
            symbol=symbol or "",
            side=side or "",
            strategy=strategy,
            timeframe=timeframe,
            confidence=confidence,
            entry=0.0,
            stop=0.0,
            tp=0.0,
            rr=0.0,
            risk_pct=0.0,
            stop_distance_pct=0.0,
            leverage_recommended=1.0,
            leverage_cap_applied=False,
            position_value_usdt=0.0,
            qty_est=0.0,
            notional_est=0.0,
            resistance_1=None,
            support_1=None,
            atr14_used=float(preview.get("atr_used", 0.0) or 0.0),
            atr_source=str(preview.get("atr_source", "") or ""),
            degraded=True,
            execution_ready=False,
            bar_ts_used=bar_ts_used,
            notes=["signal_missing_symbol_or_side"],
        )

    if not preview.get("ok"):
        reason = str(preview.get("reason", "") or "PREVIEW_NOT_OK")
        return TradePlan(
            ok=False,
            reason=reason,
            symbol=symbol,
            side=side,
            strategy=strategy,
            timeframe=timeframe,
            confidence=confidence,
            entry=float(preview.get("entry") or 0.0),
            stop=float(preview.get("sl") or 0.0),
            tp=float(preview.get("tp") or 0.0),
            rr=float(preview.get("rr_ratio") or 0.0),
            risk_pct=0.0,
            stop_distance_pct=0.0,
            leverage_recommended=1.0,
            leverage_cap_applied=False,
            position_value_usdt=0.0,
            qty_est=0.0,
            notional_est=0.0,
            resistance_1=None,
            support_1=None,
            atr14_used=float(preview.get("atr_used", 0.0) or 0.0),
            atr_source=str(preview.get("atr_source", "") or ""),
            degraded=True,
            execution_ready=False,
            bar_ts_used=bar_ts_used,
            notes=["preview_not_ok"],
        )

    entry = _as_float(preview.get("entry")) or 0.0
    stop = _as_float(preview.get("sl")) or 0.0
    tp_preview = _as_float(preview.get("tp")) or 0.0
    atr_used = _as_float(preview.get("atr_used")) or 0.0
    rr = _as_float(preview.get("rr_ratio")) or 0.0
    if entry <= 0 or stop <= 0 or atr_used <= 0:
        return TradePlan(
            ok=False,
            reason="INVALID_LEVELS_OR_ATR",
            symbol=symbol,
            side=side,
            strategy=strategy,
            timeframe=timeframe,
            confidence=confidence,
            entry=entry,
            stop=stop,
            tp=tp_preview,
            rr=rr or 0.0,
            risk_pct=0.0,
            stop_distance_pct=0.0,
            leverage_recommended=1.0,
            leverage_cap_applied=False,
            position_value_usdt=0.0,
            qty_est=0.0,
            notional_est=0.0,
            resistance_1=None,
            support_1=None,
            atr14_used=atr_used,
            atr_source=str(preview.get("atr_source", "") or ""),
            degraded=True,
            execution_ready=False,
            bar_ts_used=bar_ts_used,
            notes=["invalid_entry_or_stop_or_atr"],
        )

    stop_distance = abs(entry - stop)
    stop_distance_pct = (stop_distance / max(entry, 1e-10)) * 100.0

    # Structure-based resistance/support.
    resistance_1, support_1, degraded_structure = _first_resistance_support(
        side=side,
        entry=entry,
        candles=list(candles or []),
    )

    tp: float = tp_preview
    notes: List[str] = []
    degraded = bool(preview.get("degraded_preview", False))
    if degraded_structure:
        degraded = True
        notes.append("structure_degraded_fallback_to_atr_tp")
    else:
        if side == "LONG" and resistance_1 is not None:
            tp = min(tp_preview, resistance_1) if tp_preview > 0 else resistance_1
            if tp < entry:
                tp = entry + stop_distance  # keep RR ~1 if structure is odd
                degraded = True
                notes.append("structure_resistance_below_entry_adjusted")
        elif side == "SHORT" and support_1 is not None:
            tp = max(tp_preview, support_1) if tp_preview > 0 else support_1
            if tp > entry:
                tp = entry - stop_distance
                degraded = True
                notes.append("structure_support_above_entry_adjusted")

    tp_distance = abs(tp - entry)
    if stop_distance <= 0 or tp_distance <= 0:
        return TradePlan(
            ok=False,
            reason="INVALID_GEOMETRY",
            symbol=symbol,
            side=side,
            strategy=strategy,
            timeframe=timeframe,
            confidence=confidence,
            entry=entry,
            stop=stop,
            tp=tp,
            rr=0.0,
            risk_pct=0.0,
            stop_distance_pct=stop_distance_pct,
            leverage_recommended=1.0,
            leverage_cap_applied=False,
            position_value_usdt=0.0,
            qty_est=0.0,
            notional_est=0.0,
            resistance_1=resistance_1,
            support_1=support_1,
            atr14_used=atr_used,
            atr_source=str(preview.get("atr_source", "") or ""),
            degraded=True,
            execution_ready=False,
            bar_ts_used=bar_ts_used,
            notes=notes + ["invalid_geometry"],
        )

    rr_effective = tp_distance / max(stop_distance, 1e-10)

    # Risk sizing: risk_per_trade_pct from risk_settings, equity_usdt from caller.
    risk_pct = float(getattr(risk_settings, "risk_per_trade_pct", 0.0) or 0.0)
    equity_val = max(0.0, float(equity_usdt or 0.0))
    risk_usdt = equity_val * (risk_pct / 100.0) if risk_pct > 0 else 0.0
    qty_est = risk_usdt / stop_distance if risk_usdt > 0 and stop_distance > 0 else 0.0
    notional_est = qty_est * entry

    # Leverage suggestion (isolated, approximate).
    leverage_max_cfg = _as_float(
        getattr(risk_settings, "max_leverage", max_leverage if max_leverage is not None else 1.0)
    ) or 1.0
    leverage_recommended, leverage_cap_applied, lev_notes = _derive_leverage(
        stop_distance_pct=stop_distance_pct,
        max_leverage=leverage_max_cfg,
    )
    notes.extend(lev_notes)

    execution_ready = True
    if degraded:
        notes.append("plan_degraded")

    return TradePlan(
        ok=True,
        reason="" if not degraded else "PLAN_DEGRADED",
        symbol=symbol,
        side=side,
        strategy=strategy,
        timeframe=timeframe,
        confidence=confidence,
        entry=entry,
        stop=stop,
        tp=tp,
        rr=rr_effective,
        risk_pct=risk_pct,
        stop_distance_pct=stop_distance_pct,
        leverage_recommended=leverage_recommended,
        leverage_cap_applied=leverage_cap_applied,
        position_value_usdt=notional_est,
        qty_est=qty_est,
        notional_est=notional_est,
        resistance_1=resistance_1,
        support_1=support_1,
        atr14_used=atr_used,
        atr_source=str(preview.get("atr_source", "") or ""),
        degraded=degraded,
        execution_ready=execution_ready,
        bar_ts_used=bar_ts_used,
        notes=notes,
    )

