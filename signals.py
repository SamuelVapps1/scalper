import os
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional, Tuple

from indicators import atr_wilder, ema as ema_fn, rsi_wilder


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


RB_RANGE_BARS = max(5, _env_int("RB_RANGE_BARS", 24))
RANGE_LEN = RB_RANGE_BARS
RETEST_MAX_BARS = 6
MIN_IMPULSE_ATR = 0.30

FAIL_CONFIRM_BARS = 3
EMA_FAIL_WINDOW = 6

ATR_PERIOD = 14
EMA_TREND_PERIOD = 200

THRESHOLD_PRESETS: Dict[str, Dict[str, float]] = {
    # A = current behavior
    "A": {
        "RB_MIN_RANGE_ATR": 2.0,
        "RB_BREAKOUT_BUFFER_ATR": 0.10,
        "RB_RETEST_TOL_ATR": 0.15,
        "RB_CONFIRM_CLOSE_BUFFER_ATR": 0.05,
        "FB_SWEEP_WICK_ATR": 0.10,
        "FB_CLOSE_BACK_INSIDE_BUFFER_ATR": 0.02,
        "FB_MIN_DIST_FROM_EMA200_PCT": 0.20,
        "FB_MIN_CONFIDENCE": 0.60,
    },
    # B = moderately looser
    "B": {
        "RB_MIN_RANGE_ATR": 1.8,
        "RB_BREAKOUT_BUFFER_ATR": 0.08,
        "RB_RETEST_TOL_ATR": 0.18,
        "RB_CONFIRM_CLOSE_BUFFER_ATR": 0.04,
        "FB_SWEEP_WICK_ATR": 0.07,
        "FB_CLOSE_BACK_INSIDE_BUFFER_ATR": 0.015,
        "FB_MIN_DIST_FROM_EMA200_PCT": 0.15,
        "FB_MIN_CONFIDENCE": 0.55,
    },
    # C = discovery / loosest
    "C": {
        "RB_MIN_RANGE_ATR": 1.5,
        "RB_BREAKOUT_BUFFER_ATR": 0.05,
        "RB_RETEST_TOL_ATR": 0.22,
        "RB_CONFIRM_CLOSE_BUFFER_ATR": 0.03,
        "FB_SWEEP_WICK_ATR": 0.04,
        "FB_CLOSE_BACK_INSIDE_BUFFER_ATR": 0.01,
        "FB_MIN_DIST_FROM_EMA200_PCT": 0.10,
        "FB_MIN_CONFIDENCE": 0.50,
    },
}


def _normalize_profile(profile: str) -> str:
    p = str(profile or "A").strip().upper()
    if p not in THRESHOLD_PRESETS:
        return "A"
    return p


def _profile_thresholds(profile: str) -> Dict[str, float]:
    return dict(THRESHOLD_PRESETS[_normalize_profile(profile)])


def _to_ohlc_lists(candles: List[Dict[str, float]]) -> Tuple[List[float], List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for c in candles:
        highs.append(float(c["high"]))
        lows.append(float(c["low"]))
        closes.append(float(c["close"]))
    return highs, lows, closes


def _candle_ts_utc(candle: Dict[str, object]) -> str:
    ts_utc = str(candle.get("timestamp_utc", "") or "").strip()
    if ts_utc:
        return ts_utc
    try:
        ts_ms = float(candle.get("timestamp", 0.0) or 0.0)
    except (TypeError, ValueError):
        ts_ms = 0.0
    if ts_ms > 0:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def ema(values: List[float], period: int) -> List[float]:
    return ema_fn(values, period)


def atr(high: List[float], low: List[float], close: List[float], period: int = ATR_PERIOD) -> List[Optional[float]]:
    return atr_wilder(high, low, close, period)


def _get_range_lookback() -> int:
    try:
        import config
        return max(10, int(getattr(config, "RANGE_LOOKBACK_BARS", 80)))
    except Exception:
        return 80


def _get_min_range_atr() -> float:
    try:
        import config
        return max(0.0, float(getattr(config, "MIN_RANGE_ATR", 2.0)))
    except Exception:
        return 2.0


def _get_range_exclude_tail() -> int:
    try:
        import config
        return max(0, int(getattr(config, "RANGE_EXCLUDE_TAIL", 2)))
    except Exception:
        return 2


def _compute_range(
    highs: List[float],
    lows: List[float],
    idx: int,
    atr_now: float,
    *,
    lookback: int = 80,
    exclude_tail: int = 2,
    min_range_atr: float = 2.0,
) -> Optional[Dict[str, float]]:
    """Range from bars [idx - lookback - exclude_tail : idx - exclude_tail]. Returns None if invalid."""
    start = max(0, idx - lookback - exclude_tail)
    end = max(0, idx - exclude_tail)
    if end <= start or atr_now <= 0:
        return None
    window_highs = highs[start:end]
    window_lows = lows[start:end]
    if not window_highs or not window_lows:
        return None
    range_high = max(window_highs)
    range_low = min(window_lows)
    range_mid = (range_high + range_low) / 2.0
    range_size = range_high - range_low
    if range_size < min_range_atr * atr_now:
        return None
    return {
        "range_high": range_high,
        "range_low": range_low,
        "range_mid": range_mid,
        "range_size": range_size,
    }


def _clip_confidence(value: float) -> float:
    return max(0.0, min(0.95, round(value, 4)))


def _bar_body_position(close: float, low: float, high: float) -> float:
    bar_range = max(high - low, 1e-10)
    return (close - low) / bar_range


def _fmt_num(value: Optional[float], digits: int = 6) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def _build_intent(
    symbol: str,
    side: str,
    strategy: str,
    reason: str,
    confidence: float,
    ts: str,
    bar_idx: int,
    level_ref: float,
    close_price: float,
    *,
    entry_type: str = "market",
    meta: Optional[Dict[str, object]] = None,
    notes: Optional[str] = None,
) -> Dict[str, object]:
    intent_id = f"{symbol}|{strategy}|{side}|{bar_idx}|{level_ref:.6f}"
    out = {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "reason": reason,
        "confidence": _clip_confidence(confidence),
        "score": _clip_confidence(confidence),
        "ts": ts,
        "bar_ts_used": ts,
        "intent_id": intent_id,
        "setup": strategy,
        "direction": side,
        "timestamp_utc": ts,
        "close": float(close_price),
        "level_ref": level_ref,
        "entry_type": entry_type,
        "notes": notes or reason,
    }
    if meta:
        out["meta"] = dict(meta)
    return out


def range_breakout_retest_go(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    ema200: List[float],
    atr14: List[Optional[float]],
    idx: int,
    ts: str,
    *,
    threshold_profile: str = "A",
    signal_debug: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    lookback = _get_range_lookback()
    min_range_atr = _get_min_range_atr()
    debug = {"strategy": "RANGE_BREAKOUT_RETEST_GO", "reasons": []}

    if idx < lookback + 4:
        if signal_debug:
            debug["reasons"].append("insufficient_history_for_range")
        return [], debug

    atr_now_val = atr14[idx]
    if atr_now_val is None:
        if signal_debug:
            debug["reasons"].append("atr14_not_ready")
        return [], debug
    atr_now = float(atr_now_val)
    if atr_now <= 0:
        if signal_debug:
            debug["reasons"].append("atr14_non_positive")
        return [], debug

    exclude_tail = _get_range_exclude_tail()
    rng = _compute_range(
        highs, lows, idx, atr_now,
        lookback=lookback,
        exclude_tail=exclude_tail,
        min_range_atr=min_range_atr,
    )
    if not rng:
        if signal_debug:
            debug["reasons"].append(f"RANGE_TOO_SMALL(min={min_range_atr:.2f}ATR)")
        return [], debug

    range_high = rng["range_high"]
    range_low = rng["range_low"]
    range_mid = rng["range_mid"]
    candidates: List[Dict[str, object]] = []

    # LONG: breakout candle closes above range_high
    if closes[idx] > range_high:
        invalidation = range_mid
        conf = 0.55
        if closes[idx] > ema200[idx]:
            conf += 0.10
        reason = f"RB_RTG: breakout close above range_high, retest intent (EMA200 ok)" if closes[idx] > ema200[idx] else "RB_RTG: breakout close above range_high, retest intent"
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="LONG",
                strategy="RANGE_BREAKOUT_RETEST_GO",
                reason=reason,
                confidence=conf,
                ts=ts,
                bar_idx=idx,
                level_ref=range_high,
                close_price=closes[idx],
                entry_type="retest",
                meta={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_mid": range_mid,
                    "range_size": rng["range_size"],
                    "break_level": range_high,
                    "retest_level": range_high,
                    "invalidation_level": invalidation,
                },
                notes=reason,
            )
        )

    # SHORT: breakout candle closes below range_low
    if closes[idx] < range_low:
        invalidation = range_mid
        conf = 0.55
        if closes[idx] < ema200[idx]:
            conf += 0.10
        reason = f"RB_RTG: breakout close below range_low, retest intent (EMA200 ok)" if closes[idx] < ema200[idx] else "RB_RTG: breakout close below range_low, retest intent"
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="SHORT",
                strategy="RANGE_BREAKOUT_RETEST_GO",
                reason=reason,
                confidence=conf,
                ts=ts,
                bar_idx=idx,
                level_ref=range_low,
                close_price=closes[idx],
                entry_type="retest",
                meta={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_mid": range_mid,
                    "range_size": rng["range_size"],
                    "break_level": range_low,
                    "retest_level": range_low,
                    "invalidation_level": invalidation,
                },
                notes=reason,
            )
        )

    return candidates, debug


def failed_breakout_or_failed_ema200_fade(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    ema200: List[float],
    atr14: List[Optional[float]],
    idx: int,
    ts: str,
    *,
    threshold_profile: str = "A",
    signal_debug: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    thr = _profile_thresholds(threshold_profile)
    fb_min_dist_from_ema200_pct = float(thr["FB_MIN_DIST_FROM_EMA200_PCT"])
    fb_min_confidence = float(thr["FB_MIN_CONFIDENCE"])
    lookback = _get_range_lookback()
    min_range_atr = _get_min_range_atr()
    debug = {
        "strategy": "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
        "reasons": [],
    }
    if idx < lookback + 4:
        if signal_debug:
            debug["reasons"].append("insufficient_history_for_range")
        return [], debug
    atr_now_val = atr14[idx]
    if atr_now_val is None:
        if signal_debug:
            debug["reasons"].append("atr14_not_ready")
        return [], debug
    atr_now = float(atr_now_val)
    if atr_now <= 0:
        if signal_debug:
            debug["reasons"].append("atr14_non_positive")
        return [], debug

    exclude_tail = _get_range_exclude_tail()
    rng = _compute_range(highs, lows, idx, atr_now, lookback=lookback, exclude_tail=exclude_tail, min_range_atr=min_range_atr)
    range_high = rng["range_high"] if rng else max(highs[max(0, idx - RANGE_LEN) : idx])
    range_low = rng["range_low"] if rng else min(lows[max(0, idx - RANGE_LEN) : idx])
    range_mid = rng["range_mid"] if rng else (range_high + range_low) / 2.0
    atr_ratio = atr_now / max(closes[idx], 1e-10)
    volatility_penalty = 0.10 if (atr_ratio < 0.001 or atr_ratio > 0.05) else 0.0

    candidates: List[Dict[str, object]] = []
    failed_breakout_seen = False
    failed_ema_reclaim_seen = False
    sweep_seen = False
    close_back_inside_seen = False
    conf_filtered_out = False

    # B1: Failed range breakout trap (fade short). high > range_high and close < range_high
    if highs[idx] > range_high and closes[idx] < range_high:
        failed_breakout_seen = True
        sweep_seen = True
        close_back_inside_seen = True
        conf = 0.55
        if (highs[idx] - range_high) / atr_now >= 0.30:
            conf += 0.15
        if (range_high - closes[idx]) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
            conf += 0.10
        if abs(range_high - ema200[idx]) <= (0.25 * atr_now):
            conf += 0.10
        conf -= volatility_penalty
        ema_dist_pct = abs((closes[idx] - ema200[idx]) / max(ema200[idx], 1e-10)) * 100.0
        if ema_dist_pct < fb_min_dist_from_ema200_pct or conf < fb_min_confidence:
            conf_filtered_out = True
        else:
            invalidation = range_high + 0.5 * atr_now
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="SHORT",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="FB_FADE: swept rangeHigh then closed back inside (trap), fade short",
                    confidence=conf,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=range_high,
                    close_price=closes[idx],
                    entry_type="market",
                    meta={
                        "failed_level": range_high,
                        "range_high": range_high,
                        "range_low": range_low,
                        "target_level": range_mid,
                        "invalidation_level": invalidation,
                    },
                    notes="FB_FADE B1: failed breakout trap (sweep above range_high)",
                )
            )

    # B1: Failed range breakdown trap (fade long). low < range_low and close > range_low
    if lows[idx] < range_low and closes[idx] > range_low:
        failed_breakout_seen = True
        sweep_seen = True
        close_back_inside_seen = True
        conf = 0.55
        if (range_low - lows[idx]) / atr_now >= 0.30:
            conf += 0.15
        if (closes[idx] - range_low) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
            conf += 0.10
        if abs(range_low - ema200[idx]) <= (0.25 * atr_now):
            conf += 0.10
        conf -= volatility_penalty
        ema_dist_pct = abs((closes[idx] - ema200[idx]) / max(ema200[idx], 1e-10)) * 100.0
        if ema_dist_pct < fb_min_dist_from_ema200_pct or conf < fb_min_confidence:
            conf_filtered_out = True
        else:
            invalidation = range_low - 0.5 * atr_now
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="LONG",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="FB_FADE: swept rangeLow then closed back inside (trap), fade long",
                    confidence=conf,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=range_low,
                    close_price=closes[idx],
                    entry_type="market",
                    meta={
                        "failed_level": range_low,
                        "range_high": range_high,
                        "range_low": range_low,
                        "target_level": range_mid,
                        "invalidation_level": invalidation,
                    },
                    notes="FB_FADE B1: failed breakout trap (sweep below range_low)",
                )
            )

    # B2: Failed EMA200 reclaim (fade short). high > ema200 and close < ema200
    if highs[idx] > ema200[idx] and closes[idx] < ema200[idx]:
        failed_ema_reclaim_seen = True
        conf = 0.55
        conf -= volatility_penalty
        ema_dist_pct = abs((closes[idx] - ema200[idx]) / max(ema200[idx], 1e-10)) * 100.0
        if ema_dist_pct < fb_min_dist_from_ema200_pct or conf < fb_min_confidence:
            conf_filtered_out = True
        else:
            ema_val = ema200[idx]
            target = range_mid if rng else ema_val - 1.0 * atr_now
            invalidation = ema_val + 0.5 * atr_now
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="SHORT",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="FB_FADE: failed EMA200 reclaim (high>ema200, close<ema200), fade short",
                    confidence=conf,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=ema_val,
                    close_price=closes[idx],
                    entry_type="market",
                    meta={
                        "ema200": ema_val,
                        "failed_level": ema_val,
                        "target_level": target,
                        "invalidation_level": invalidation,
                        "range_high": range_high,
                        "range_low": range_low,
                    },
                    notes="FB_FADE B2: failed EMA200 reclaim",
                )
            )

    # B2: Failed EMA200 reclaim (fade long). low < ema200 and close > ema200
    if lows[idx] < ema200[idx] and closes[idx] > ema200[idx]:
        failed_ema_reclaim_seen = True
        conf = 0.55
        conf -= volatility_penalty
        ema_dist_pct = abs((closes[idx] - ema200[idx]) / max(ema200[idx], 1e-10)) * 100.0
        if ema_dist_pct < fb_min_dist_from_ema200_pct or conf < fb_min_confidence:
            conf_filtered_out = True
        else:
            ema_val = ema200[idx]
            target = range_mid if rng else ema_val + 1.0 * atr_now
            invalidation = ema_val - 0.5 * atr_now
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="LONG",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="FB_FADE: failed EMA200 reclaim (low<ema200, close>ema200), fade long",
                    confidence=conf,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=ema_val,
                    close_price=closes[idx],
                    entry_type="market",
                    meta={
                        "ema200": ema_val,
                        "failed_level": ema_val,
                        "target_level": target,
                        "invalidation_level": invalidation,
                        "range_high": range_high,
                        "range_low": range_low,
                    },
                    notes="FB_FADE B2: failed EMA200 reclaim",
                )
            )

    if highs[idx] > range_high or lows[idx] < range_low:
        sweep_seen = True
    if (highs[idx] > range_high and closes[idx] < range_high) or (lows[idx] < range_low and closes[idx] > range_low):
        close_back_inside_seen = True

    if signal_debug and not candidates:
        if not sweep_seen:
            debug["reasons"].append("NO_SWEEP(high<=range_high and low>=range_low)")
        elif sweep_seen and not close_back_inside_seen:
            debug["reasons"].append("NO_CLOSE_BACK_INSIDE(close outside range)")
        if not failed_ema_reclaim_seen:
            debug["reasons"].append("EMA200_NOT_FAILED_RECLAIM")
        if volatility_penalty > 0 or conf_filtered_out:
            debug["reasons"].append(
                f"CONF_TOO_LOW(min={fb_min_confidence:.2f},ema_dist={fb_min_dist_from_ema200_pct:.2f}%)"
            )
    return candidates, debug


def _get_config_float(name: str, default: float) -> float:
    try:
        import config
        return float(getattr(config, name, default))
    except Exception:
        return default


def _get_config_bool(name: str, default: bool) -> bool:
    try:
        import config
        return bool(getattr(config, name, default))
    except Exception:
        return default


def _get_config_int(name: str, default: int) -> int:
    try:
        import config
        return int(getattr(config, name, default))
    except Exception:
        return default


def _get_config_str(name: str, default: str) -> str:
    try:
        import config
        return str(getattr(config, name, default)).strip().lower()
    except Exception:
        return default


def _setup_a_range_breakout_retest_go_v1(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    opens: List[float],
    atr14: List[Optional[float]],
    idx: int,
    ts: str,
    bias: str,
    atr14_15m: float,
) -> Tuple[List[Dict[str, object]], Optional[Dict[str, float]]]:
    """Setup A: RANGE_BREAKOUT_RETEST_GO. Trigger: LONG if close>range_high+buffer*atr, SHORT if close<range_low-buffer*atr."""
    lookback = _get_range_lookback()
    exclude_tail = _get_range_exclude_tail()
    min_range_atr = _get_min_range_atr()
    if idx < lookback + 4 or atr14_15m <= 0:
        return [], None
    rng = _compute_range(
        highs, lows, idx, atr14_15m,
        lookback=lookback,
        exclude_tail=exclude_tail,
        min_range_atr=min_range_atr,
    )
    if not rng:
        return [], None
    range_high = rng["range_high"]
    range_low = rng["range_low"]
    range_mid = rng["range_mid"]
    candidates: List[Dict[str, object]] = []
    last_close = closes[idx]
    breakout_buffer = _get_config_float("BREAKOUT_BUFFER_ATR", 0.10)
    atr_safe = max(atr14_15m, 1e-10)
    require_body = _get_config_bool("REQUIRE_BREAKOUT_BODY_CONFIRM", True)
    last_open = float(opens[idx]) if idx < len(opens) else 0.0
    last_high = highs[idx]
    last_low = lows[idx]
    body_ok_long = not require_body or (last_close > last_open)
    body_ok_short = not require_body or (last_close < last_open)
    hl_range = max(last_high - last_low, 1e-10)
    body_pct = abs(last_close - last_open) / hl_range
    strong_body_pct = _get_config_float("BREAKOUT_STRONG_BODY_PCT", 0.60)
    strong_market = _get_config_bool("BREAKOUT_STRONG_MARKET", False)
    if bias == "LONG" and last_close > range_high + breakout_buffer * atr_safe and body_ok_long:
        invalidation = range_high - 0.5 * atr14_15m
        strong_breakout = body_pct >= strong_body_pct
        entry_type = "market_sim" if (strong_market and strong_breakout) else "retest"
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="LONG",
                strategy="RANGE_BREAKOUT_RETEST_GO",
                reason="SETUP_A breakout close above range_high, retest intent",
                confidence=0.65,
                ts=ts,
                bar_idx=idx,
                level_ref=range_high,
                close_price=last_close,
                entry_type=entry_type,
                meta={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_mid": range_mid,
                    "range_size": rng["range_size"],
                    "break_level": range_high,
                    "retest_level": range_high,
                    "invalidation_level": invalidation,
                },
                notes="SETUP_A breakout",
            )
        )
    elif bias == "SHORT" and last_close < range_low - breakout_buffer * atr_safe and body_ok_short:
        invalidation = range_low + 0.5 * atr14_15m
        strong_breakout = body_pct >= strong_body_pct
        entry_type = "market_sim" if (strong_market and strong_breakout) else "retest"
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="SHORT",
                strategy="RANGE_BREAKOUT_RETEST_GO",
                reason="SETUP_A breakout close below range_low, retest intent",
                confidence=0.65,
                ts=ts,
                bar_idx=idx,
                level_ref=range_low,
                close_price=last_close,
                entry_type=entry_type,
                meta={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_mid": range_mid,
                    "range_size": rng["range_size"],
                    "break_level": range_low,
                    "retest_level": range_low,
                    "invalidation_level": invalidation,
                },
                notes="SETUP_A breakout",
            )
        )
    return candidates, rng


def _setup_b_trap_fade_only_with_bias(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    idx: int,
    ts: str,
    bias: str,
    rng: Dict[str, float],
    atr14_15m: float,
) -> List[Dict[str, object]]:
    """Setup B: TRAP_FADE_ONLY_WITH_BIAS. LONG: low<range_low and close>range_low. SHORT: high>range_high and close<range_high."""
    range_high = rng["range_high"]
    range_low = rng["range_low"]
    range_mid = rng["range_mid"]
    last_high = highs[idx]
    last_low = lows[idx]
    last_close = closes[idx]
    trap_min_wick = _get_config_float("TRAP_MIN_WICK_ATR", 0.8)
    trap_close_back = _get_config_float("TRAP_CLOSE_BACK_ATR", 0.15)
    atr_safe = max(atr14_15m, 1e-10)
    candidates: List[Dict[str, object]] = []
    if bias == "LONG" and last_low < range_low and last_close > range_low:
        wick = range_low - last_low
        if wick >= trap_min_wick * atr_safe and last_close >= range_low + trap_close_back * atr_safe:
            invalidation = range_low - 0.5 * atr14_15m
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="LONG",
                    strategy="TRAP_FADE_ONLY_WITH_BIAS",
                    reason="SETUP_B trap: swept range_low then closed back inside, fade long",
                    confidence=0.60,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=range_low,
                    close_price=last_close,
                    entry_type="market_sim",
                    meta={
                        "failed_level": range_low,
                        "range_high": range_high,
                        "range_low": range_low,
                        "range_mid": range_mid,
                        "target_level": range_mid,
                        "invalidation_level": invalidation,
                    },
                    notes="SETUP_B trap",
                )
            )
    elif bias == "SHORT" and last_high > range_high and last_close < range_high:
        wick = last_high - range_high
        if wick >= trap_min_wick * atr_safe and last_close <= range_high - trap_close_back * atr_safe:
            invalidation = range_high + 0.5 * atr14_15m
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="SHORT",
                    strategy="TRAP_FADE_ONLY_WITH_BIAS",
                    reason="SETUP_B trap: swept range_high then closed back inside, fade short",
                    confidence=0.60,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=range_high,
                    close_price=last_close,
                    entry_type="market_sim",
                    meta={
                        "failed_level": range_high,
                        "range_high": range_high,
                        "range_low": range_low,
                        "range_mid": range_mid,
                        "target_level": range_mid,
                        "invalidation_level": invalidation,
                    },
                    notes="SETUP_B trap",
                )
            )
    return candidates


def _compute_score_v1(
    dist_pct_4h: float,
    slope10_4h: Optional[float],
    atr_pct_15m: float,
) -> int:
    """Score 0..3: +1 dist<5%, +1 slope non-zero, +1 atr in mid-band (0.4..1.8)."""
    score = 0
    if abs(dist_pct_4h) < 5.0:
        score += 1
    if slope10_4h is not None and abs(float(slope10_4h)) > 1e-8:
        score += 1
    if 0.4 <= atr_pct_15m <= 1.8:
        score += 1
    return score


def _detect_pivot_lows(
    highs: List[float],
    lows: List[float],
    rsi: List[Optional[float]],
    left: int,
    right: int,
    lookback_bars: int,
    n_pivots: int,
) -> List[Dict[str, Any]]:
    """Detect pivot lows in last lookback_bars. Return up to n_pivots most recent (newest first)."""
    n = len(lows)
    if n < left + right + 1 or lookback_bars <= 0:
        return []
    start = max(left, n - lookback_bars)
    pivots: List[Dict[str, Any]] = []
    for i in range(start, n - right):
        window_lows = lows[i - left : i + right + 1]
        if lows[i] <= min(window_lows):
            rsi_val = rsi[i] if i < len(rsi) and rsi[i] is not None else None
            pivots.append({"idx": i, "low": lows[i], "rsi": rsi_val})
    return list(reversed(pivots[-n_pivots:])) if pivots else []


def _detect_pivot_highs(
    highs: List[float],
    lows: List[float],
    rsi: List[Optional[float]],
    left: int,
    right: int,
    lookback_bars: int,
    n_pivots: int,
) -> List[Dict[str, Any]]:
    """Detect pivot highs in last lookback_bars. Return up to n_pivots most recent (newest first)."""
    n = len(highs)
    if n < left + right + 1 or lookback_bars <= 0:
        return []
    start = max(left, n - lookback_bars)
    pivots: List[Dict[str, Any]] = []
    for i in range(start, n - right):
        window_highs = highs[i - left : i + right + 1]
        if highs[i] >= max(window_highs):
            rsi_val = rsi[i] if i < len(rsi) and rsi[i] is not None else None
            pivots.append({"idx": i, "high": highs[i], "rsi": rsi_val})
    return list(reversed(pivots[-n_pivots:])) if pivots else []


def _setup_c_rsi_divergence_with_bias(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    idx: int,
    ts: str,
    bias: str,
    atr14_15m: float,
    ema20_15m: float,
) -> List[Dict[str, object]]:
    """
    RSI divergence add-on: bullish (bias=LONG) or bearish (bias=SHORT).
    No RSI threshold filters. Score +1 for strong divergence.
    """
    candidates: List[Dict[str, object]] = []
    pivot_left = _get_config_int("PIVOT_LEFT", 2)
    pivot_right = _get_config_int("PIVOT_RIGHT", 2)
    n_pivots = _get_config_int("DIVERGENCE_LOOKBACK_PIVOTS", 5)
    lookback_bars = _get_config_int("DIVERGENCE_LOOKBACK_BARS", 120)
    min_price_delta_atr = _get_config_float("DIVERGENCE_MIN_PRICE_DELTA_ATR", 0.3)
    min_rsi_delta = _get_config_float("DIVERGENCE_MIN_RSI_DELTA", 3.0)
    confirm_mode = _get_config_str("DIVERGENCE_CONFIRM_MODE", "ema20")

    rsi_list = rsi_wilder(closes, 14)
    if not rsi_list or rsi_list[idx] is None or atr14_15m <= 0:
        return []
    atr_safe = max(atr14_15m, 1e-10)
    last_close = closes[idx]

    if bias == "LONG":
        pivot_lows = _detect_pivot_lows(highs, lows, rsi_list, pivot_left, pivot_right, lookback_bars, n_pivots)
        if len(pivot_lows) < 2:
            return []
        p1, p2 = pivot_lows[-2], pivot_lows[-1]
        if p1["idx"] >= p2["idx"]:
            return []
        price_ll = lows[p2["idx"]] < lows[p1["idx"]] - min_price_delta_atr * atr_safe
        rsi_hl = (p2["rsi"] is not None and p1["rsi"] is not None and
                  float(p2["rsi"]) > float(p1["rsi"]) + min_rsi_delta)
        if not (price_ll and rsi_hl):
            return []
        confirmed = False
        confirm_reason = ""
        if confirm_mode == "ema20" and ema20_15m > 0:
            confirmed = last_close > ema20_15m
            confirm_reason = "ema20" if confirmed else "close<=ema20"
        else:
            pivot_highs_after = _detect_pivot_highs(
                highs, lows, rsi_list, pivot_left, pivot_right, lookback_bars, n_pivots
            )
            last_ph_after_p2 = next((p for p in reversed(pivot_highs_after) if p["idx"] > p2["idx"]), None)
            if last_ph_after_p2 and last_close > last_ph_after_p2["high"]:
                confirmed = True
                confirm_reason = "break_structure"
            else:
                confirm_reason = "no_break_above_pivot_high"
        if not confirmed:
            return []
        price_delta_atr = (lows[p1["idx"]] - lows[p2["idx"]]) / atr_safe
        rsi_delta = float(p2["rsi"]) - float(p1["rsi"]) if (p2["rsi"] and p1["rsi"]) else 0.0
        strong_div = price_delta_atr > min_price_delta_atr * 1.5 and rsi_delta > min_rsi_delta * 1.5
        score_bonus = 1 if strong_div else 0
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="LONG",
                strategy="RSI_DIVERGENCE_WITH_BIAS",
                reason="Bullish RSI divergence (price LL, RSI HL), confirm=%s" % confirm_reason,
                confidence=0.65 + (0.05 if strong_div else 0),
                ts=ts,
                bar_idx=idx,
                level_ref=ema20_15m if ema20_15m > 0 else last_close,
                close_price=last_close,
                entry_type="market_sim",
                meta={
                    "pivot_p1_idx": p1["idx"],
                    "pivot_p2_idx": p2["idx"],
                    "pivot_p1_low": lows[p1["idx"]],
                    "pivot_p2_low": lows[p2["idx"]],
                    "pivot_p1_rsi": p1["rsi"],
                    "pivot_p2_rsi": p2["rsi"],
                    "confirm_reason": confirm_reason,
                    "score_bonus": score_bonus,
                },
                notes="RSI_DIVERGENCE bullish",
            )
        )
        if candidates:
            candidates[0]["score"] = candidates[0].get("score", 0) + score_bonus
    else:
        pivot_highs = _detect_pivot_highs(highs, lows, rsi_list, pivot_left, pivot_right, lookback_bars, n_pivots)
        if len(pivot_highs) < 2:
            return []
        p1, p2 = pivot_highs[-2], pivot_highs[-1]
        if p1["idx"] >= p2["idx"]:
            return []
        price_hh = highs[p2["idx"]] > highs[p1["idx"]] + min_price_delta_atr * atr_safe
        rsi_lh = (p2["rsi"] is not None and p1["rsi"] is not None and
                  float(p1["rsi"]) > float(p2["rsi"]) + min_rsi_delta)
        if not (price_hh and rsi_lh):
            return []
        confirmed = False
        confirm_reason = ""
        if confirm_mode == "ema20" and ema20_15m > 0:
            confirmed = last_close < ema20_15m
            confirm_reason = "ema20" if confirmed else "close>=ema20"
        else:
            pivot_lows_after = _detect_pivot_lows(
                highs, lows, rsi_list, pivot_left, pivot_right, lookback_bars, n_pivots
            )
            last_pl_after_p2 = next((p for p in reversed(pivot_lows_after) if p["idx"] > p2["idx"]), None)
            if last_pl_after_p2 and last_close < last_pl_after_p2["low"]:
                confirmed = True
                confirm_reason = "break_structure"
            else:
                confirm_reason = "no_break_below_pivot_low"
        if not confirmed:
            return []
        price_delta_atr = (highs[p2["idx"]] - highs[p1["idx"]]) / atr_safe
        rsi_delta = float(p1["rsi"]) - float(p2["rsi"]) if (p1["rsi"] and p2["rsi"]) else 0.0
        strong_div = price_delta_atr > min_price_delta_atr * 1.5 and rsi_delta > min_rsi_delta * 1.5
        score_bonus = 1 if strong_div else 0
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="SHORT",
                strategy="RSI_DIVERGENCE_WITH_BIAS",
                reason="Bearish RSI divergence (price HH, RSI LH), confirm=%s" % confirm_reason,
                confidence=0.65 + (0.05 if strong_div else 0),
                ts=ts,
                bar_idx=idx,
                level_ref=ema20_15m if ema20_15m > 0 else last_close,
                close_price=last_close,
                entry_type="market_sim",
                meta={
                    "pivot_p1_idx": p1["idx"],
                    "pivot_p2_idx": p2["idx"],
                    "pivot_p1_high": highs[p1["idx"]],
                    "pivot_p2_high": highs[p2["idx"]],
                    "pivot_p1_rsi": p1["rsi"],
                    "pivot_p2_rsi": p2["rsi"],
                    "confirm_reason": confirm_reason,
                    "score_bonus": score_bonus,
                },
                notes="RSI_DIVERGENCE bearish",
            )
        )
        if candidates:
            candidates[0]["score"] = candidates[0].get("score", 0) + score_bonus
    return candidates


def _setup_d_ema_pullback_go(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    idx: int,
    ts: str,
    bias: str,
    atr14_15m: float,
    ema20_15m: float,
) -> List[Dict[str, object]]:
    """
    Setup D: EMA_PULLBACK_GO. High-frequency pullback in direction of 4H bias.
    LONG: ema20>ema50, last low<=ema20 (or ema50 if deep), close>ema20. SL below pullback low-0.3*atr, TP 1.5R.
    SHORT: mirrored.
    """
    if not _get_config_bool("EMA_PULLBACK_ENABLED", True):
        return []
    if atr14_15m <= 0 or ema20_15m <= 0:
        return []
    ema50_list = ema(closes, 50)
    ema50_15m = float(ema50_list[idx]) if ema50_list and idx < len(ema50_list) else 0.0
    if ema50_15m <= 0:
        return []
    deep_to_ema50 = _get_config_bool("EMA_PULLBACK_DEEP_TO_EMA50", True)
    atr_safe = max(atr14_15m, 1e-10)
    last_high = highs[idx]
    last_low = lows[idx]
    last_close = closes[idx]
    sl_buffer_atr = 0.3
    tp_r_mult = 1.5
    candidates: List[Dict[str, object]] = []

    if bias == "LONG":
        if ema20_15m <= ema50_15m:
            return []
        pullback_ok = False
        pullback_low = last_low
        if last_low <= ema20_15m and last_close > ema20_15m:
            pullback_ok = True
        elif deep_to_ema50 and last_low <= ema50_15m and last_close > ema20_15m:
            pullback_ok = True
        if not pullback_ok:
            return []
        sl_price = pullback_low - sl_buffer_atr * atr_safe
        entry = last_close
        r = entry - sl_price
        if r <= 0:
            return []
        tp_price = entry + tp_r_mult * r
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="LONG",
                strategy="EMA_PULLBACK_GO",
                reason="EMA pullback: low<=ema20/50, close back above ema20, trend ema20>ema50",
                confidence=0.55,
                ts=ts,
                bar_idx=idx,
                level_ref=ema20_15m,
                close_price=last_close,
                entry_type="market_sim",
                meta={
                    "sl_hint": sl_price,
                    "tp_r_mult": tp_r_mult,
                    "pullback_low": pullback_low,
                    "ema20": ema20_15m,
                    "ema50": ema50_15m,
                    "retest_level": ema20_15m,
                },
                notes="EMA_PULLBACK_GO long",
            )
        )
    elif bias == "SHORT":
        if ema20_15m >= ema50_15m:
            return []
        pullback_ok = False
        pullback_high = last_high
        if last_high >= ema20_15m and last_close < ema20_15m:
            pullback_ok = True
        elif deep_to_ema50 and last_high >= ema50_15m and last_close < ema20_15m:
            pullback_ok = True
        if not pullback_ok:
            return []
        sl_price = pullback_high + sl_buffer_atr * atr_safe
        entry = last_close
        r = sl_price - entry
        if r <= 0:
            return []
        tp_price = entry - tp_r_mult * r
        candidates.append(
            _build_intent(
                symbol=symbol,
                side="SHORT",
                strategy="EMA_PULLBACK_GO",
                reason="EMA pullback: high>=ema20/50, close back below ema20, trend ema20<ema50",
                confidence=0.55,
                ts=ts,
                bar_idx=idx,
                level_ref=ema20_15m,
                close_price=last_close,
                entry_type="market_sim",
                meta={
                    "sl_hint": sl_price,
                    "tp_r_mult": tp_r_mult,
                    "pullback_high": pullback_high,
                    "ema20": ema20_15m,
                    "ema50": ema50_15m,
                    "retest_level": ema20_15m,
                },
                notes="EMA_PULLBACK_GO short",
            )
        )
    return candidates


def evaluate_symbol_intents_v2_trend_pullback(
    symbol: str,
    candles_15m: List[Dict[str, float]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    *,
    bar_ts_used: str = "",
) -> Dict[str, object]:
    """
    Strategy V2: TREND_PULLBACK_EMA20. 15m trigger, pullback to ema20, confirm close above/below.
    Requires 4H bias LONG or SHORT. Returns dict with final_intents, market_snapshot, skip_reason.
    """
    bias = str(bias_info.get("bias", "") or "").upper()
    if bias not in ("LONG", "SHORT"):
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "final_intents": [],
            "skip_reason": "bias_none",
        }
    if len(candles_15m) < 55:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "final_intents": [],
            "skip_reason": "insufficient_candles",
        }
    try:
        highs, lows, closes = _to_ohlc_lists(candles_15m)
        opens = [float(c.get("open", 0) or 0) for c in candles_15m]
    except (KeyError, TypeError, ValueError):
        return {"symbol": symbol, "market_snapshot": {}, "final_intents": [], "skip_reason": "invalid_candles"}
    atr14 = atr(highs, lows, closes, ATR_PERIOD)
    idx = len(candles_15m) - 2
    if atr14[idx] is None:
        return {"symbol": symbol, "market_snapshot": {}, "final_intents": [], "skip_reason": "indicators_not_ready"}
    atr15m = float(atr14[idx])
    close_15m = closes[idx]
    low_15m = lows[idx]
    high_15m = highs[idx]
    open_15m = opens[idx] if idx < len(opens) else close_15m
    ts = _candle_ts_utc(candles_15m[idx])
    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema20_15m = float(ema20_list[idx]) if ema20_list and idx < len(ema20_list) else 0.0
    ema50_15m = float(ema50_list[idx]) if ema50_list and idx < len(ema50_list) else 0.0
    snap_1h = (mtf_snapshot or {}).get(60, {})
    close_1h = float(snap_1h.get("close", 0) or 0)
    ema200_1h = float(snap_1h.get("ema200", 0) or 0)
    pullback_tol = _get_config_float("PULLBACK_TOL_ATR", 0.10)
    sl_atr_mult = _get_config_float("PULLBACK_SL_ATR_MULT", 0.60)
    tp_r = _get_config_float("PULLBACK_TP_R", 1.5)
    req_1h_align = _get_config_bool("PULLBACK_REQUIRE_1H_EMA200_ALIGN", True)
    req_ema20_gt_ema50 = _get_config_bool("PULLBACK_REQUIRE_15M_EMA20_GT_EMA50", True)
    req_body = _get_config_bool("PULLBACK_REQUIRE_BODY_CONFIRM", True)
    if req_1h_align and ema200_1h > 0:
        if bias == "LONG" and close_1h <= ema200_1h:
            return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "ema200_align_fail"}
        if bias == "SHORT" and close_1h >= ema200_1h:
            return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "ema200_align_fail"}
    if req_ema20_gt_ema50 and ema50_15m > 0:
        if bias == "LONG" and ema20_15m <= ema50_15m:
            return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "ema20_ema50_fail"}
        if bias == "SHORT" and ema20_15m >= ema50_15m:
            return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "ema20_ema50_fail"}
    trend_min_sep = _get_config_float("TREND_MIN_SEP_ATR", 0.35)
    if atr15m > 0 and abs(ema20_15m - ema50_15m) < trend_min_sep * atr15m:
        return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "trend_weak"}
    tol = pullback_tol * atr15m
    body_ok = not req_body or (bias == "LONG" and close_15m > open_15m) or (bias == "SHORT" and close_15m < open_15m)
    candidates: List[Dict[str, object]] = []
    if bias == "LONG":
        pullback_ok = low_15m <= ema20_15m + tol
        confirm_ok = close_15m > ema20_15m and body_ok
        if pullback_ok and confirm_ok:
            sl_price = low_15m - sl_atr_mult * atr15m
            meta = {
                "sl_hint": sl_price,
                "tp_r_mult": tp_r,
                "atr14": atr15m,
            }
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="LONG",
                    strategy="TREND_PULLBACK_EMA20",
                    reason="V2 trend pullback: low<=ema20+tol, close>ema20",
                    confidence=0.70,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=ema20_15m,
                    close_price=close_15m,
                    entry_type="market_sim",
                    meta=meta,
                    notes="TREND_PULLBACK_EMA20",
                )
            )
    elif bias == "SHORT":
        pullback_ok = high_15m >= ema20_15m - tol
        confirm_ok = close_15m < ema20_15m and body_ok
        if pullback_ok and confirm_ok:
            sl_price = high_15m + sl_atr_mult * atr15m
            meta = {
                "sl_hint": sl_price,
                "tp_r_mult": tp_r,
                "atr14": atr15m,
            }
            candidates.append(
                _build_intent(
                    symbol=symbol,
                    side="SHORT",
                    strategy="TREND_PULLBACK_EMA20",
                    reason="V2 trend pullback: high>=ema20-tol, close<ema20",
                    confidence=0.70,
                    ts=ts,
                    bar_idx=idx,
                    level_ref=ema20_15m,
                    close_price=close_15m,
                    entry_type="market_sim",
                    meta=meta,
                    notes="TREND_PULLBACK_EMA20",
                )
            )
    if not candidates:
        return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "pullback_not_triggered"}
    momo_confirm = _get_config_bool("MOMO_CONFIRM_5M", True)
    if momo_confirm:
        snap_5m = (mtf_snapshot or {}).get(5, {})
        close_5m = float(snap_5m.get("close", 0) or 0)
        open_5m = float(snap_5m.get("open", 0) or 0)
        ema20_5m = float(snap_5m.get("ema20", 0) or 0)
        atr5m = float(snap_5m.get("atr14", 0) or 0)
        momo_min_body = _get_config_float("MOMO_MIN_BODY_ATR_5M", 0.25)
        body_5m = abs(close_5m - open_5m)
        body_ok_5m = atr5m > 0 and body_5m >= momo_min_body * atr5m
        ema_ok = (
            (bias == "LONG" and close_5m > ema20_5m) or
            (bias == "SHORT" and close_5m < ema20_5m)
        )
        if not (body_ok_5m and ema_ok):
            return {"symbol": symbol, "market_snapshot": {"atr14": atr15m}, "final_intents": [], "skip_reason": "momo_confirm_fail"}
    return {
        "symbol": symbol,
        "market_snapshot": {"atr14": atr15m},
        "final_intents": candidates,
        "skip_reason": None,
    }


def evaluate_symbol_intents_v3_trend_breakout(
    symbol: str,
    candles_15m: List[Dict[str, float]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    *,
    bar_ts_used: str = "",
) -> Dict[str, object]:
    """
    Strategy V3: TREND_CONTINUATION_BREAKOUT. Donchian breakout in trend, 15m trigger, optional 5m confirm.
    Requires 4H bias LONG or SHORT. Returns dict with final_intents, market_snapshot, skip_reason.
    Skip reasons: v3_bias_none, v3_trend_filter_fail, v3_sep_fail, v3_donchian_not_broken,
    v3_body_too_small, v3_5m_confirm_fail, v3_not_enough_bars.
    """
    bias = str(bias_info.get("bias", "") or "").upper()
    if bias not in ("LONG", "SHORT"):
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "final_intents": [],
            "skip_reason": "v3_bias_none",
        }

    donchian_n = _get_config_int("DONCHIAN_N_15M", 20)
    body_atr_min = _get_config_float("BODY_ATR_15M", 0.25)
    trend_sep_atr_1h = _get_config_float("TREND_SEP_ATR_1H", 0.8)
    use_5m_confirm = _get_config_bool("USE_5M_CONFIRM", True)
    min_bars = max(donchian_n + 2, ATR_PERIOD + 2, 55)

    if len(candles_15m) < min_bars:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "final_intents": [],
            "skip_reason": "v3_not_enough_bars",
        }
    try:
        highs, lows, closes = _to_ohlc_lists(candles_15m)
        opens = [float(c.get("open", 0) or 0) for c in candles_15m]
    except (KeyError, TypeError, ValueError):
        return {"symbol": symbol, "market_snapshot": {}, "final_intents": [], "skip_reason": "v3_not_enough_bars"}

    atr14_list = atr(highs, lows, closes, ATR_PERIOD)
    idx = len(candles_15m) - 1
    if atr14_list[idx] is None:
        return {"symbol": symbol, "market_snapshot": {}, "final_intents": [], "skip_reason": "v3_not_enough_bars"}
    atr15m = float(atr14_list[idx])
    close_15m = closes[idx]
    high_15m = highs[idx]
    low_15m = lows[idx]
    open_15m = opens[idx] if idx < len(opens) else close_15m
    body = abs(close_15m - open_15m)
    ts = _candle_ts_utc(candles_15m[idx])

    snap_1h = (mtf_snapshot or {}).get(60, {})
    ema20_1h = float(snap_1h.get("ema20", 0) or 0)
    ema50_1h = float(snap_1h.get("ema50", 0) or 0)
    ema200_1h = float(snap_1h.get("ema200", 0) or 0)
    atr_1h = float(snap_1h.get("atr14", 0) or 0)

    if atr_1h <= 0:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_trend_filter_fail",
        }

    if bias == "LONG":
        trend_ok = ema20_1h > ema50_1h > ema200_1h
        sep = (ema20_1h - ema200_1h) / atr_1h if trend_ok else 0.0
    else:
        trend_ok = ema20_1h < ema50_1h < ema200_1h
        sep = (ema200_1h - ema20_1h) / atr_1h if trend_ok else 0.0

    if not trend_ok:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_trend_filter_fail",
        }
    if sep < trend_sep_atr_1h:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_sep_fail",
        }

    if idx < donchian_n:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_not_enough_bars",
        }
    donch_high = max(highs[idx - donchian_n : idx])
    donch_low = min(lows[idx - donchian_n : idx])

    if bias == "LONG":
        broken = close_15m > donch_high
    else:
        broken = close_15m < donch_low

    if not broken:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_donchian_not_broken",
        }

    body_min = body_atr_min * atr15m
    if body < body_min:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr14": atr15m},
            "final_intents": [],
            "skip_reason": "v3_body_too_small",
        }

    if use_5m_confirm:
        snap_5m = (mtf_snapshot or {}).get(5, {})
        close_5m = float(snap_5m.get("close", 0) or 0)
        if bias == "LONG":
            confirm_ok = close_5m >= donch_high
        else:
            confirm_ok = close_5m <= donch_low
        if not confirm_ok:
            return {
                "symbol": symbol,
                "market_snapshot": {"atr14": atr15m},
                "final_intents": [],
                "skip_reason": "v3_5m_confirm_fail",
            }

    tp_r = _get_config_float("PULLBACK_TP_R", 1.5)
    sl_atr_mult = _get_config_float("PULLBACK_SL_ATR_MULT", 0.60)
    breakout_level = donch_high if bias == "LONG" else donch_low
    sl_price = low_15m - sl_atr_mult * atr15m if bias == "LONG" else high_15m + sl_atr_mult * atr15m

    meta = {
        "sl_hint": sl_price,
        "tp_r_mult": tp_r,
        "atr14": atr15m,
    }
    candidates = [
        _build_intent(
            symbol=symbol,
            side=bias,
            strategy="V3_TREND_BREAKOUT",
            reason=f"V3 Donchian breakout: close {'>' if bias == 'LONG' else '<'} {breakout_level:.4f}",
            confidence=0.70,
            ts=ts,
            bar_idx=idx,
            level_ref=breakout_level,
            close_price=close_15m,
            entry_type="market_sim",
            meta=meta,
            notes="V3_TREND_BREAKOUT",
        )
    ]
    return {
        "symbol": symbol,
        "market_snapshot": {"atr14": atr15m},
        "final_intents": candidates,
        "skip_reason": None,
    }


def evaluate_symbol_intents_v1(
    symbol: str,
    candles_15m: List[Dict[str, float]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    *,
    dedupe_store: Optional[Dict[str, object]] = None,
    signal_debug: bool = False,
    timeframe: str = "15",
) -> Dict[str, object]:
    """
    Strategy V1: Setup A (RANGE_BREAKOUT_RETEST_GO) + Setup B (TRAP_FADE_ONLY_WITH_BIAS).
    Requires bias LONG or SHORT. Never trades against bias.
    """
    bias = str(bias_info.get("bias", "") or "").upper()
    if bias not in ("LONG", "SHORT"):
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {"BIAS": "NONE"},
            "error": "bias_none",
            "skip_reason": "bias_none",
        }
    min_needed = max(RANGE_LEN + 4, ATR_PERIOD + 2)
    if len(candles_15m) < min_needed:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": "insufficient_candles",
            "skip_reason": "insufficient_candles",
        }
    try:
        highs, lows, closes = _to_ohlc_lists(candles_15m)
    except (KeyError, TypeError, ValueError):
        return {"symbol": symbol, "market_snapshot": {}, "candidates_before": [], "final_intents": [], "collisions": [], "rejections": [], "debug_why_none": {}, "error": "invalid_candles", "skip_reason": "invalid_candles"}
    atr14 = atr(highs, lows, closes, ATR_PERIOD)
    idx = len(candles_15m) - 2
    if atr14[idx] is None:
        return {"symbol": symbol, "market_snapshot": {}, "candidates_before": [], "final_intents": [], "collisions": [], "rejections": [], "debug_why_none": {}, "error": "indicators_not_ready", "skip_reason": "indicators_not_ready"}
    atr14_15m = float(atr14[idx])
    close_15m = closes[idx]
    atr_pct_15m = (atr14_15m / max(close_15m, 1e-10)) * 100.0
    min_atr = _get_config_float("MIN_ATR_PCT_15M", 0.2)
    max_atr = _get_config_float("MAX_ATR_PCT_15M", 3.0)
    if atr_pct_15m < min_atr or atr_pct_15m > max_atr:
        return {
            "symbol": symbol,
            "market_snapshot": {"atr_pct_15m": atr_pct_15m},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {"VOLATILITY": f"atr_pct={atr_pct_15m:.2f}%"},
            "error": "volatility_filter",
            "skip_reason": "atr_filter_fail",
        }
    ts = _candle_ts_utc(candles_15m[idx])
    snap_1h = (mtf_snapshot or {}).get(60, {})
    snap_5m = (mtf_snapshot or {}).get(5, {})
    align_fail = False
    if _get_config_bool("REQUIRE_1H_EMA200_ALIGN", False):
        close_1h = float(snap_1h.get("close", 0) or 0)
        ema200_1h = float(snap_1h.get("ema200", 0) or 0)
        if ema200_1h > 0:
            if bias == "LONG" and close_1h < ema200_1h:
                align_fail = True
            elif bias == "SHORT" and close_1h > ema200_1h:
                align_fail = True
    dist_pct_4h = float(bias_info.get("dist_pct", 0) or 0)
    max_abs_dist = _get_config_float("MAX_ABS_DIST_PCT_4H", 10.0)
    dist_skip_pct = _get_config_float("DIST_SKIP_PCT_4H", 40.0)
    abs_dist = abs(dist_pct_4h)
    if abs_dist > dist_skip_pct:
        return {
            "symbol": symbol,
            "market_snapshot": {"dist_pct_4h": dist_pct_4h},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {"DIST_SKIP": f"abs(dist_pct_4h)={dist_pct_4h:.2f}% > {dist_skip_pct}"},
            "error": "max_abs_dist_4h_hard",
            "skip_reason": "max_abs_dist_4h_hard",
        }
    dist_far = abs_dist > max_abs_dist
    opens = [float(c.get("open", 0) or 0) for c in candles_15m]
    run_setup_a = _get_config_bool("STRATEGY_V1", True) and _get_config_bool("V1_SETUP_BREAKOUT", True)
    run_setup_b = _get_config_bool("STRATEGY_V1", True) and _get_config_bool("V1_SETUP_TRAP", False)
    setup_a_cands, rng = _setup_a_range_breakout_retest_go_v1(
        symbol, highs, lows, closes, opens, atr14, idx, ts, bias, atr14_15m
    )
    if not run_setup_a:
        setup_a_cands = []
    setup_a_pre_retest = len(setup_a_cands)
    retest_tol = _get_config_float("RETEST_TOL_ATR", 0.10)
    low_5m = float(snap_5m.get("low", 0) or 0)
    high_5m = float(snap_5m.get("high", 0) or 0)
    atr5m = float(snap_5m.get("atr14", 0) or 0)
    if retest_tol > 0 and atr5m > 0:
        filtered_a: List[Dict[str, object]] = []
        for c in setup_a_cands:
            retest_level = c.get("meta", {}).get("retest_level")
            if retest_level is None:
                filtered_a.append(c)
                continue
            rl = float(retest_level)
            side = str(c.get("side", "")).upper()
            if side == "LONG":
                if low_5m <= rl + retest_tol * atr5m:
                    filtered_a.append(c)
            elif side == "SHORT":
                if high_5m >= rl - retest_tol * atr5m:
                    filtered_a.append(c)
            else:
                filtered_a.append(c)
        setup_a_cands = filtered_a
    setup_b_cands: List[Dict[str, object]] = []
    if rng and run_setup_b:
        setup_b_cands = _setup_b_trap_fade_only_with_bias(
            symbol, highs, lows, closes, idx, ts, bias, rng, atr14_15m
        )
    ema20_15m_list = ema(closes, 20)
    ema20_15m = float(ema20_15m_list[idx]) if ema20_15m_list and idx < len(ema20_15m_list) else 0.0
    setup_c_cands = _setup_c_rsi_divergence_with_bias(
        symbol, highs, lows, closes, idx, ts, bias, atr14_15m, ema20_15m
    )
    setup_d_cands = _setup_d_ema_pullback_go(
        symbol, highs, lows, closes, idx, ts, bias, atr14_15m, ema20_15m
    )
    candidates = setup_a_cands + setup_b_cands + setup_c_cands + setup_d_cands
    candidates_before_ema20 = len(candidates)
    require_ema20_setup_a = _get_config_bool("REQUIRE_5M_EMA20_CONFIRM", False)
    require_ema20_trap = _get_config_bool("REQUIRE_5M_EMA20_CONFIRM_FOR_TRAP", False)
    if (require_ema20_setup_a or require_ema20_trap) and candidates:
        close_5m = float(snap_5m.get("close", 0) or 0)
        ema20_5m = float(snap_5m.get("ema20", 0) or 0)
        if ema20_5m > 0:
            filtered: List[Dict[str, object]] = []
            for c in candidates:
                strat = str(c.get("strategy", "") or "")
                apply_to_this = (
                    (require_ema20_setup_a and strat == "RANGE_BREAKOUT_RETEST_GO")
                    or (require_ema20_trap and strat == "TRAP_FADE_ONLY_WITH_BIAS")
                )
                if not apply_to_this:
                    filtered.append(c)
                    continue
                side = str(c.get("side", "")).upper()
                if side == "LONG" and close_5m >= ema20_5m:
                    filtered.append(c)
                elif side == "SHORT" and close_5m <= ema20_5m:
                    filtered.append(c)
            candidates = filtered
    candidates_after_ema20 = len(candidates)
    slope10_4h = bias_info.get("slope10")
    score = _compute_score_v1(dist_pct_4h, slope10_4h, atr_pct_15m)
    if align_fail:
        align_penalty = abs(_get_config_float("ALIGN_SCORE_PENALTY", -1.0))
        score = score - align_penalty
    if dist_far:
        dist_penalty = abs(_get_config_float("DIST_SCORE_PENALTY", -1.0))
        score = score - dist_penalty
    min_score = _get_config_float("MIN_SCORE", 0.0)
    for c in candidates:
        c["score"] = score
        c["meta"] = dict(c.get("meta", {}))
        c["meta"]["atr_pct_15m"] = atr_pct_15m
        c["meta"]["score_v1"] = score
        if align_fail:
            c["meta"]["align_note"] = "ALIGN_FAIL"
        if dist_far:
            c["meta"]["dist_note"] = "DIST_FAR"
    candidates = [c for c in candidates if score >= min_score]
    candidates_after_score = len(candidates)
    if signal_debug:
        if setup_a_cands:
            logging.getLogger(__name__).debug("SETUP_A breakout symbol=%s bias=%s", symbol, bias)
        if setup_b_cands:
            logging.getLogger(__name__).debug("SETUP_B trap symbol=%s bias=%s", symbol, bias)
    final_intents: List[Dict[str, object]] = []
    if candidates:
        best = max(candidates, key=lambda x: (float(x.get("score", 0)), float(x.get("confidence", 0))))
        best["intent_id"] = f"{symbol}|{best.get('strategy','')}|{best.get('side','')}|{timeframe}|{ts}"
        best["bar_ts_used"] = ts
        final_intents = _dedupe_intents([best], dedupe_store=dedupe_store)
    snapshot = _build_market_snapshot(symbol, highs, lows, closes, ema(closes, EMA_TREND_PERIOD), atr14, idx, ts)
    near_miss_candidates: List[Dict[str, object]] = []
    if not final_intents and bias in ("LONG", "SHORT") and atr14_15m > 0:
        range_high = float(snapshot.get("range_high", 0) or 0)
        range_low = float(snapshot.get("range_low", 0) or 0)
        close_15m = float(snapshot.get("last_close", 0) or 0)
        high_15m = float(snapshot.get("last_high", 0) or 0)
        low_15m = float(snapshot.get("last_low", 0) or 0)
        if range_high > 0 and range_low > 0:
            if bias == "LONG":
                dist_breakout = (range_high - close_15m) / atr14_15m
                if dist_breakout > 0:
                    near_miss_candidates.append({
                        "symbol": symbol,
                        "setup_hint": "breakout",
                        "dist_atr": round(dist_breakout, 4),
                        "bias": "LONG",
                    })
                wick_need = (range_low - low_15m) / atr14_15m
                if wick_need < 0:
                    dist_trap = (low_15m - range_low) / atr14_15m
                    if dist_trap > 0:
                        near_miss_candidates.append({
                            "symbol": symbol,
                            "setup_hint": "trap",
                            "dist_atr": round(dist_trap, 4),
                            "bias": "LONG",
                        })
            else:
                dist_breakout = (close_15m - range_low) / atr14_15m
                if dist_breakout > 0:
                    near_miss_candidates.append({
                        "symbol": symbol,
                        "setup_hint": "breakout",
                        "dist_atr": round(dist_breakout, 4),
                        "bias": "SHORT",
                    })
                wick_need = (high_15m - range_high) / atr14_15m
                if wick_need < 0:
                    dist_trap = (range_high - high_15m) / atr14_15m
                    if dist_trap > 0:
                        near_miss_candidates.append({
                            "symbol": symbol,
                            "setup_hint": "trap",
                            "dist_atr": round(dist_trap, 4),
                            "bias": "SHORT",
                        })
    skip_reason: Optional[str] = None
    if not final_intents:
        if rng is None:
            skip_reason = "range_too_small"
        elif candidates_before_ema20 == 0:
            if setup_a_pre_retest > 0 and len(setup_a_cands) == 0:
                skip_reason = "retest_not_filled"
            else:
                skip_reason = "breakout_not_triggered"
        elif (require_ema20_setup_a or require_ema20_trap) and candidates_before_ema20 > 0 and candidates_after_ema20 == 0:
            skip_reason = "ema20_confirm_fail"
        elif candidates_after_ema20 > 0 and candidates_after_score == 0:
            skip_reason = "score_below_min"
        else:
            skip_reason = "breakout_not_triggered"
    return {
        "symbol": symbol,
        "threshold_profile": "A",
        "market_snapshot": snapshot,
        "candidates_before": candidates,
        "final_intents": final_intents,
        "near_miss_candidates": near_miss_candidates,
        "early_intents": [],
        "collisions": [],
        "rejections": [],
        "debug_why_none": {} if final_intents else {"SCORE": f"score={score} min={min_score}"},
        "error": None,
        "skip_reason": skip_reason,
    }


def _dedupe_intents(
    intents: List[Dict[str, object]],
    dedupe_store: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    if dedupe_store is None:
        return intents
    out: List[Dict[str, object]] = []
    for intent in intents:
        intent_id = str(intent.get("intent_id", ""))
        if not intent_id or dedupe_store.get(intent_id):
            continue
        dedupe_store[intent_id] = True
        out.append(intent)
    return out


def _build_market_snapshot(
    symbol: str,
    highs: List[float],
    lows: List[float],
    closes: List[float],
    ema200: List[float],
    atr14: List[Optional[float]],
    idx: int,
    ts: str,
) -> Dict[str, object]:
    last_close = closes[idx]
    last_high = highs[idx]
    last_low = lows[idx]
    atr_value = float(atr14[idx]) if atr14[idx] is not None else 0.0
    ema_value = float(ema200[idx])
    ema_distance_pct = ((last_close - ema_value) / max(ema_value, 1e-10)) * 100.0
    lookback = _get_range_lookback()
    exclude_tail = _get_range_exclude_tail()
    rng = _compute_range(highs, lows, idx, atr_value if atr_value > 0 else 1e-6, lookback=lookback, exclude_tail=exclude_tail, min_range_atr=_get_min_range_atr()) if atr_value > 0 else None
    if rng:
        range_high = rng["range_high"]
        range_low = rng["range_low"]
        range_mid = rng["range_mid"]
        range_size = rng["range_size"]
    else:
        start = max(0, idx - lookback - exclude_tail)
        end = max(0, idx - exclude_tail)
        if end > start:
            range_high = max(highs[start:end])
            range_low = min(lows[start:end])
        else:
            range_high = max(highs[: idx + 1]) if highs else last_close
            range_low = min(lows[: idx + 1]) if lows else last_close
        range_mid = (range_high + range_low) / 2.0
        range_size = range_high - range_low
    if last_close > range_high:
        range_pos = "above"
    elif last_close < range_low:
        range_pos = "below"
    else:
        range_pos = "inside"
    rsi_list = rsi_wilder(closes, 14)
    rsi14_val = float(rsi_list[idx]) if rsi_list and idx < len(rsi_list) and rsi_list[idx] is not None else None
    return {
        "symbol": symbol,
        "ts": ts,
        "bar_ts_used": ts,
        "last_close": float(last_close),
        "last_high": float(last_high),
        "last_low": float(last_low),
        "atr14": atr_value,
        "atr14_pct": (atr_value / max(last_close, 1e-10)) * 100.0,
        "ema200": ema_value,
        "ema_distance_pct": ema_distance_pct,
        "rsi14": rsi14_val,
        "range_high": float(range_high),
        "range_low": float(range_low),
        "range_mid": float(range_mid),
        "range_size": float(range_size),
        "range_position": range_pos,
    }


def _first_fail_rb(debug: Dict[str, object]) -> str:
    reasons = list(debug.get("reasons", []) or [])
    for reason in reasons:
        if str(reason).startswith("insufficient_history_for_range"):
            return "NO_RANGE"
        if str(reason).startswith("RANGE_TOO_SMALL"):
            return "RANGE_TOO_SMALL"
        if str(reason) in {"atr14_not_ready", "atr14_non_positive"}:
            return "ATR_OUT_OF_BOUNDS"
        if str(reason).startswith("NO_BREAKOUT"):
            return "NO_BREAKOUT"
        if str(reason).startswith("NO_RETEST"):
            return "NO_RETEST"
        if str(reason).startswith("NO_CONFIRM"):
            return "NO_CONFIRM"
        if str(reason).startswith("EMA200_MISALIGN"):
            return "EMA200_MISALIGN"
    return "NO_BREAKOUT"


def _first_fail_fb(debug: Dict[str, object]) -> str:
    reasons = list(debug.get("reasons", []) or [])
    for reason in reasons:
        if str(reason).startswith("insufficient_history_for_range"):
            return "NO_SWEEP"
        if str(reason).startswith("CONF_TOO_LOW"):
            return "CONF_TOO_LOW"
        if str(reason).startswith("NO_SWEEP"):
            return "NO_SWEEP"
        if str(reason).startswith("NO_CLOSE_BACK_INSIDE"):
            return "NO_CLOSE_BACK_INSIDE"
        if str(reason).startswith("EMA200_NOT_FAILED_RECLAIM"):
            return "EMA200_NOT_FAILED_RECLAIM"
        if str(reason).startswith("atr14_not_ready") or str(reason).startswith("atr14_non_positive"):
            return "CONF_TOO_LOW"
    return "NO_SWEEP"


def evaluate_symbol_intents(
    symbol: str,
    candles: List[Dict[str, float]],
    dedupe_store: Optional[Dict[str, object]] = None,
    signal_debug: bool = False,
    early_min_conf: float = 0.35,
    threshold_profile: str = "A",
) -> Dict[str, object]:
    profile = _normalize_profile(threshold_profile)
    min_needed = max(EMA_TREND_PERIOD, RANGE_LEN + RETEST_MAX_BARS + 1, ATR_PERIOD + 2)
    if len(candles) < min_needed:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": "insufficient_candles",
        }

    try:
        highs, lows, closes = _to_ohlc_lists(candles)
    except (KeyError, TypeError, ValueError):
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": "invalid_candles",
        }

    ema200 = ema(closes, EMA_TREND_PERIOD)
    atr14 = atr(highs, lows, closes, ATR_PERIOD)
    if len(candles) < 2:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": "insufficient_candles_for_closed_bar",
        }
    decision_idx = len(candles) - 2
    live_idx = len(candles) - 1  # optional preview bar, never used for strategy decisions
    _ = live_idx
    idx = decision_idx
    if atr14[idx] is None:
        return {
            "symbol": symbol,
            "market_snapshot": {},
            "candidates_before": [],
            "final_intents": [],
            "collisions": [],
            "rejections": [],
            "debug_why_none": {},
            "error": "indicators_not_ready",
        }

    ts = _candle_ts_utc(candles[idx])
    candidates: List[Dict[str, object]] = []
    rb_candidates, rb_debug = range_breakout_retest_go(
        symbol=symbol,
        highs=highs,
        lows=lows,
        closes=closes,
        ema200=ema200,
        atr14=atr14,
        idx=idx,
        ts=ts,
        threshold_profile=profile,
        signal_debug=signal_debug,
    )
    fb_candidates, fb_debug = failed_breakout_or_failed_ema200_fade(
        symbol=symbol,
        highs=highs,
        lows=lows,
        closes=closes,
        ema200=ema200,
        atr14=atr14,
        idx=idx,
        ts=ts,
        threshold_profile=profile,
        signal_debug=signal_debug,
    )
    candidates.extend(rb_candidates)
    candidates.extend(fb_candidates)

    final_intents: List[Dict[str, object]] = []
    collisions: List[Dict[str, object]] = []
    if candidates:
        best_intent = max(candidates, key=lambda x: float(x.get("confidence", 0.0)))
        losers = [c for c in candidates if c is not best_intent]
        if losers:
            collisions.append(
                {
                    "symbol": symbol,
                    "chosen_strategy": str(best_intent.get("strategy", "")),
                    "chosen_confidence": float(best_intent.get("confidence", 0.0)),
                    "dropped": [
                        {
                            "strategy": str(item.get("strategy", "")),
                            "confidence": float(item.get("confidence", 0.0)),
                        }
                        for item in losers
                    ],
                }
            )
        final_intents = _dedupe_intents([best_intent], dedupe_store=dedupe_store)

    rejections: List[Dict[str, object]] = []
    debug_why_none: Dict[str, str] = {}
    if signal_debug and not final_intents:
        if not rb_candidates and rb_debug.get("reasons"):
            rejections.append(
                {
                    "strategy": rb_debug.get("strategy", "RANGE_BREAKOUT_RETEST_GO"),
                    "reasons": list(rb_debug.get("reasons", []))[:4],
                }
            )
            debug_why_none["RB_RTG"] = _first_fail_rb(rb_debug)
        if not fb_candidates and fb_debug.get("reasons"):
            rejections.append(
                {
                    "strategy": fb_debug.get("strategy", "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE"),
                    "reasons": list(fb_debug.get("reasons", []))[:4],
                }
            )
            debug_why_none["FB_FADE"] = _first_fail_fb(fb_debug)

    snapshot = _build_market_snapshot(
        symbol=symbol,
        highs=highs,
        lows=lows,
        closes=closes,
        ema200=ema200,
        atr14=atr14,
        idx=idx,
        ts=ts,
    )
    # EARLY alerts are evaluated from dedicated 5m bridge logic elsewhere.
    # Keep this empty in 15m evaluation to avoid accidental paper/risk coupling.
    early_intents: List[Dict[str, object]] = []
    return {
        "symbol": symbol,
        "threshold_profile": profile,
        "market_snapshot": snapshot,
        "candidates_before": candidates,
        "final_intents": final_intents,
        "early_intents": early_intents,
        "collisions": collisions,
        "rejections": rejections,
        "debug_why_none": debug_why_none,
        "error": None,
    }


def generate_signals(
    symbol: str,
    candles: List[Dict[str, float]],
    dedupe_store: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    evaluated = evaluate_symbol_intents(
        symbol=symbol,
        candles=candles,
        dedupe_store=dedupe_store,
        signal_debug=False,
    )
    return list(evaluated.get("final_intents", []))


def evaluate_early_intents_from_5m(
    *,
    symbol: str,
    candles_5m: List[Dict[str, float]],
    context_15m: Dict[str, object],
    early_min_conf: float,
    require_15m_context: bool,
) -> List[Dict[str, object]]:
    profile = _normalize_profile(str(context_15m.get("threshold_profile", "A")))
    thr = _profile_thresholds(profile)
    rb_breakout_buffer_atr = float(thr["RB_BREAKOUT_BUFFER_ATR"])
    rb_retest_tol_atr = float(thr["RB_RETEST_TOL_ATR"])
    rb_confirm_close_buffer_atr = float(thr["RB_CONFIRM_CLOSE_BUFFER_ATR"])
    if len(candles_5m) < max(RANGE_LEN + RETEST_MAX_BARS + 2, ATR_PERIOD + 2):
        return []
    try:
        highs, lows, closes = _to_ohlc_lists(candles_5m)
    except (KeyError, TypeError, ValueError):
        return []

    atr14 = atr(highs, lows, closes, ATR_PERIOD)
    idx = len(candles_5m) - 1  # EARLY uses currently forming 5m bar.
    if idx < 1 or atr14[idx] is None or float(atr14[idx]) <= 0:
        return []
    atr_now = float(atr14[idx])
    ts_5m = _candle_ts_utc(candles_5m[idx])

    snap_15m = dict(context_15m.get("market_snapshot", {}) or {})
    range_high_15m = float(snap_15m.get("range_high", 0.0) or 0.0)
    range_low_15m = float(snap_15m.get("range_low", 0.0) or 0.0)
    bar_ts_15m = str(snap_15m.get("bar_ts_used", snap_15m.get("ts", "")) or "")

    ctx_candidates = list(context_15m.get("candidates_before", []) or [])
    has_rb_ctx = any(str(c.get("strategy", "")) == "RANGE_BREAKOUT_RETEST_GO" for c in ctx_candidates)
    has_fb_ctx = any(str(c.get("strategy", "")) == "FAILED_BREAKOUT_OR_FAILED_EMA200_FADE" for c in ctx_candidates)
    if require_15m_context and not (has_rb_ctx or has_fb_ctx):
        return []

    out: List[Dict[str, object]] = []

    # FB_FADE EARLY on 5m with 15m range context.
    if (not require_15m_context) or has_fb_ctx:
        if range_high_15m > 0 and highs[idx] > range_high_15m and closes[idx] < range_high_15m:
            conf = 0.35
            if (highs[idx] - range_high_15m) / max(atr_now, 1e-10) >= 0.30:
                conf += 0.10
            if (range_high_15m - closes[idx]) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
                conf += 0.10
            if conf >= early_min_conf:
                intent = _build_intent(
                    symbol=symbol,
                    side="SHORT",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="EARLY[5m] FB_FADE: sweep above 15m rangeHigh then close back inside",
                    confidence=conf,
                    ts=ts_5m,
                    bar_idx=idx,
                    level_ref=range_high_15m,
                    close_price=closes[idx],
                )
                intent["bar_ts_5m"] = ts_5m
                intent["bar_ts_15m"] = bar_ts_15m
                out.append(intent)
        if range_low_15m > 0 and lows[idx] < range_low_15m and closes[idx] > range_low_15m:
            conf = 0.35
            if (range_low_15m - lows[idx]) / max(atr_now, 1e-10) >= 0.30:
                conf += 0.10
            if (closes[idx] - range_low_15m) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
                conf += 0.10
            if conf >= early_min_conf:
                intent = _build_intent(
                    symbol=symbol,
                    side="LONG",
                    strategy="FAILED_BREAKOUT_OR_FAILED_EMA200_FADE",
                    reason="EARLY[5m] FB_FADE: sweep below 15m rangeLow then close back inside",
                    confidence=conf,
                    ts=ts_5m,
                    bar_idx=idx,
                    level_ref=range_low_15m,
                    close_price=closes[idx],
                )
                intent["bar_ts_5m"] = ts_5m
                intent["bar_ts_15m"] = bar_ts_15m
                out.append(intent)

    # RB_RTG EARLY on 5m around 15m breakout levels.
    if (not require_15m_context) or has_rb_ctx:
        look_from = max(1, idx - RETEST_MAX_BARS)
        level_high = range_high_15m
        level_low = range_low_15m

        bull_breakout = False
        for b_idx in range(look_from, idx):
            atr_b = atr14[b_idx]
            if atr_b is None or float(atr_b) <= 0:
                continue
            atr_b = float(atr_b)
            if level_high > 0 and closes[b_idx] > level_high + (rb_breakout_buffer_atr * atr_b):
                bull_breakout = True
                break
        if bull_breakout and level_high > 0:
            retest_ok = abs(lows[idx] - level_high) <= (rb_retest_tol_atr * atr_now)
            reclaim_ok = closes[idx] > (level_high + (rb_confirm_close_buffer_atr * atr_now))
            if retest_ok and reclaim_ok:
                conf = 0.35
                if abs(lows[idx] - level_high) <= (0.10 * atr_now):
                    conf += 0.10
                if _bar_body_position(closes[idx], lows[idx], highs[idx]) >= 0.60:
                    conf += 0.10
                if conf >= early_min_conf:
                    intent = _build_intent(
                        symbol=symbol,
                        side="LONG",
                        strategy="RANGE_BREAKOUT_RETEST_GO",
                        reason="EARLY[5m] RB_RTG: breakout/retest/reclaim around 15m rangeHigh",
                        confidence=conf,
                        ts=ts_5m,
                        bar_idx=idx,
                        level_ref=level_high,
                        close_price=closes[idx],
                    )
                    intent["bar_ts_5m"] = ts_5m
                    intent["bar_ts_15m"] = bar_ts_15m
                    out.append(intent)

        bear_breakout = False
        for b_idx in range(look_from, idx):
            atr_b = atr14[b_idx]
            if atr_b is None or float(atr_b) <= 0:
                continue
            atr_b = float(atr_b)
            if level_low > 0 and closes[b_idx] < level_low - (rb_breakout_buffer_atr * atr_b):
                bear_breakout = True
                break
        if bear_breakout and level_low > 0:
            retest_ok = abs(highs[idx] - level_low) <= (rb_retest_tol_atr * atr_now)
            reclaim_ok = closes[idx] < (level_low - (rb_confirm_close_buffer_atr * atr_now))
            if retest_ok and reclaim_ok:
                conf = 0.35
                if abs(highs[idx] - level_low) <= (0.10 * atr_now):
                    conf += 0.10
                if _bar_body_position(closes[idx], lows[idx], highs[idx]) <= 0.40:
                    conf += 0.10
                if conf >= early_min_conf:
                    intent = _build_intent(
                        symbol=symbol,
                        side="SHORT",
                        strategy="RANGE_BREAKOUT_RETEST_GO",
                        reason="EARLY[5m] RB_RTG: breakout/retest/reclaim around 15m rangeLow",
                        confidence=conf,
                        ts=ts_5m,
                        bar_idx=idx,
                        level_ref=level_low,
                        close_price=closes[idx],
                    )
                    intent["bar_ts_5m"] = ts_5m
                    intent["bar_ts_15m"] = bar_ts_15m
                    out.append(intent)

    if not out:
        return []
    # Keep most confident early signal to avoid noise.
    best = max(out, key=lambda x: float(x.get("confidence", 0.0)))
    return [best]


def build_reconcile_report(
    symbol: str,
    candles: List[Dict[str, float]],
    candles_5m: Optional[List[Dict[str, float]]] = None,
    threshold_profile: str = "A",
) -> str:
    profile = _normalize_profile(threshold_profile)
    thr = _profile_thresholds(profile)
    rb_min_range_atr = float(thr["RB_MIN_RANGE_ATR"])
    rb_breakout_buffer_atr = float(thr["RB_BREAKOUT_BUFFER_ATR"])
    rb_retest_tol_atr = float(thr["RB_RETEST_TOL_ATR"])
    rb_confirm_close_buffer_atr = float(thr["RB_CONFIRM_CLOSE_BUFFER_ATR"])
    fb_min_dist_from_ema200_pct = float(thr["FB_MIN_DIST_FROM_EMA200_PCT"])
    fb_min_confidence = float(thr["FB_MIN_CONFIDENCE"])
    lines: List[str] = []
    lines.append(f"RECONCILE {symbol} (DRY RUN) profile={profile}")
    lines.append(f"candles={len(candles)}")
    if len(candles) < 2:
        lines.append("error=insufficient_candles_for_closed_bar")
        return "\n".join(lines)

    try:
        highs, lows, closes = _to_ohlc_lists(candles)
    except (KeyError, TypeError, ValueError):
        lines.append("error=invalid_candles")
        return "\n".join(lines)

    ema200 = ema(closes, EMA_TREND_PERIOD)
    atr14 = atr(highs, lows, closes, ATR_PERIOD)
    idx = len(candles) - 2
    ts_used = _candle_ts_utc(candles[idx])
    lines.append(f"bar_ts_used={ts_used} (index=-2)")

    lines.append("")
    lines.append("Last 5 bars (15m):")
    start = max(0, len(candles) - 5)
    for i in range(start, len(candles)):
        c = candles[i]
        bar_ts = _candle_ts_utc(c)
        marker = " <- CONFIRMED[15m](-2)" if i == idx else ""
        lines.append(
            f"- {bar_ts}{marker} "
            f"O={_fmt_num(float(c.get('open', 0.0)))} "
            f"H={_fmt_num(float(c.get('high', 0.0)))} "
            f"L={_fmt_num(float(c.get('low', 0.0)))} "
            f"C={_fmt_num(float(c.get('close', 0.0)))} "
            f"ATR14={_fmt_num(atr14[i])} "
            f"EMA200={_fmt_num(ema200[i])}"
        )

    if atr14[idx] is None or float(atr14[idx]) <= 0:
        lines.append("")
        lines.append("error=indicators_not_ready_on_used_bar")
        return "\n".join(lines)

    atr_now = float(atr14[idx])
    range_high = max(highs[idx - RANGE_LEN : idx]) if idx >= RANGE_LEN else max(highs[: idx + 1])
    range_low = min(lows[idx - RANGE_LEN : idx]) if idx >= RANGE_LEN else min(lows[: idx + 1])
    lines.append("")
    lines.append(
        f"Levels: ATR14={_fmt_num(atr_now)} EMA200={_fmt_num(ema200[idx])} "
        f"rangeHigh={_fmt_num(range_high)} rangeLow={_fmt_num(range_low)}"
    )

    # RB_RTG conditions.
    rb_has_history = idx > RANGE_LEN
    rb_range_height = range_high - range_low
    rb_range_too_small = rb_range_height < (rb_min_range_atr * atr_now)
    breakout_from = max(idx - RETEST_MAX_BARS, 1)
    breakout_to = idx
    bull_breakout_seen = False
    bear_breakout_seen = False
    for b_idx in range(breakout_from, breakout_to):
        atr_b = atr14[b_idx]
        if atr_b is None or float(atr_b) <= 0:
            continue
        atr_b = float(atr_b)
        if (
            closes[b_idx] > range_high + (rb_breakout_buffer_atr * atr_b)
            and (highs[b_idx] - lows[b_idx]) >= (MIN_IMPULSE_ATR * atr_b)
        ):
            bull_breakout_seen = True
        if (
            closes[b_idx] < range_low - (rb_breakout_buffer_atr * atr_b)
            and (highs[b_idx] - lows[b_idx]) >= (MIN_IMPULSE_ATR * atr_b)
        ):
            bear_breakout_seen = True
    bull_retest_ok = abs(lows[idx] - range_high) <= (rb_retest_tol_atr * atr_now)
    bull_confirm_ok = closes[idx] > (range_high + (rb_confirm_close_buffer_atr * atr_now))
    bear_retest_ok = abs(highs[idx] - range_low) <= (rb_retest_tol_atr * atr_now)
    bear_confirm_ok = closes[idx] < (range_low - (rb_confirm_close_buffer_atr * atr_now))
    rb_long_trigger = rb_has_history and (not rb_range_too_small) and bull_breakout_seen and bull_retest_ok and bull_confirm_ok
    rb_short_trigger = rb_has_history and (not rb_range_too_small) and bear_breakout_seen and bear_retest_ok and bear_confirm_ok

    lines.append("")
    lines.append("RB_RTG conditions:")
    lines.append(f"- has_history: {rb_has_history}")
    lines.append(f"- atr_ready: {atr_now > 0}")
    lines.append(f"- range_height_ok(min={rb_min_range_atr:.2f}*ATR): {not rb_range_too_small}")
    lines.append(f"- bull_breakout_seen(buffer={rb_breakout_buffer_atr:.2f}*ATR): {bull_breakout_seen}")
    lines.append(f"- bull_retest_ok(tol={rb_retest_tol_atr:.2f}*ATR): {bull_retest_ok}")
    lines.append(f"- bull_confirm_ok(close_buf={rb_confirm_close_buffer_atr:.2f}*ATR): {bull_confirm_ok}")
    lines.append(f"- bear_breakout_seen(buffer={rb_breakout_buffer_atr:.2f}*ATR): {bear_breakout_seen}")
    lines.append(f"- bear_retest_ok(tol={rb_retest_tol_atr:.2f}*ATR): {bear_retest_ok}")
    lines.append(f"- bear_confirm_ok(close_buf={rb_confirm_close_buffer_atr:.2f}*ATR): {bear_confirm_ok}")
    lines.append(f"- TRIGGER_LONG: {rb_long_trigger}")
    lines.append(f"- TRIGGER_SHORT: {rb_short_trigger}")

    # FB_FADE conditions.
    fb_has_history = idx > RANGE_LEN
    sweep_up = highs[idx] > range_high
    close_inside_after_up = closes[idx] < range_high
    sweep_down = lows[idx] < range_low
    close_inside_after_down = closes[idx] > range_low
    ema_dist_pct = abs((closes[idx] - ema200[idx]) / max(ema200[idx], 1e-10)) * 100.0

    fb_short_conf = None
    fb_long_conf = None
    if sweep_up and close_inside_after_up:
        c = 0.55
        if (highs[idx] - range_high) / atr_now >= 0.30:
            c += 0.15
        if (range_high - closes[idx]) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
            c += 0.10
        if abs(range_high - ema200[idx]) <= (0.25 * atr_now):
            c += 0.10
        atr_ratio = atr_now / max(closes[idx], 1e-10)
        c -= 0.10 if (atr_ratio < 0.001 or atr_ratio > 0.05) else 0.0
        fb_short_conf = c
    if sweep_down and close_inside_after_down:
        c = 0.55
        if (range_low - lows[idx]) / atr_now >= 0.30:
            c += 0.15
        if (closes[idx] - range_low) / max(highs[idx] - lows[idx], 1e-10) >= 0.50:
            c += 0.10
        if abs(range_low - ema200[idx]) <= (0.25 * atr_now):
            c += 0.10
        atr_ratio = atr_now / max(closes[idx], 1e-10)
        c -= 0.10 if (atr_ratio < 0.001 or atr_ratio > 0.05) else 0.0
        fb_long_conf = c

    fb_short_trigger = (
        fb_has_history
        and sweep_up
        and close_inside_after_up
        and fb_short_conf is not None
        and fb_short_conf >= fb_min_confidence
        and ema_dist_pct >= fb_min_dist_from_ema200_pct
    )
    fb_long_trigger = (
        fb_has_history
        and sweep_down
        and close_inside_after_down
        and fb_long_conf is not None
        and fb_long_conf >= fb_min_confidence
        and ema_dist_pct >= fb_min_dist_from_ema200_pct
    )

    lines.append("")
    lines.append("FB_FADE conditions:")
    lines.append(f"- has_history: {fb_has_history}")
    lines.append(f"- sweep_up(high>range_high): {sweep_up}")
    lines.append(f"- close_inside_after_up(close<range_high): {close_inside_after_up}")
    lines.append(f"- sweep_down(low<range_low): {sweep_down}")
    lines.append(f"- close_inside_after_down(close>range_low): {close_inside_after_down}")
    lines.append(f"- ema_dist_pct={_fmt_num(ema_dist_pct, 4)} >= {fb_min_dist_from_ema200_pct:.4f}: {ema_dist_pct >= fb_min_dist_from_ema200_pct}")
    lines.append(f"- conf_short={_fmt_num(fb_short_conf, 4)} >= {fb_min_confidence:.2f}: {(fb_short_conf or 0.0) >= fb_min_confidence}")
    lines.append(f"- conf_long={_fmt_num(fb_long_conf, 4)} >= {fb_min_confidence:.2f}: {(fb_long_conf or 0.0) >= fb_min_confidence}")
    lines.append(f"- TRIGGER_SHORT: {fb_short_trigger}")
    lines.append(f"- TRIGGER_LONG: {fb_long_trigger}")

    evaluated = evaluate_symbol_intents(
        symbol=symbol,
        candles=candles,
        signal_debug=True,
        threshold_profile=profile,
    )
    final_intents = list(evaluated.get("final_intents", []) or [])
    lines.append("")
    lines.append(f"Final intents count={len(final_intents)}")
    for intent in final_intents:
        lines.append(
            f"- {intent.get('strategy', '?')} {intent.get('side', '?')} "
            f"conf={_fmt_num(float(intent.get('confidence', 0.0)), 4)} "
            f"bar_ts_used={intent.get('bar_ts_used', intent.get('ts', 'n/a'))}"
        )

    compact = evaluated.get("debug_why_none", {}) or {}
    if compact:
        lines.append(f"debug_why_none={compact}")

    if candles_5m:
        lines.append("")
        lines.append(f"Last 12 bars (5m), candles={len(candles_5m)}:")
        start_5 = max(0, len(candles_5m) - 12)
        for i in range(start_5, len(candles_5m)):
            c = candles_5m[i]
            marker = " <- EARLY[5m](-1)" if i == len(candles_5m) - 1 else ""
            lines.append(
                f"- {_candle_ts_utc(c)}{marker} "
                f"O={_fmt_num(float(c.get('open', 0.0)))} "
                f"H={_fmt_num(float(c.get('high', 0.0)))} "
                f"L={_fmt_num(float(c.get('low', 0.0)))} "
                f"C={_fmt_num(float(c.get('close', 0.0)))}"
            )
    return "\n".join(lines)


def evaluate_symbol_intents_with_plugins(
    symbol: str,
    *,
    candles_15m: List[Dict[str, float]],
    candles_5m: Optional[List[Dict[str, float]]],
    mtf_snapshot: Dict[int, Dict[str, Any]],
    bias_info: Dict[str, Any],
    signal_debug: bool = False,
    timeframe: str = "15",
    bar_ts_used: str = "",
) -> Dict[str, object]:
    """
    Thin compatibility wrapper for plugin architecture.
    Returns legacy evaluate_* dict shape for existing consumers.
    """
    from scalper.settings import get_settings
    from scalper.strategies import (
        evaluate_enabled_first,
        load_enabled_strategies,
        strategy_result_to_evaluated,
    )

    settings = get_settings()
    strategies = load_enabled_strategies(settings)
    ctx: Dict[str, Any] = {
        "candles_15m": candles_15m or [],
        "candles_5m": candles_5m or [],
        "mtf_snapshot": mtf_snapshot or {},
        "bias_info": bias_info or {},
        "bar_ts_used": str(bar_ts_used or ""),
        "signal_debug": bool(signal_debug),
        "timeframe": str(timeframe or "15"),
        "v3_params": {
            "DONCHIAN_N_15M": settings.strategy_v3.donchian_n_15m,
            "BODY_ATR_15M": settings.strategy_v3.body_atr_15m,
            "TREND_SEP_ATR_1H": settings.strategy_v3.trend_sep_atr_1h,
            "USE_5M_CONFIRM": settings.strategy_v3.use_5m_confirm,
        },
        "i15": max(0, len(candles_15m or []) - 1),
        "sl_atr_mult": float(_get_config_float("PULLBACK_SL_ATR_MULT", 0.60)),
        "tp_r": float(_get_config_float("PAPER_TP_ATR", 1.5)),
    }
    result = evaluate_enabled_first(symbol=symbol, context=ctx, strategies=strategies)
    return strategy_result_to_evaluated(result, context=ctx)
