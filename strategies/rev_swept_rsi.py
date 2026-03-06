from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from indicators import rsi_wilder
from scalper.types import Intent


def _params(settings: Any) -> Dict[str, Any]:
    s = getattr(settings, "strategy_v3", settings)
    return {
        "enabled": bool(getattr(s, "rev_enabled", True)),
        "sweep_lookback_bars": int(getattr(s, "rev_sweep_lookback_bars", 30) or 30),
        "sweep_tol_atr": float(getattr(s, "rev_sweep_tol_atr", 0.25) or 0.25),
        "rsi_period": int(getattr(s, "rev_rsi_period", 14) or 14),
        "pivot_left": int(getattr(s, "rev_pivot_left", 3) or 3),
        "pivot_right": int(getattr(s, "rev_pivot_right", 3) or 3),
        "min_rsi_delta": float(getattr(s, "rev_min_rsi_delta", 3.0) or 3.0),
        "ema200_dist_pct": float(getattr(s, "rev_ema200_dist_pct", 0.01) or 0.01),
        "sl_atr_mult": float(getattr(s, "rev_sl_atr_mult", 0.35) or 0.35),
        "sl_buffer_atr": float(getattr(s, "rev_sl_buffer_atr", 0.10) or 0.10),
        "entry_mode": str(getattr(s, "rev_entry_mode", "bos") or "bos").strip().lower(),
        "max_trend_sep_atr_1h": float(getattr(s, "rev_max_trend_sep_atr_1h", 1.0) or 1.0),
        "require_1h_align": bool(
            getattr(s, "rev_require_1h_align", False) or getattr(s, "require_1h_ema200_align", False)
        ),
        "bos_lookback_5m": int(getattr(s, "bos_lookback_5m", 20) or 20),
    }


def _pivot_lows(values: List[float], left: int, right: int) -> List[int]:
    pivots: List[int] = []
    n = len(values)
    for i in range(left, n - right):
        v = values[i]
        if all(v <= values[j] for j in range(i - left, i + right + 1)):
            pivots.append(i)
    return pivots


def _pivot_highs(values: List[float], left: int, right: int) -> List[int]:
    pivots: List[int] = []
    n = len(values)
    for i in range(left, n - right):
        v = values[i]
        if all(v >= values[j] for j in range(i - left, i + right + 1)):
            pivots.append(i)
    return pivots


def _trend_context_ok(
    *,
    mtf_snapshot: Dict[int, Dict[str, Any]],
    side: str,
    ema200_dist_pct: float,
    max_trend_sep_atr_1h: float,
    require_1h_align: bool,
) -> Tuple[bool, str, float]:
    snap_15 = (mtf_snapshot or {}).get(15, {}) or {}
    snap_1h = (mtf_snapshot or {}).get(60, {}) or {}

    close_15 = float(snap_15.get("close", 0) or 0)
    ema200_15 = float(snap_15.get("ema200", 0) or 0)
    if close_15 <= 0 or ema200_15 <= 0:
        return False, "ctx_15m_missing", 0.0
    dist_15 = abs(close_15 - ema200_15) / max(close_15, 1e-10)
    if dist_15 > ema200_dist_pct:
        return False, "ctx_ema200_dist_too_far", dist_15

    close_1h = float(snap_1h.get("close", 0) or 0)
    ema200_1h = float(snap_1h.get("ema200", 0) or 0)
    atr_1h = float(snap_1h.get("atr14", 0) or 0)
    if close_1h > 0 and ema200_1h > 0 and atr_1h > 0:
        sep_atr_1h = abs(close_1h - ema200_1h) / max(atr_1h, 1e-10)
        if sep_atr_1h > max_trend_sep_atr_1h:
            return False, "ctx_1h_trend_too_strong", dist_15
        if require_1h_align:
            if side == "LONG" and close_1h < ema200_1h:
                return False, "ctx_1h_not_aligned_long", dist_15
            if side == "SHORT" and close_1h > ema200_1h:
                return False, "ctx_1h_not_aligned_short", dist_15

    return True, "ok", dist_15


def evaluate_reconcile(df_5m: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate REV_SWEPT_RSI on last closed 5m bar and return pass/fail reasons.
    """
    out: Dict[str, Any] = {
        "ok_long": False,
        "ok_short": False,
        "reasons_long": [],
        "reasons_short": [],
        "values": {},
    }
    p = _params(ctx.get("settings"))
    if not p["enabled"]:
        out["reasons_long"].append("rev_disabled")
        out["reasons_short"].append("rev_disabled")
        return out

    if str(ctx.get("tf", "5")) != "5":
        out["reasons_long"].append("rev_tf_not_5m")
        out["reasons_short"].append("rev_tf_not_5m")
        return out

    if df_5m.empty or len(df_5m) < max(40, p["sweep_lookback_bars"] + p["pivot_left"] + p["pivot_right"] + 5):
        out["reasons_long"].append("rev_not_enough_bars")
        out["reasons_short"].append("rev_not_enough_bars")
        return out

    cur = df_5m.iloc[-1]
    close = float(cur.get("close", 0) or 0)
    high = float(cur.get("high", 0) or 0)
    low = float(cur.get("low", 0) or 0)
    atr14 = float(cur.get("atr14", 0) or 0)
    if close <= 0 or atr14 <= 0:
        out["reasons_long"].append("rev_missing_close_or_atr")
        out["reasons_short"].append("rev_missing_close_or_atr")
        return out

    prior = df_5m.iloc[-(p["sweep_lookback_bars"] + 1) : -1]
    if prior.empty:
        out["reasons_long"].append("rev_prior_window_empty")
        out["reasons_short"].append("rev_prior_window_empty")
        return out
    prior_low = float(prior["low"].min())
    prior_high = float(prior["high"].max())

    sweep_bull = low < (prior_low - p["sweep_tol_atr"] * atr14)
    sweep_bear = high > (prior_high + p["sweep_tol_atr"] * atr14)

    lows = [float(x or 0) for x in df_5m["low"].tolist()]
    highs = [float(x or 0) for x in df_5m["high"].tolist()]
    closes = [float(x or 0) for x in df_5m["close"].tolist()]
    rsi_list = rsi_wilder(closes, period=p["rsi_period"])
    rsi = float(rsi_list[-1] or 0.0) if rsi_list else 0.0
    rsi_safe = [float(x or 0.0) for x in rsi_list]

    piv_l = _pivot_lows(lows, p["pivot_left"], p["pivot_right"])
    piv_h = _pivot_highs(highs, p["pivot_left"], p["pivot_right"])
    bull_div = False
    bear_div = False
    prev_swing_low = prior_low
    prev_swing_high = prior_high

    if len(piv_l) >= 2:
        i1, i2 = piv_l[-2], piv_l[-1]
        price_ll = lows[i2] < lows[i1]
        rsi_hl = (rsi_safe[i2] - rsi_safe[i1]) >= p["min_rsi_delta"]
        bull_div = bool(price_ll and rsi_hl)
        prev_swing_low = lows[i1]
    if len(piv_h) >= 2:
        i1, i2 = piv_h[-2], piv_h[-1]
        price_hh = highs[i2] > highs[i1]
        rsi_lh = (rsi_safe[i1] - rsi_safe[i2]) >= p["min_rsi_delta"]
        bear_div = bool(price_hh and rsi_lh)
        prev_swing_high = highs[i1]

    bos_n = max(3, int(p["bos_lookback_5m"]))
    struct_prev = df_5m.iloc[-(bos_n + 1) : -1]
    bos_up = bool(not struct_prev.empty and close > float(struct_prev["high"].max()))
    bos_dn = bool(not struct_prev.empty and close < float(struct_prev["low"].min()))

    mode = p["entry_mode"]
    if mode == "reenter":
        entry_long_ok = close > prev_swing_low
        entry_short_ok = close < prev_swing_high
    elif mode == "close":
        entry_long_ok = True
        entry_short_ok = True
    else:  # bos
        entry_long_ok = bos_up
        entry_short_ok = bos_dn

    risk_settings = getattr(ctx.get("settings"), "risk", ctx.get("settings"))
    min_atr_pct = float(getattr(risk_settings, "min_atr_pct_universe", 0.003) or 0.003) * 100.0
    atr_pct_5m = (atr14 / max(close, 1e-10)) * 100.0

    ctx_long_ok, ctx_long_reason, ema200_dist = _trend_context_ok(
        mtf_snapshot=ctx.get("mtf_snapshot", {}) or {},
        side="LONG",
        ema200_dist_pct=p["ema200_dist_pct"],
        max_trend_sep_atr_1h=p["max_trend_sep_atr_1h"],
        require_1h_align=p["require_1h_align"],
    )
    ctx_short_ok, ctx_short_reason, _ = _trend_context_ok(
        mtf_snapshot=ctx.get("mtf_snapshot", {}) or {},
        side="SHORT",
        ema200_dist_pct=p["ema200_dist_pct"],
        max_trend_sep_atr_1h=p["max_trend_sep_atr_1h"],
        require_1h_align=p["require_1h_align"],
    )

    if not sweep_bull:
        out["reasons_long"].append("rev_no_bull_sweep")
    if not bull_div:
        out["reasons_long"].append("rev_no_bull_div")
    if not entry_long_ok:
        out["reasons_long"].append(f"rev_entry_mode_fail_{mode}")
    if not ctx_long_ok:
        out["reasons_long"].append(ctx_long_reason)
    if atr_pct_5m < min_atr_pct:
        out["reasons_long"].append("rev_atr_pct_below_min")

    if not sweep_bear:
        out["reasons_short"].append("rev_no_bear_sweep")
    if not bear_div:
        out["reasons_short"].append("rev_no_bear_div")
    if not entry_short_ok:
        out["reasons_short"].append(f"rev_entry_mode_fail_{mode}")
    if not ctx_short_ok:
        out["reasons_short"].append(ctx_short_reason)
    if atr_pct_5m < min_atr_pct:
        out["reasons_short"].append("rev_atr_pct_below_min")

    out["ok_long"] = sweep_bull and bull_div and entry_long_ok and ctx_long_ok and atr_pct_5m >= min_atr_pct
    out["ok_short"] = sweep_bear and bear_div and entry_short_ok and ctx_short_ok and atr_pct_5m >= min_atr_pct
    out["values"] = {
        "sweep_bull": sweep_bull,
        "sweep_bear": sweep_bear,
        "bull_div": bull_div,
        "bear_div": bear_div,
        "ema200_dist": ema200_dist,
        "rsi": rsi,
        "atr_pct_5m": atr_pct_5m,
        "prior_low": prior_low,
        "prior_high": prior_high,
        "prev_swing_low": prev_swing_low,
        "prev_swing_high": prev_swing_high,
        "entry_mode": mode,
    }
    return out


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    REV_SWEPT_RSI: liquidity sweep + RSI divergence reversal on 5m,
    gated by 15m/1h EMA context.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    p = _params(ctx.get("settings"))
    recon = evaluate_reconcile(df, ctx)
    vals = dict(recon.get("values", {}) or {})
    if not vals:
        return []

    cur = df.iloc[-1]
    close = float(cur.get("close", 0) or 0)
    high = float(cur.get("high", 0) or 0)
    low = float(cur.get("low", 0) or 0)
    atr14 = float(cur.get("atr14", 0) or 0)
    bar_ts = str(cur.get("ts", ctx.get("bar_ts_used", "")) or "")
    intents: List[Intent] = []

    if recon.get("ok_long"):
        sl = low - p["sl_atr_mult"] * atr14 - p["sl_buffer_atr"] * atr14
        sl_dist = abs(close - sl)
        sl_pct = sl_dist / max(close, 1e-10) * 100.0
        logging.info(
            "REV_CANDIDATE symbol=%s dir=LONG sweep=%s div=%s ema200_dist=%.4f rsi=%.2f atr_pct_5m=%.4f reason=%s",
            symbol,
            bool(vals.get("sweep_bull", False)),
            bool(vals.get("bull_div", False)),
            float(vals.get("ema200_dist", 0.0) or 0.0),
            float(vals.get("rsi", 0.0) or 0.0),
            float(vals.get("atr_pct_5m", 0.0) or 0.0),
            "ok_long",
        )
        intents.append(
            Intent(
                symbol=symbol,
                tf="5",
                side="LONG",
                setup="REV_SWEPT_RSI",
                confidence=0.68,
                entry=close,
                sl=sl,
                tp=None,
                sl_pct=sl_pct,
                tp_pct=None,
                bar_ts_used=bar_ts,
                reason="REV long: sweep-down + RSI bullish divergence",
                meta={
                    "sl_hint": sl,
                    "entry_mode": p["entry_mode"],
                    "sweep": "bullish",
                    "divergence": "bullish",
                    "atr14_5m": atr14,
                },
            )
        )
    if recon.get("ok_short"):
        sl = high + p["sl_atr_mult"] * atr14 + p["sl_buffer_atr"] * atr14
        sl_dist = abs(sl - close)
        sl_pct = sl_dist / max(close, 1e-10) * 100.0
        logging.info(
            "REV_CANDIDATE symbol=%s dir=SHORT sweep=%s div=%s ema200_dist=%.4f rsi=%.2f atr_pct_5m=%.4f reason=%s",
            symbol,
            bool(vals.get("sweep_bear", False)),
            bool(vals.get("bear_div", False)),
            float(vals.get("ema200_dist", 0.0) or 0.0),
            float(vals.get("rsi", 0.0) or 0.0),
            float(vals.get("atr_pct_5m", 0.0) or 0.0),
            "ok_short",
        )
        intents.append(
            Intent(
                symbol=symbol,
                tf="5",
                side="SHORT",
                setup="REV_SWEPT_RSI",
                confidence=0.68,
                entry=close,
                sl=sl,
                tp=None,
                sl_pct=sl_pct,
                tp_pct=None,
                bar_ts_used=bar_ts,
                reason="REV short: sweep-up + RSI bearish divergence",
                meta={
                    "sl_hint": sl,
                    "entry_mode": p["entry_mode"],
                    "sweep": "bearish",
                    "divergence": "bearish",
                    "atr14_5m": atr14,
                },
            )
        )
    return intents

