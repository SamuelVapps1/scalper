from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from scalper.scalper_types import Intent


def _get_lsr_params(settings: Any) -> Dict[str, Any]:
    s = getattr(settings, "strategy_v3", settings)
    return {
        "lookback_bars": int(getattr(s, "lsr_lookback_bars", 20) or 20),
        "min_wick_ratio": float(getattr(s, "lsr_min_wick_ratio", 0.5) or 0.5),
        "ema_tol_pct": float(getattr(s, "lsr_ema_tol_pct", 0.003) or 0.003),
        "sl_atr_mult": float(getattr(s, "lsr_sl_atr_mult", 0.3) or 0.3),
        "max_tp_atr_mult": float(getattr(s, "lsr_max_tp_atr_mult", 6.0) or 6.0),
        "entry_mode": str(getattr(s, "lsr_entry_mode", "close") or "close").lower(),
    }


def evaluate_last_bar(df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate LSR conditions on the last closed bar and return reconcile diagnostics.
    """
    tf = str(ctx.get("tf", "15"))
    settings = ctx.get("settings")
    p = _get_lsr_params(settings)

    out: Dict[str, Any] = {
        "ok_long": False,
        "ok_short": False,
        "reasons_long": [],
        "reasons_short": [],
        "values": {},
    }
    if tf != "5":
        out["reasons_long"].append("lsr_tf_not_5m")
        out["reasons_short"].append("lsr_tf_not_5m")
        return out
    if df.empty or len(df) < max(5, p["lookback_bars"]):
        out["reasons_long"].append("lsr_not_enough_bars")
        out["reasons_short"].append("lsr_not_enough_bars")
        return out

    cur = df.iloc[-1]
    prev_n = df.iloc[-p["lookback_bars"] : -1]
    if prev_n.empty:
        out["reasons_long"].append("lsr_prev_window_empty")
        out["reasons_short"].append("lsr_prev_window_empty")
        return out

    prev_range_high = float(prev_n["high"].max())
    prev_range_low = float(prev_n["low"].min())
    o = float(cur.get("open", 0) or 0)
    h = float(cur.get("high", 0) or 0)
    l = float(cur.get("low", 0) or 0)
    c = float(cur.get("close", 0) or 0)
    atr14 = float(cur.get("atr14", 0) or 0)
    ema200 = float(cur.get("ema200", 0) or 0)
    ema200_prev10 = float(cur.get("ema200_prev10", ema200) or ema200)
    ema_slope = ema200 - ema200_prev10
    atr_pct = (atr14 / max(c, 1e-10)) * 100.0 if c > 0 else 0.0
    wick_denom = (h - l + 1e-9)
    upper_wick_ratio = (h - max(o, c)) / wick_denom
    lower_wick_ratio = (min(o, c) - l) / wick_denom

    risk_settings = getattr(settings, "risk", settings)
    min_atr_pct = float(getattr(risk_settings, "min_atr_pct_universe", 0.003) or 0.003) * 100.0

    out["values"] = {
        "prev_range_high": prev_range_high,
        "prev_range_low": prev_range_low,
        "upper_wick_ratio": upper_wick_ratio,
        "lower_wick_ratio": lower_wick_ratio,
        "atr_pct": atr_pct,
        "ema200": ema200,
        "ema_slope": ema_slope,
        "min_atr_pct": min_atr_pct,
    }

    # Balanced ATR gate (existing vol filter remains in scanner as well).
    if atr_pct < min_atr_pct:
        out["reasons_long"].append("lsr_atr_below_min")
        out["reasons_short"].append("lsr_atr_below_min")

    # SHORT sweep-up reject.
    short_struct = (h > prev_range_high) and (c < prev_range_high)
    short_wick = upper_wick_ratio >= p["min_wick_ratio"]
    short_ema = (c <= ema200 * (1.0 + p["ema_tol_pct"])) or (ema_slope <= 0.0)
    if not short_struct:
        out["reasons_short"].append("lsr_short_struct_fail")
    if not short_wick:
        out["reasons_short"].append("lsr_short_wick_fail")
    if not short_ema:
        out["reasons_short"].append("lsr_short_ema_filter_fail")
    out["ok_short"] = short_struct and short_wick and short_ema and atr_pct >= min_atr_pct

    # LONG sweep-down reject.
    long_struct = (l < prev_range_low) and (c > prev_range_low)
    long_wick = lower_wick_ratio >= p["min_wick_ratio"]
    long_ema = (c >= ema200 * (1.0 - p["ema_tol_pct"])) or (ema_slope >= 0.0)
    if not long_struct:
        out["reasons_long"].append("lsr_long_struct_fail")
    if not long_wick:
        out["reasons_long"].append("lsr_long_wick_fail")
    if not long_ema:
        out["reasons_long"].append("lsr_long_ema_filter_fail")
    out["ok_long"] = long_struct and long_wick and long_ema and atr_pct >= min_atr_pct

    return out


def generate_intents(df: pd.DataFrame, ctx: Dict[str, Any]) -> List[Intent]:
    """
    LIQUIDITY_SWEEP_REVERSAL (LSR) for 5m timeframe.
    """
    symbol = str(ctx.get("symbol", "")).upper()
    tf = str(ctx.get("tf", "15"))
    p = _get_lsr_params(ctx.get("settings"))
    recon = evaluate_last_bar(df, ctx)
    vals = dict(recon.get("values", {}) or {})
    if not vals:
        return []

    cur = df.iloc[-1]
    o = float(cur.get("open", 0) or 0)
    h = float(cur.get("high", 0) or 0)
    l = float(cur.get("low", 0) or 0)
    c = float(cur.get("close", 0) or 0)
    atr14 = float(cur.get("atr14", 0) or 0)
    bar_ts = str(cur.get("ts", ctx.get("bar_ts_used", "")) or "")
    prev_range_high = float(vals.get("prev_range_high", 0) or 0)
    prev_range_low = float(vals.get("prev_range_low", 0) or 0)

    intents: List[Intent] = []
    if tf != "5" or c <= 0 or atr14 <= 0:
        return intents

    # Entry mode: close (default) or break.
    # "break" approximates confirmation trigger with signal bar extremes.
    short_entry = c if p["entry_mode"] != "break" else min(c, l)
    long_entry = c if p["entry_mode"] != "break" else max(c, h)

    if recon.get("ok_short"):
        sl = h + p["sl_atr_mult"] * atr14
        risk = abs(sl - short_entry)
        tp_raw = prev_range_low
        tp_cap = short_entry - p["max_tp_atr_mult"] * atr14
        tp = max(tp_raw, tp_cap)
        sl_pct = risk / max(short_entry, 1e-10) * 100.0
        tp_pct = abs(short_entry - tp) / max(short_entry, 1e-10) * 100.0
        intents.append(
            Intent(
                symbol=symbol,
                tf=tf,
                side="SHORT",
                setup="Liquidity sweep reversal",
                confidence=0.67,
                entry=short_entry,
                sl=sl,
                tp=tp,
                sl_pct=sl_pct,
                tp_pct=tp_pct,
                bar_ts_used=bar_ts,
                reason="LSR short: sweep above prior range high with rejection wick",
                meta={
                    "lsr_signal": "SWEEP_UP_REJECT",
                    "prev_range_high": prev_range_high,
                    "prev_range_low": prev_range_low,
                    "target1_r": 1.0,
                    "partial_tp_at_r": 1.0,
                    "entry_mode": p["entry_mode"],
                },
            )
        )

    if recon.get("ok_long"):
        sl = l - p["sl_atr_mult"] * atr14
        risk = abs(long_entry - sl)
        tp_raw = prev_range_high
        tp_cap = long_entry + p["max_tp_atr_mult"] * atr14
        tp = min(tp_raw, tp_cap)
        sl_pct = risk / max(long_entry, 1e-10) * 100.0
        tp_pct = abs(tp - long_entry) / max(long_entry, 1e-10) * 100.0
        intents.append(
            Intent(
                symbol=symbol,
                tf=tf,
                side="LONG",
                setup="Liquidity sweep reversal",
                confidence=0.67,
                entry=long_entry,
                sl=sl,
                tp=tp,
                sl_pct=sl_pct,
                tp_pct=tp_pct,
                bar_ts_used=bar_ts,
                reason="LSR long: sweep below prior range low with rejection wick",
                meta={
                    "lsr_signal": "SWEEP_DOWN_REJECT",
                    "prev_range_high": prev_range_high,
                    "prev_range_low": prev_range_low,
                    "target1_r": 1.0,
                    "partial_tp_at_r": 1.0,
                    "entry_mode": p["entry_mode"],
                },
            )
        )

    return intents


def debug_lsr_harness() -> Dict[str, Any]:
    """
    Tiny unit-style harness (no pytest): crafted OHLC to trigger both short/long sweeps.
    """
    def _mk_df(direction: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        # Build calm base range.
        px = 100.0
        for i in range(1, 26):
            rows.append(
                {
                    "ts": f"2026-01-01T00:{i:02d}:00+00:00",
                    "open": px,
                    "high": px + 0.6,
                    "low": px - 0.6,
                    "close": px + (0.05 if i % 2 else -0.05),
                    "ema20": 100.0,
                    "ema50": 100.0,
                    "ema200": 100.0,
                    "ema200_prev10": 100.0,
                    "atr14": 0.9,
                }
            )
        if direction == "short":
            # Sweep up, reject, close back below range high.
            rows[-1].update({"open": 100.4, "high": 102.8, "low": 99.9, "close": 100.2})
        else:
            # Sweep down, reject, close back above range low.
            rows[-1].update({"open": 99.6, "high": 100.1, "low": 97.2, "close": 99.8})
        return pd.DataFrame(rows)

    settings_stub = type(
        "S",
        (),
        {
            "strategy_v3": type(
                "SV3",
                (),
                {
                    "lsr_lookback_bars": 20,
                    "lsr_min_wick_ratio": 0.5,
                    "lsr_ema_tol_pct": 0.003,
                    "lsr_sl_atr_mult": 0.3,
                    "lsr_max_tp_atr_mult": 6.0,
                    "lsr_entry_mode": "close",
                },
            )(),
            "risk": type("R", (), {"min_atr_pct_universe": 0.003})(),
        },
    )()

    ctx = {"symbol": "TESTUSDT", "tf": "5", "settings": settings_stub, "bar_ts_used": ""}
    short_intents = generate_intents(_mk_df("short"), ctx)
    long_intents = generate_intents(_mk_df("long"), ctx)
    return {
        "short_triggered": bool([x for x in short_intents if x.side == "SHORT"]),
        "long_triggered": bool([x for x in long_intents if x.side == "LONG"]),
        "short_count": len(short_intents),
        "long_count": len(long_intents),
    }

