#!/usr/bin/env python3
"""
REPLAY runner: simulate many paper trades over historical data without real-time wait.
DRY RUN only. No Telegram. No orders. Uses rate-limit pacing.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root on path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(1, str(_project_root / "scripts"))

# Load config (dotenv) same as bot - must run before other project imports
import config as _config

from bybit import _pace_before_request, _to_bybit_interval
from indicators import atr_wilder, ema
from mtf import compute_4h_bias

import candle_cache
from paper import PaperPosition, update_and_maybe_close
from paper_engine import try_open_position
from signals import (
    evaluate_symbol_intents_v1,
    evaluate_symbol_intents_v2_trend_pullback,
)
from strategies.strategy_v3_tcb import _build_15m_to_5m_index, v3_tcb_evaluate
from storage import compute_paper_kpis

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)

def _candle_ts_ms(c: Dict[str, Any]) -> int:
    ts = c.get("timestamp") or c.get("timestamp_utc")
    if isinstance(ts, (int, float)):
        return int(ts) if ts >= 1e12 else int(ts * 1000)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
    return 0


def fetch_klines_range(
    symbol: str,
    interval_min: int,
    start_ms: int,
    end_ms: int,
    *,
    pace_ms: int = 300,
    use_cache: bool = True,
    cache_days: int = 365,
    cache_only: bool = False,
    cache_hits: Optional[Dict[str, int]] = None,
    cache_misses: Optional[Dict[str, int]] = None,
    _timing_out: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all klines in [start_ms, end_ms]. Uses candle_cache.get_candles with pagination."""
    return candle_cache.get_candles(
        symbol=symbol,
        tf=interval_min,
        start_ms=start_ms,
        end_ms=end_ms,
        use_cache=use_cache,
        pace_ms=pace_ms,
        cache_days=cache_days,
        cache_only=cache_only,
        cache_hits=cache_hits if use_cache else None,
        cache_misses=cache_misses if use_cache else None,
        _timing_out=_timing_out,
    )


def fetch_candles_for_replay(
    symbols: List[str],
    days: int,
    tf_trigger: int,
    tf_timing: int,
    tf_bias: int,
    tf_setup: int,
) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
    """Fetch candles for all symbols and TFs. Used by sweep to reuse data."""
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)
    tfs = [tf_bias, tf_setup, tf_trigger, tf_timing]
    lookbacks = {
        tf_bias: max(250, days * 6 + 50),
        tf_setup: max(250, days * 24 + 50),
        tf_trigger: max(400, days * 24 * 4 + 100),
        tf_timing: max(400, days * 24 * 12 + 100),
    }
    out: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}
    for symbol in symbols:
        out[symbol] = {}
        for tf_min in tfs:
            lb = lookbacks.get(tf_min, 500)
            fetch_start = start_ms - lb * tf_min * 60 * 1000
            c = fetch_klines_range(symbol, tf_min, fetch_start, end_ms, pace_ms=300)
            out[symbol][tf_min] = c
    return out


def _build_closed_trade(
    pos: PaperPosition,
    close_ts: str,
    pnl_usdt: float,
    close_reason: str,
    *,
    exit_price: Optional[float] = None,
    entry_price: Optional[float] = None,
    sl_price: Optional[float] = None,
    qty: Optional[float] = None,
    risk_usdt: Optional[float] = None,
    risk_status: str = "ok",
) -> Dict[str, Any]:
    """Build closed trade dict with risk_usdt and r_multiple (SL-based, same as live bot)."""
    entry = float(pos.entry_price if entry_price is None else entry_price)
    sl = float(pos.sl_price if sl_price is None else sl_price)
    qty_est = float(pos.qty_est or 0)
    notional = float(pos.notional_usdt or 0)
    qty_val = float(qty) if qty is not None else (
        qty_est if qty_est > 0 else (notional / max(entry, 1e-10) if entry > 0 else 0.0)
    )
    risk_val = (
        float(risk_usdt)
        if risk_usdt is not None
        else abs(entry - sl) * max(0.0, qty_val)
    )
    r_multiple = (pnl_usdt / risk_val) if (risk_status != "invalid_risk" and risk_val > 0) else None
    out = {
        "symbol": pos.symbol,
        "side": pos.side,
        "setup": pos.strategy,
        "entry_ts": pos.entry_ts,
        "close_ts": close_ts,
        "pnl_usdt": pnl_usdt,
        "close_reason": close_reason,
        "exit_price": float(exit_price) if exit_price is not None else None,
        "entry_price": entry,
        "sl_price": sl,
        "tp_price": pos.tp_price,
        "notional_usdt": notional,
        "qty_est": qty_est,
        "qty": qty_val,
        "risk_usdt": (risk_val if risk_status != "invalid_risk" else None),
        "r_multiple": r_multiple,
        "risk_status": risk_status,
        "partial": 0,
    }
    return out


def _build_tf_indicators(candles: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    ts_list = [_candle_ts_ms(c) for c in candles]
    open_list = [float(c.get("open", 0) or 0) for c in candles]
    close_list = [float(c.get("close", 0) or 0) for c in candles]
    high_list = [float(c.get("high", 0) or 0) for c in candles]
    low_list = [float(c.get("low", 0) or 0) for c in candles]
    return {
        "ts": ts_list,
        "open": open_list,
        "close": close_list,
        "high": high_list,
        "low": low_list,
        "ema20": ema(close_list, 20),
        "ema50": ema(close_list, 50),
        "ema200": ema(close_list, 200),
        "atr14": atr_wilder(high_list, low_list, close_list, 14),
    }


def _build_ts_index_map(trigger_ts: List[int], target_ts: List[int]) -> List[int]:
    """Map each trigger ts to last target index with target_ts[idx] <= trigger_ts[i]."""
    if not trigger_ts:
        return []
    if not target_ts:
        return [-1] * len(trigger_ts)
    out = [-1] * len(trigger_ts)
    j = 0
    for i in range(len(trigger_ts)):
        while j + 1 < len(target_ts) and target_ts[j + 1] <= trigger_ts[i]:
            j += 1
        out[i] = j if target_ts[j] <= trigger_ts[i] else -1
    return out


def build_mtf_snapshot_at_ts(
    ind_by_tf: Dict[int, Dict[str, List[Any]]],
    trigger_to_tf_index: Dict[int, List[int]],
    trigger_idx: int,
    tfs: List[int],
) -> Dict[int, Dict[str, Any]]:
    """Build MTF snapshot from precomputed indicators at trigger index."""
    snapshot: Dict[int, Dict[str, Any]] = {}
    for tf_min in tfs:
        ind = ind_by_tf.get(tf_min)
        idx_map = trigger_to_tf_index.get(tf_min)
        if not ind or not idx_map or trigger_idx >= len(idx_map):
            continue
        i_tf = idx_map[trigger_idx]
        if i_tf < 0:
            continue
        ts_list = ind["ts"]
        if i_tf >= len(ts_list):
            continue
        ema20_v = ind["ema20"][i_tf]
        ema50_v = ind["ema50"][i_tf]
        ema200_v = ind["ema200"][i_tf]
        atr14_v = ind["atr14"][i_tf]
        ema200_prev10 = ind["ema200"][i_tf - 10] if i_tf >= 10 else None
        snapshot[tf_min] = {
            "ema20": float(ema20_v) if ema20_v is not None else 0.0,
            "ema50": float(ema50_v) if ema50_v is not None else 0.0,
            "ema200": float(ema200_v) if ema200_v is not None else 0.0,
            "ema200_slope_10": (
                float(ema200_v) - float(ema200_prev10)
                if ema200_v is not None and ema200_prev10 is not None
                else None
            ),
            "atr14": float(atr14_v) if atr14_v is not None else 0.0,
            "open": float(ind["open"][i_tf]),
            "close": float(ind["close"][i_tf]),
            "high": float(ind["high"][i_tf]),
            "low": float(ind["low"][i_tf]),
            "ts": datetime.fromtimestamp(ts_list[i_tf] / 1000.0, tz=timezone.utc).isoformat(),
        }
    return snapshot


def run_replay(
    symbols: List[str],
    days: int,
    tf_trigger: int,
    tf_timing: int,
    tf_bias: int,
    tf_setup: int,
    step_bars: int,
    *,
    candles_by_symbol_tf: Optional[Dict[str, Dict[int, List[Dict[str, Any]]]]] = None,
    silent: bool = False,
    use_cache: bool = True,
    cache_days: int = 365,
    cache_only: bool = False,
    end_days_ago: Optional[int] = None,
    start_days_ago: Optional[int] = None,
) -> Dict[str, Any]:
    import config as _config

    now_dt = datetime.now(timezone.utc)
    if start_days_ago is not None and end_days_ago is not None:
        start_dt = now_dt - timedelta(days=start_days_ago)
        end_dt = now_dt - timedelta(days=end_days_ago)
        days = start_days_ago - end_days_ago
    elif end_days_ago is not None:
        end_dt = now_dt - timedelta(days=end_days_ago)
        start_dt = end_dt - timedelta(days=days)
    else:
        end_dt = now_dt
        start_dt = end_dt - timedelta(days=days)
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)

    tfs = [tf_bias, tf_setup, tf_trigger, tf_timing]
    lookbacks = {
        tf_bias: max(250, days * 6 + 50),
        tf_setup: max(250, days * 24 + 50),
        tf_trigger: max(400, days * 24 * 4 + 100),
        tf_timing: max(400, days * 24 * 12 + 100),
    }

    tp_r = getattr(_config, "PAPER_TP_ATR", 1.5)
    sl_atr_mult = getattr(_config, "PAPER_SL_ATR", 1.0)
    tol_atr = getattr(_config, "PULLBACK_TOL_ATR", 0.10)
    trend_min_sep = getattr(_config, "TREND_MIN_SEP_ATR", 0.35)
    momo_min_body = getattr(_config, "MOMO_MIN_BODY_ATR_5M", 0.25)
    be_at_r = getattr(_config, "BE_AT_R", 1.0)
    partial_tp_at_r = getattr(_config, "PARTIAL_TP_AT_R", 0.0)
    trail_after_r = getattr(_config, "TRAIL_AFTER_R", 0.0)
    v2 = getattr(_config, "V2_TREND_PULLBACK", True)
    v1 = getattr(_config, "STRATEGY_V1", False)
    v1_brk = getattr(_config, "V1_SETUP_BREAKOUT", False)
    v1_trap = getattr(_config, "V1_SETUP_TRAP", False)
    v3 = getattr(_config, "V3_TREND_BREAKOUT", False)
    donchian_n = getattr(_config, "DONCHIAN_N_15M", 20)
    body_atr_15m = getattr(_config, "BODY_ATR_15M", 0.25)
    trend_sep_atr_1h = getattr(_config, "TREND_SEP_ATR_1H", 0.8)
    use_5m_confirm = getattr(_config, "USE_5M_CONFIRM", True)

    range_info = (
        {"start_days_ago": start_days_ago, "end_days_ago": end_days_ago}
        if (start_days_ago is not None and end_days_ago is not None)
        else {"days": days, "end_days_ago": end_days_ago}
    )
    RUN_SIGNATURE = {
        "symbols": symbols,
        "range": range_info,
        "tf_trigger": tf_trigger,
        "tf_timing": tf_timing,
        "tf_bias": tf_bias,
        "tf_setup": tf_setup,
        "step_bars": step_bars,
        "setup_flags": {
            "V2_TREND_PULLBACK": v2,
            "STRATEGY_V1": v1,
            "V1_SETUP_BREAKOUT": v1_brk,
            "V1_SETUP_TRAP": v1_trap,
            "V3_TREND_BREAKOUT": v3,
        },
        "v3_params": {
            "V3_TREND_BREAKOUT": v3,
            "DONCHIAN_N_15M": donchian_n,
            "BODY_ATR_15M": body_atr_15m,
            "TREND_SEP_ATR_1H": trend_sep_atr_1h,
            "USE_5M_CONFIRM": use_5m_confirm,
        },
        "TP_R": tp_r,
        "SL_ATR_MULT": sl_atr_mult,
        "TOL_ATR": tol_atr,
        "TREND_MIN_SEP_ATR": trend_min_sep,
        "MOMO_MIN_BODY_ATR_5M": momo_min_body,
        "BE_AT_R": be_at_r,
        "PARTIAL_TP_AT_R": partial_tp_at_r,
        "TRAIL_AFTER_R": trail_after_r,
    }
    run_id = hashlib.sha256(json.dumps(RUN_SIGNATURE, sort_keys=True).encode()).hexdigest()[:12]

    if not silent:
        _log.info("RUN_SIGNATURE[%s]: %s", run_id, json.dumps(RUN_SIGNATURE, sort_keys=True))
        _log.info("=== ACTIVE_SETUPS ===")
        _log.info(
            "V2_TREND_PULLBACK=%s STRATEGY_V1=%s V1_SETUP_BREAKOUT=%s V1_SETUP_TRAP=%s V3_TREND_BREAKOUT=%s",
            RUN_SIGNATURE["setup_flags"]["V2_TREND_PULLBACK"],
            RUN_SIGNATURE["setup_flags"]["STRATEGY_V1"],
            RUN_SIGNATURE["setup_flags"]["V1_SETUP_BREAKOUT"],
            RUN_SIGNATURE["setup_flags"]["V1_SETUP_TRAP"],
            RUN_SIGNATURE["setup_flags"]["V3_TREND_BREAKOUT"],
        )
        _log.info(
            "STRATEGY_V1=%s V1_SETUP_BREAKOUT=%s V1_SETUP_TRAP=%s "
            "RETEST_CONFIRM_MODE=%s BOS_LOOKBACK_5M=%s BREAKOUT_STRONG_MARKET=%s "
            "BREAKOUT_STRONG_BODY_PCT=%s BREAKOUT_BUFFER_ATR=%s",
            RUN_SIGNATURE["setup_flags"]["STRATEGY_V1"],
            RUN_SIGNATURE["setup_flags"]["V1_SETUP_BREAKOUT"],
            RUN_SIGNATURE["setup_flags"]["V1_SETUP_TRAP"],
            getattr(_config, "RETEST_CONFIRM_MODE", "bos"),
            getattr(_config, "BOS_LOOKBACK_5M", 20),
            getattr(_config, "BREAKOUT_STRONG_MARKET", False),
            getattr(_config, "BREAKOUT_STRONG_BODY_PCT", 0.60),
            getattr(_config, "BREAKOUT_BUFFER_ATR", 0.10),
        )
        _log.info(
            "TRAP_MIN_WICK_ATR=%s REQUIRE_1H_EMA200_ALIGN=%s REQUIRE_5M_EMA20_CONFIRM=%s "
            "MIN_ATR_PCT_15M=%s MAX_ATR_PCT_15M=%s",
            getattr(_config, "TRAP_MIN_WICK_ATR", 0.8),
            getattr(_config, "REQUIRE_1H_EMA200_ALIGN", False),
            getattr(_config, "REQUIRE_5M_EMA20_CONFIRM", False),
            getattr(_config, "MIN_ATR_PCT_15M", 0.2),
            getattr(_config, "MAX_ATR_PCT_15M", 3.0),
        )
        _log.info("")

    cache_hits: Dict[str, int] = {}
    cache_misses: Dict[str, int] = {}
    t_total_start = time.perf_counter()
    load_candles_s = 0.0
    compute_indicators_s = 0.0
    precompute_s = 0.0
    walk_loop_s = 0.0
    export_s = 0.0

    if candles_by_symbol_tf is None:
        candles_by_symbol_tf = {}
        if not silent:
            _log.info("Fetching historical candles (use_cache=%s)...", use_cache)
        for symbol in symbols:
            candles_by_symbol_tf[symbol] = {}
            for tf_min in tfs:
                lb = lookbacks.get(tf_min, 500)
                fetch_end = end_ms
                fetch_start = start_ms - lb * tf_min * 60 * 1000
                timing_out: Dict[str, Any] = {}
                t0 = time.perf_counter()
                c = fetch_klines_range(
                    symbol, tf_min, fetch_start, fetch_end,
                    pace_ms=300,
                    use_cache=use_cache,
                    cache_days=cache_days,
                    cache_only=cache_only,
                    cache_hits=cache_hits if use_cache else None,
                    cache_misses=cache_misses if use_cache else None,
                    _timing_out=timing_out,
                )
                load_s = time.perf_counter() - t0
                load_candles_s += load_s
                _log.info(
                    "TIMING_CANDLES tf=%s bars=%d source=%s load_s=%.2f",
                    tf_min, len(c), timing_out.get("source", "?"), load_s,
                )
                candles_by_symbol_tf[symbol][tf_min] = c
                if not silent:
                    _log.info("  %s %dm: %d bars", symbol, tf_min, len(c))

    t_after_load = time.perf_counter()
    c15 = candles_by_symbol_tf[symbols[0]][tf_trigger]
    bars_15m = [c for c in c15 if start_ms <= _candle_ts_ms(c) <= end_ms]
    bars_15m_ts = [_candle_ts_ms(c) for c in bars_15m]

    # Precompute indicators once per symbol+TF and build trigger->TF index maps.
    ind_by_symbol_tf: Dict[str, Dict[int, Dict[str, List[Any]]]] = {}
    trigger_to_tf_by_symbol: Dict[str, Dict[int, List[int]]] = {}
    for sym in symbols:
        ind_by_symbol_tf[sym] = {}
        trigger_to_tf_by_symbol[sym] = {}
        for tf_min in tfs:
            candles_tf = candles_by_symbol_tf[sym][tf_min]
            ind = _build_tf_indicators(candles_tf)
            ind_by_symbol_tf[sym][tf_min] = ind
            trigger_to_tf_by_symbol[sym][tf_min] = _build_ts_index_map(bars_15m_ts, ind["ts"])

    compute_indicators_s = time.perf_counter() - t_after_load
    t_precompute_start = time.perf_counter()
    if not silent:
        _log.info("Walking %d trigger bars (step=%d)", len(bars_15m), step_bars)

    closed_trades: List[Dict[str, Any]] = []
    paper_equity = float(getattr(_config, "PAPER_EQUITY_USDT", 200.0))
    timeout_bars = int(getattr(_config, "PAPER_TIMEOUT_BARS", 12))
    fees_bps = float(getattr(_config, "PAPER_FEES_BPS", 6.0))

    skip_reasons = (
        "bias_none",
        "atr_filter_fail",
        "range_too_small",
        "breakout_not_triggered",
        "retest_not_filled",
        "bos_not_confirmed",
        "ema20_confirm_fail",
        "ema200_align_fail",
        "ema20_ema50_fail",
        "trend_weak",
        "momo_confirm_fail",
        "pullback_not_triggered",
        "no_active_setup",
        "score_below_min",
        "max_abs_dist_4h",
        "max_abs_dist_4h_hard",
        "rate_limited",
        "v3_bias_none",
        "v3_trend_align_fail",
        "v3_sep_fail",
        "v3_donchian_not_broken",
        "v3_body_too_small",
        "v3_missing_5m",
        "v3_5m_confirm_fail",
        "v3_not_enough_15m_bars",
        "v3_missing_4h_slope_inputs",
        "v3_missing_1h_inputs",
    )
    skip_total: Dict[str, int] = {r: 0 for r in skip_reasons}
    skip_by_symbol: Dict[str, Dict[str, int]] = {s: {r: 0 for r in skip_reasons} for s in symbols}
    v3_triggers_total = 0
    risk_invalid_count = 0

    v3_params_dict = {
        "DONCHIAN_N_15M": getattr(_config, "DONCHIAN_N_15M", 20),
        "BODY_ATR_15M": getattr(_config, "BODY_ATR_15M", 0.25),
        "TREND_SEP_ATR_1H": getattr(_config, "TREND_SEP_ATR_1H", 0.8),
        "USE_5M_CONFIRM": getattr(_config, "USE_5M_CONFIRM", True),
    }

    v3_map15_to_5: Dict[str, List[int]] = {}
    v3_close5: Dict[str, List[float]] = {}
    for sym in symbols:
        c15 = candles_by_symbol_tf[sym][tf_trigger]
        c5 = candles_by_symbol_tf[sym][tf_timing]
        v3_map15_to_5[sym] = _build_15m_to_5m_index(c15, c5)
        v3_close5[sym] = [float(c.get("close", 0) or 0) for c in c5]

    precompute_s = time.perf_counter() - t_precompute_start
    t_walk_start = time.perf_counter()
    total_steps = (len(bars_15m) + step_bars - 1) // step_bars
    for i, step in enumerate(range(0, len(bars_15m), step_bars)):
        bar = bars_15m[step]
        bar_ts_ms = _candle_ts_ms(bar)
        bar_ts_utc = bar.get("timestamp_utc", "")

        progress_every = getattr(_config, "REPLAY_PROGRESS_EVERY", 0)
        if progress_every > 0 and i % progress_every == 0:
            elapsed_s = time.perf_counter() - t_walk_start
            steps_per_s = i / elapsed_s if elapsed_s > 0 else 0.0
            _log.info(
                "PROGRESS i=%d/%d trades=%d elapsed_s=%.1f steps_per_s=%.1f",
                i, total_steps, len(closed_trades), elapsed_s, steps_per_s,
            )

        for symbol in symbols:
            c15_sym = candles_by_symbol_tf[symbol][tf_trigger]
            trigger_idx_15m = trigger_to_tf_by_symbol[symbol][tf_trigger][step]
            if trigger_idx_15m < 49:
                continue
            c15_up_to = c15_sym[: trigger_idx_15m + 1]

            mtf = build_mtf_snapshot_at_ts(
                ind_by_symbol_tf[symbol],
                trigger_to_tf_by_symbol[symbol],
                step,
                tfs,
            )
            snap_4h = mtf.get(tf_bias) or {}
            bias_info = compute_4h_bias(symbol, snap_4h)
            if (bias_info.get("bias") or "NONE") not in ("LONG", "SHORT"):
                skip_total["bias_none"] += 1
                skip_by_symbol[symbol]["bias_none"] += 1
                continue

            v2_trend_pullback = getattr(_config, "V2_TREND_PULLBACK", True)
            strategy_v1 = getattr(_config, "STRATEGY_V1", False)
            v3_trend_breakout = getattr(_config, "V3_TREND_BREAKOUT", False)
            if v3_trend_breakout and (bias_info.get("bias") or "NONE") in ("LONG", "SHORT"):
                i15 = trigger_idx_15m
                result = v3_tcb_evaluate(
                    symbol=symbol,
                    snapshot_symbol=mtf,
                    candles_15m=c15_sym,
                    candles_5m=None,
                    i15=i15,
                    params=v3_params_dict,
                    map15_to_5=v3_map15_to_5[symbol],
                    close5=v3_close5[symbol],
                )
                if result["ok"]:
                    cur = c15_up_to[i15]
                    low_15m = float(cur.get("low", 0) or 0)
                    high_15m = float(cur.get("high", 0) or 0)
                    atr15m = float(result["debug"].get("atr15m", 0) or 0)
                    sl_price = (
                        low_15m - sl_atr_mult * atr15m
                        if result["side"] == "LONG"
                        else high_15m + sl_atr_mult * atr15m
                    )
                    intent = {
                        "symbol": symbol,
                        "side": result["side"],
                        "strategy": "V3_TREND_BREAKOUT",
                        "close": result["debug"].get("close_15m"),
                        "level_ref": result["breakout_level"],
                        "entry_type": "market_sim",
                        "meta": {
                            "sl_hint": sl_price,
                            "tp_r_mult": tp_r,
                            "atr14": atr15m,
                        },
                    }
                    evaluated = {
                        "final_intents": [intent],
                        "market_snapshot": {"atr14": atr15m},
                    }
                else:
                    reason = str(result.get("reason", "") or "breakout_not_triggered").strip()
                    if reason not in skip_total:
                        reason = "breakout_not_triggered"
                    skip_total[reason] += 1
                    skip_by_symbol[symbol][reason] += 1
                    continue
            elif v2_trend_pullback and (bias_info.get("bias") or "NONE") in ("LONG", "SHORT"):
                bar_ts_v2 = bar.get("timestamp_utc", "") or ""
                evaluated = evaluate_symbol_intents_v2_trend_pullback(
                    symbol=symbol,
                    candles_15m=c15_up_to,
                    mtf_snapshot=mtf,
                    bias_info=bias_info,
                    bar_ts_used=bar_ts_v2,
                )
            elif strategy_v1 and (bias_info.get("bias") or "NONE") in ("LONG", "SHORT"):
                evaluated = evaluate_symbol_intents_v1(
                    symbol=symbol,
                    candles_15m=c15_up_to,
                    mtf_snapshot=mtf,
                    bias_info=bias_info,
                    signal_debug=False,
                    timeframe=str(tf_trigger),
                )
            else:
                evaluated = {"final_intents": [], "market_snapshot": {}, "skip_reason": "no_active_setup"}
            intents = evaluated.get("final_intents") or []
            if not intents:
                reason = str(evaluated.get("skip_reason") or "breakout_not_triggered").strip()
                if reason not in skip_total:
                    reason = "breakout_not_triggered"
                skip_total[reason] += 1
                skip_by_symbol[symbol][reason] += 1
                continue

            intent = intents[0]
            if intent.get("strategy") == "V3_TREND_BREAKOUT":
                v3_triggers_total += 1
                if getattr(_config, "LOG_V3_TRIGGERS", False):
                    _log.info(
                        "V3_TRIGGER %s %s close=%.4f level=%.4f bar_ts=%s",
                        symbol,
                        intent.get("side", ""),
                        float(intent.get("close", 0) or 0),
                        float(intent.get("level_ref", 0) or 0),
                        bar_ts_utc,
                    )
            market_snap = evaluated.get("market_snapshot") or {}
            atr_val = float(market_snap.get("atr14", 0) or 0)
            if atr_val <= 0:
                continue

            c5 = candles_by_symbol_tf[symbol][tf_timing]
            c5_from = [c for c in c5 if _candle_ts_ms(c) >= bar_ts_ms][:200]
            if not c5_from:
                continue

            pos_dict, skip_reason = try_open_position(
                intent,
                c5_from,
                market_snap,
                paper_position_usdt=paper_equity,
                sl_atr_mult=sl_atr_mult,
                tp_atr_mult=tp_r,
                intent_id=f"replay|{symbol}|{intent.get('strategy','')}|{bar_ts_utc}",
            )
            if skip_reason:
                skip_total[skip_reason] = skip_total.get(skip_reason, 0) + 1
                skip_by_symbol[symbol][skip_reason] = skip_by_symbol[symbol].get(skip_reason, 0) + 1
                continue
            if not pos_dict:
                continue

            pos = PaperPosition.from_dict(pos_dict)
            entry_price_open = float(pos.entry_price)
            sl_price_open = float(pos.sl_price)
            qty_open = float(pos.qty_est or 0.0)
            risk_usdt_open = abs(entry_price_open - sl_price_open) * qty_open
            risk_status = "ok"
            if qty_open <= 0 or risk_usdt_open <= 0:
                risk_status = "invalid_risk"
                risk_invalid_count += 1
                risk_usdt_open = 0.0
            entry_ts_ms = _candle_ts_ms({"timestamp_utc": pos.entry_ts})
            c5_after = [c for c in c5 if _candle_ts_ms(c) > entry_ts_ms]
            replay_strict = getattr(_config, "REPLAY_EXIT_MODE", "hard") == "hard"
            for c in c5_after:
                updated, closed, pnl, reason, partial_trade = update_and_maybe_close(
                    pos, c, fees_bps, timeout_bars, replay_strict_exit=replay_strict
                )
                pos = updated
                if partial_trade and not replay_strict:
                    partial_trade["risk_status"] = (
                        "invalid_risk"
                        if float(partial_trade.get("risk_usdt", 0) or 0) <= 0
                        else "ok"
                    )
                    partial_trade["exit_price"] = partial_trade.get("tp_price")
                    partial_trade["qty"] = partial_trade.get("qty_est")
                    if partial_trade["risk_status"] == "invalid_risk":
                        partial_trade["risk_usdt"] = None
                        partial_trade["r_multiple"] = None
                        risk_invalid_count += 1
                    closed_trades.append(partial_trade)
                if closed:
                    if reason == "SL":
                        exit_price = float(pos.sl_price)
                    elif reason == "TP":
                        exit_price = float(pos.tp_price)
                    else:
                        exit_price = float(c.get("close", 0) or 0)
                    closed_trades.append(_build_closed_trade(
                        pos,
                        c.get("timestamp_utc", ""),
                        pnl,
                        reason,
                        exit_price=exit_price,
                        entry_price=entry_price_open,
                        sl_price=sl_price_open,
                        qty=qty_open,
                        risk_usdt=risk_usdt_open if risk_status == "ok" else None,
                        risk_status=risk_status,
                    ))
                    break
            else:
                if c5_after:
                    last_c = c5_after[-1]
                    close_price = float(last_c.get("close", 0) or 0)
                    side = pos.side.upper()
                    gross = (close_price - pos.entry_price) * pos.qty_est if side == "LONG" else (pos.entry_price - close_price) * pos.qty_est
                    fees = pos.notional_usdt * (fees_bps / 10000.0) * 2.0
                    pnl = gross - fees
                    closed_trades.append(_build_closed_trade(
                        pos,
                        last_c.get("timestamp_utc", ""),
                        pnl,
                        "END_OF_DATA",
                        exit_price=close_price,
                        entry_price=entry_price_open,
                        sl_price=sl_price_open,
                        qty=qty_open,
                        risk_usdt=risk_usdt_open if risk_status == "ok" else None,
                        risk_status=risk_status,
                    ))

    walk_loop_s = time.perf_counter() - t_walk_start
    t_export_start = time.perf_counter()
    kpi_input = [t for t in closed_trades if str(t.get("risk_status", "ok")) != "invalid_risk"]
    kpi_result = compute_paper_kpis(kpi_input, paper_equity_usdt=paper_equity)
    kpi = kpi_result.get("kpi") or {}
    by_setup = kpi_result.get("kpi_by_setup") or {}

    if not silent:
        trades_total = kpi.get("trades_total", 0)
        trades_per_day = trades_total / max(days, 1)
        trades_per_symbol_per_day = trades_total / max(len(symbols) * days, 1)
        _log.info("")
        _log.info("=== REPLAY KPI SUMMARY ===")
        _log.info("trades_total=%d wins=%d losses=%d", trades_total, kpi.get("wins", 0), kpi.get("losses", 0))
        _log.info("trades_per_day=%.2f trades_per_symbol_per_day=%.2f", trades_per_day, trades_per_symbol_per_day)
        _log.info("winrate=%.2f%% expectancy_R=%.4f profit_factor=%.2f", (kpi.get("winrate", 0) or 0) * 100, kpi.get("expectancy_R", 0), kpi.get("profit_factor", 0))
        _log.info("avg_win_R=%.4f avg_loss_R=%.4f max_dd_usdt=%.2f", kpi.get("avg_win_R", 0), kpi.get("avg_loss_R", 0), kpi.get("max_dd_usdt", 0))
        _log.info("")
        _log.info("By setup:")
        for setup, data in by_setup.items():
            _log.info(
                "  %s: trades=%d winrate=%.2f%% exp_R=%.4f avg_win_R=%.4f avg_loss_R=%.4f",
                setup, data.get("trades_total", 0), (data.get("winrate", 0) or 0) * 100,
                data.get("expectancy_R", 0), data.get("avg_win_R", 0), data.get("avg_loss_R", 0),
            )
        _log.info("")
        _log.info("=== SKIP_REASON COUNTS (why no trade) ===")
        _log.info("%-28s %8s", "reason", "total")
        _log.info("-" * 38)
        for r in skip_reasons:
            _log.info("%-28s %8d", r, skip_total[r])
        _log.info("-" * 38)
        _log.info("%-28s %8d", "TOTAL", sum(skip_total.values()))
        if len(symbols) > 1:
            _log.info("")
            _log.info("Per symbol:")
            for sym in symbols:
                row = [f"{r}:{skip_by_symbol[sym][r]}" for r in skip_reasons if skip_by_symbol[sym][r] > 0]
                if row:
                    _log.info("  %s: %s", sym, " | ".join(row))
        hits_total = sum(cache_hits.values())
        misses_total = sum(cache_misses.values())
        _log.info("")
        _log.info("CACHE_HITS=%d CACHE_MISSES=%d", hits_total, misses_total)
        _log.info("V3_TRIGGERS_TOTAL=%d", v3_triggers_total)

    data_dir = Path("./data")
    data_dir.mkdir(parents=True, exist_ok=True)

    csv_columns = [
        "ts_entry", "ts_exit", "symbol", "setup", "side", "entry", "sl", "tp", "exit_price",
        "qty", "pnl_usdt", "risk_usdt", "r_multiple", "risk_status", "exit_reason", "partial",
    ]

    def _trade_row(t: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ts_entry": t.get("entry_ts", ""),
            "ts_exit": t.get("close_ts", ""),
            "symbol": t.get("symbol", ""),
            "setup": t.get("setup", ""),
            "side": t.get("side", ""),
            "entry": t.get("entry_price"),
            "sl": t.get("sl_price"),
            "tp": t.get("tp_price"),
            "exit_price": t.get("exit_price"),
            "qty": t.get("qty", t.get("qty_est")),
            "pnl_usdt": t.get("pnl_usdt"),
            "risk_usdt": t.get("risk_usdt"),
            "r_multiple": t.get("r_multiple"),
            "risk_status": t.get("risk_status", "ok"),
            "exit_reason": t.get("close_reason", ""),
            "partial": t.get("partial", 0),
        }

    csv_path = data_dir / f"replay_trades_{run_id}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_columns)
        w.writeheader()
        w.writerows(_trade_row(t) for t in closed_trades)
    if not silent:
        _log.info("Exported CSV: %s", csv_path)

    exit_reason_counts: Dict[str, int] = {}
    replay_strict = getattr(_config, "REPLAY_EXIT_MODE", "hard") == "hard"
    allowed_reasons = frozenset(("SL", "TP", "END_OF_DATA")) if replay_strict else None
    for t in closed_trades:
        reason = str(t.get("close_reason", "") or "").strip() or "unknown"
        if allowed_reasons is not None and reason not in allowed_reasons:
            continue
        exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

    # Ensure diagnostic sections used by walk-forward are always present (even if empty)
    by_setup_out = dict(by_setup or {})
    if getattr(_config, "V3_TREND_BREAKOUT", False) and "V3_TREND_BREAKOUT" not in by_setup_out:
        by_setup_out["V3_TREND_BREAKOUT"] = {
            "trades_total": 0,
            "wins": 0,
            "losses": 0,
            "winrate": 0.0,
            "expectancy_R": 0.0,
            "profit_factor": 0.0,
            "avg_win_R": 0.0,
            "avg_loss_R": 0.0,
            "max_dd_usdt": 0.0,
        }
    summary_payload = {
        "run_id": run_id,
        "overall": kpi or {},
        "by_setup": by_setup_out,
        "exit_reason_counts": exit_reason_counts or {},
        "skip_reasons": dict(skip_total) if skip_total else {},
        "risk_invalid_count": int(risk_invalid_count),
    }
    summary_path = data_dir / f"replay_summary_{run_id}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, indent=2, ensure_ascii=True)
    if not silent:
        _log.info("Exported summary: %s", summary_path)

    export_s = time.perf_counter() - t_export_start
    total_s = time.perf_counter() - t_total_start
    _log.info(
        "TIMING total_s=%.2f load_candles_s=%.2f precompute_indicators_s=%.2f walk_loop_s=%.2f export_s=%.2f",
        total_s, load_candles_s, compute_indicators_s, walk_loop_s, export_s,
    )
    _log.info("RISK_INVALID_COUNT=%d", risk_invalid_count)

    return {"closed_trades": closed_trades, "kpi": kpi, "kpi_by_setup": by_setup, "run_id": run_id}


def main() -> int:
    parser = argparse.ArgumentParser(description="REPLAY: simulate paper trades over historical data. DRY RUN only.")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--days", type=int, default=7, help="Days of history")
    parser.add_argument("--tf-trigger", type=int, default=15, help="Trigger TF (minutes)")
    parser.add_argument("--tf-timing", type=int, default=5, help="Timing TF for fill/SL/TP (minutes)")
    parser.add_argument("--tf-bias", type=int, default=240, help="Bias TF (minutes)")
    parser.add_argument("--tf-setup", type=int, default=60, help="Setup TF (minutes)")
    parser.add_argument("--step-bars", type=int, default=1, help="Step every N trigger bars")
    parser.add_argument("--no-cache", action="store_true", help="Disable candle cache (fetch from API)")
    parser.add_argument("--cache-only", action="store_true", help="Use only cache; fail if range not fully covered (no API calls)")
    parser.add_argument("--cache-days", type=int, default=365, help="Max days to keep in cache (default: 365)")
    parser.add_argument("--end-days-ago", type=int, default=None, help="End date = now - N days (for walk-forward)")
    parser.add_argument("--start-days-ago", type=int, default=None, help="Start date = now - N days (use with --end-days-ago)")
    args = parser.parse_args()

    use_cache = not args.no_cache
    cache_only = getattr(args, "cache_only", False)
    if cache_only:
        use_cache = True

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        _log.error("No symbols provided")
        return 2
    for sym in symbols:
        if any(c.isspace() for c in sym):
            raise ValueError(f"Symbol contains whitespace: {repr(sym)}")
    _log.info("NORMALIZED_SYMBOLS=%s count=%d", symbols, len(symbols))

    days_arg = args.days
    if args.start_days_ago is not None and args.end_days_ago is not None:
        days_arg = args.start_days_ago - args.end_days_ago
        if days_arg <= 0:
            _log.error("start_days_ago (%d) must be > end_days_ago (%d)", args.start_days_ago, args.end_days_ago)
            return 2
    run_replay(
        symbols=symbols,
        days=days_arg,
        tf_trigger=args.tf_trigger,
        tf_timing=args.tf_timing,
        tf_bias=args.tf_bias,
        tf_setup=args.tf_setup,
        cache_only=cache_only,
        step_bars=args.step_bars,
        use_cache=use_cache,
        cache_days=args.cache_days,
        end_days_ago=args.end_days_ago,
        start_days_ago=args.start_days_ago,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
