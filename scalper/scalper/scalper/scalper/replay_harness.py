"""
Deterministic replay harness: StrategyEngine -> RiskEngine -> PaperBroker.
Replays historical candles bar-by-bar. No live trading. Same inputs => same outputs.
"""
from __future__ import annotations

import csv
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

RUNS_DIR = Path("runs")
DATA_CACHE_DIR = Path("data") / "cache"


def _ensure_dirs() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(symbol: str, interval: int, start_str: str, end_str: str) -> Path:
    safe = f"{symbol}_{interval}_{start_str}_{end_str}".replace(" ", "_")
    return DATA_CACHE_DIR / f"{safe}.json"


def load_candles(
    symbol: str,
    interval: int,
    start_str: str,
    end_str: str,
    *,
    fetch_fn: Optional[Any] = None,
    use_cache: bool = True,
) -> List[Dict[str, Any]]:
    """
    Load candles from cache or fetch. Cache: ./data/cache/<symbol>_<interval>_<start>_<end>.json
    """
    _ensure_dirs()
    path = _cache_path(symbol, interval, start_str, end_str)

    if use_cache and path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            candles = raw if isinstance(raw, list) else raw.get("candles", [])
            _log.info("Cache hit: %s (%d bars)", path.name, len(candles))
            return candles
        except (json.JSONDecodeError, OSError) as e:
            _log.warning("Cache read failed %s: %s", path, e)

    if fetch_fn is None:
        from bybit import fetch_klines

        def _fetch(sym: str, tf: int, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
            all_c: List[Dict[str, Any]] = []
            current_end = end_ms
            bar_ms = tf * 60 * 1000
            start_aligned = (start_ms // bar_ms) * bar_ms
            while current_end >= start_aligned:
                time.sleep(0.3)
                chunk = fetch_klines(
                    symbol=sym,
                    interval=str(tf),
                    limit=1000,
                    start_ms=start_aligned,
                    end_ms=current_end,
                )
                if not chunk:
                    break
                for c in chunk:
                    ts = int(c.get("timestamp", 0) or 0)
                    if start_aligned <= ts <= end_ms:
                        all_c.append(c)
                if len(chunk) < 1000:
                    break
                oldest = min(int(x.get("timestamp", 0) or 0) for x in chunk)
                current_end = oldest - 1
            by_ts = {c["timestamp"]: c for c in all_c}
            return sorted(by_ts.values(), key=lambda x: x["timestamp"])

        fetch_fn = _fetch

    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Got start={start_str!r} end={end_str!r}")

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000) + 86400 * 1000 - 1

    candles = fetch_fn(symbol, interval, start_ms, end_ms)
    if use_cache and candles:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(candles, f, separators=(",", ":"))
        _log.info("Cached %d bars to %s", len(candles), path.name)

    return candles


class ReplayStore:
    """In-memory store for replay. No sqlite."""

    def __init__(self, seed: int = 0) -> None:
        self._state: Dict[str, Any] = {
            "open_positions": [],
            "closed_trades": [],
            "daily_pnl_realized": 0.0,
            "equity_peak": 0.0,
            "trade_count_today": 0,
            "consecutive_losses": 0,
            "cooldown_until_utc": "",
            "day_utc": "",
        }
        self._seed = seed

    def load_paper_state(self) -> Dict[str, Any]:
        return dict(self._state)

    def save_paper_state(self, state: Dict[str, Any]) -> None:
        self._state = dict(state)

    def upsert_paper_position(self, position: Dict[str, Any]) -> None:
        pos_id = str(position.get("intent_id", "") or "").strip()
        existing = [p for p in self._state["open_positions"] if str(p.get("intent_id", "")) != pos_id]
        existing.append(position)
        self._state["open_positions"] = existing

    def delete_paper_position(self, position_id: str) -> None:
        pid = str(position_id or "").strip()
        self._state["open_positions"] = [
            p for p in self._state["open_positions"]
            if str(p.get("intent_id", "")) != pid
        ]

    def insert_paper_trade(self, trade: Dict[str, Any]) -> None:
        self._state["closed_trades"] = self._state.get("closed_trades", []) + [trade]

    def store_risk_event(self, event: Dict[str, Any]) -> None:
        pass

    def store_trade_intent(self, intent: Dict[str, Any]) -> None:
        pass


def run_replay(
    symbols: List[str],
    start_str: str,
    end_str: str,
    *,
    interval: int = 15,
    fees_bps: float = 6.0,
    slippage_bps: float = 2.0,
    spread_bps: float = 1.0,
    seed: int = 123,
    out_tag: str = "replay",
    use_cache: bool = True,
    fetch_fn: Optional[Any] = None,
    emit_events: bool = True,
) -> Dict[str, Any]:
    """
    Run deterministic replay. Outputs to ./runs/.
    Returns summary dict with pnl, winrate, maxDD, PF, avgR, trades_per_day.
    """
    random.seed(seed)
    _ensure_dirs()

    from paper import PaperPosition, update_and_maybe_close
    from scalper.risk_engine_core import RiskEngine
    from scalper.paper_broker import PaperBroker
    from scalper.settings import get_settings
    from scalper.models import TradeRecord
    from signals import evaluate_symbol_intents

    settings = get_settings()
    risk_settings = settings.risk
    paper_equity = float(getattr(risk_settings, "paper_equity_usdt", 200.0) or 200.0)
    timeout_bars = int(getattr(risk_settings, "paper_timeout_bars", 12) or 12)

    store = ReplayStore(seed=seed)
    store._state["equity_peak"] = paper_equity
    store._state["day_utc"] = start_str

    risk_engine = RiskEngine(store.load_paper_state(), risk_settings, store)
    paper_broker = PaperBroker(
        store=SimpleNamespace(
            load_paper_state=store.load_paper_state,
            upsert_paper_position=store.upsert_paper_position,
            insert_paper_trade=store.insert_paper_trade,
            delete_paper_position=store.delete_paper_position,
        ),
        risk_settings=SimpleNamespace(
            paper_slippage_pct=slippage_bps / 10000.0,
            paper_fee_pct=fees_bps / 10000.0,
            paper_start_equity_usdt=paper_equity,
            paper_equity_usdt=paper_equity,
            paper_sl_atr=getattr(risk_settings, "paper_sl_atr", 1.0),
            paper_tp_atr=getattr(risk_settings, "paper_tp_atr", 1.5),
            risk_per_trade_pct=getattr(risk_settings, "risk_per_trade_pct", 0.15),
        ),
    )

    candles_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for sym in symbols:
        candles_by_symbol[sym] = load_candles(
            sym, interval, start_str, end_str,
            fetch_fn=fetch_fn, use_cache=use_cache,
        )

    all_bars: List[Tuple[str, int, Dict[str, Any]]] = []
    for sym, candles in candles_by_symbol.items():
        for i, c in enumerate(candles):
            all_bars.append((sym, i, c))
    all_bars.sort(key=lambda x: (x[2]["timestamp"], x[0]))

    equity_curve: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    closed_trades: List[Dict[str, Any]] = []

    equity = paper_equity
    peak = paper_equity
    last_bar_ts: Optional[int] = None
    bars_this_day = 0
    day_utc = ""

    for sym, bar_idx, candle in all_bars:
        ts = int(candle.get("timestamp", 0) or 0)
        ts_utc = str(candle.get("timestamp_utc", "") or "")
        if ts_utc:
            day_utc = ts_utc[:10] if len(ts_utc) >= 10 else day_utc

        if last_bar_ts is not None and ts != last_bar_ts:
            bars_this_day = 0
        last_bar_ts = ts
        bars_this_day += 1

        sym_candles = candles_by_symbol[sym]
        window = sym_candles[: bar_idx + 1]
        if len(window) < 80:
            continue

        evaluated = evaluate_symbol_intents(
            symbol=sym,
            candles=window,
            signal_debug=False,
            threshold_profile="A",
        )
        intents = list(evaluated.get("final_intents", []) or [])

        state = store.load_paper_state()
        open_positions = list(state.get("open_positions", []) or [])

        for pos_raw in open_positions[:]:
            try:
                pos = PaperPosition.from_dict(pos_raw)
            except Exception:
                continue
            if pos.symbol != sym:
                continue
            updated, closed, pnl, reason, partial = update_and_maybe_close(
                position=pos,
                last_candle=candle,
                fees_bps=fees_bps,
                timeout_bars=timeout_bars,
                replay_strict_exit=True,
            )
            if partial:
                closed_trades.append(partial)
                store.insert_paper_trade(partial)
                risk_engine.on_fill(
                    TradeRecord(
                        symbol=partial.get("symbol", ""),
                        side=partial.get("side", ""),
                        setup=partial.get("strategy", ""),
                        entry_ts=partial.get("entry_ts", ""),
                        close_ts=partial.get("close_ts", ""),
                        pnl_usdt=float(partial.get("pnl_usdt", 0)),
                        close_reason=partial.get("close_reason", ""),
                    )
                )
            if closed:
                close_evt = {
                    "symbol": updated.symbol,
                    "side": updated.side,
                    "strategy": updated.strategy,
                    "pnl_usdt": pnl,
                    "close_reason": reason,
                    "entry_ts": updated.entry_ts,
                    "close_ts": ts_utc,
                    "entry_price": updated.entry_price,
                    "sl_price": updated.sl_price,
                    "qty_est": updated.qty_est,
                }
                closed_trades.append(close_evt)
                store.insert_paper_trade(close_evt)
                store.delete_paper_position(updated.intent_id)
                risk_engine.on_fill(
                    TradeRecord(
                        symbol=updated.symbol,
                        side=updated.side,
                        setup=updated.strategy,
                        entry_ts=updated.entry_ts,
                        close_ts=ts_utc,
                        pnl_usdt=pnl,
                        close_reason=reason,
                    )
                )
                equity += pnl
                peak = max(peak, equity)
                if emit_events:
                    events.append({"ts": ts_utc, "type": "close", **close_evt})

        for intent in intents:
            symbol_i = str(intent.get("symbol", sym))
            side = str(intent.get("side", intent.get("direction", ""))).upper()
            strategy = str(intent.get("strategy", intent.get("setup", "")))
            bar_ts = str(intent.get("ts", intent.get("bar_ts_used", ts_utc)))

            snapshot = {
                "equity": equity,
                "open_positions": store.load_paper_state().get("open_positions", []),
                "open_positions_count": len(store.load_paper_state().get("open_positions", [])),
            }
            try:
                bar_dt = datetime.fromisoformat(bar_ts.replace("Z", "+00:00"))
            except ValueError:
                bar_dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).replace(tzinfo=timezone.utc)

            verdict = risk_engine.evaluate(
                {
                    "symbol": symbol_i,
                    "side": side,
                    "setup": strategy,
                    "strategy": strategy,
                    "direction": side,
                    "timeframe": str(interval),
                    "candle_ts": bar_ts,
                    "ts": bar_ts,
                },
                snapshot=snapshot,
                now=bar_dt,
            )

            if verdict.allowed:
                snap = evaluated.get("market_snapshot", {}) or {}
                atr_val = float(snap.get("atr14", 0) or 0)
                pos_dict, skip = paper_broker.open_from_intent(
                    intent={
                        **intent,
                        "symbol": symbol_i,
                        "side": side,
                        "strategy": strategy,
                        "meta": intent.get("meta", {}),
                    },
                    candle=candle,
                    strategy=strategy,
                    fallback_atr=atr_val,
                    intent_id=f"{symbol_i}|{strategy}|{side}|{bar_ts}",
                    ts=bar_ts,
                )
                if pos_dict:
                    store.upsert_paper_position(pos_dict)
                    if emit_events:
                        events.append({
                            "ts": bar_ts,
                            "type": "open",
                            "symbol": symbol_i,
                            "side": side,
                            "strategy": strategy,
                        })

        equity_curve.append({
            "ts": ts_utc,
            "equity": equity,
            "peak": peak,
        })

    state = store.load_paper_state()
    closed = list(state.get("closed_trades", []) or [])

    pnls = [float(t.get("pnl_usdt", 0) or 0) for t in closed]
    total_pnl = sum(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    winrate = len(wins) / len(closed) * 100.0 if closed else 0.0
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    risk_per_trade = []
    for t in closed:
        entry = float(t.get("entry_price", 0) or 0)
        sl = float(t.get("sl_price", 0) or 0)
        qty = float(t.get("qty_est", 0) or 0)
        if entry and sl and qty:
            risk_usdt = abs(entry - sl) * qty
            pnl = float(t.get("pnl_usdt", 0) or 0)
            if risk_usdt > 0:
                risk_per_trade.append(pnl / risk_usdt)
    avg_r = sum(risk_per_trade) / len(risk_per_trade) if risk_per_trade else 0.0

    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_str, "%Y-%m-%d")
        days = max(1, (end_dt - start_dt).days)
    except ValueError:
        days = 1
    trades_per_day = len(closed) / days if days > 0 else 0.0

    dd_pct = 0.0
    peak_eq = paper_equity
    for pt in equity_curve:
        eq = float(pt.get("equity", 0) or 0)
        peak_eq = max(peak_eq, eq)
        if peak_eq > 0:
            dd_pct = max(dd_pct, (peak_eq - eq) / peak_eq * 100.0)

    summary = {
        "pnl": total_pnl,
        "winrate": round(winrate, 2),
        "maxDD": round(dd_pct, 2),
        "PF": round(pf, 2) if isinstance(pf, (int, float)) else pf,
        "avgR": round(avg_r, 4),
        "trades_per_day": round(trades_per_day, 2),
        "trades": len(closed),
        "days": days,
        "seed": seed,
    }

    tag = out_tag or "replay"
    runs = RUNS_DIR

    with open(runs / f"equity_curve_{tag}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "equity", "peak"])
        w.writeheader()
        w.writerows(equity_curve)

    with open(runs / f"trades_{tag}.csv", "w", encoding="utf-8", newline="") as f:
        cols = ["symbol", "side", "strategy", "pnl_usdt", "close_reason", "entry_ts", "close_ts"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for t in closed:
            row = {k: t.get(k, "") for k in cols}
            w.writerow(row)

    with open(runs / f"summary_{tag}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    if emit_events:
        with open(runs / f"events_{tag}.jsonl", "w", encoding="utf-8") as f:
            for evt in events:
                f.write(json.dumps(evt, ensure_ascii=True) + "\n")

    return summary
