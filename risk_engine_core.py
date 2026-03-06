from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from scalper.models import RiskVerdict, TradeRecord, validate_trade_intent


class RiskEngine:
    def __init__(self, state: Dict[str, Any], settings: Any, store: Any):
        self.settings = settings
        self.store = store
        self.state: Dict[str, Any] = dict(state or {})
        self._ensure_state_keys(int(datetime.now(timezone.utc).timestamp()))

    def evaluate(
        self,
        intent: Dict[str, Any],
        snapshot: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
        now: Optional[datetime] = None,
    ) -> RiskVerdict:
        if state is not None:
            self.state = dict(state or {})
        else:
            self._reload_state()
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_ts = int(now_dt.timestamp())
        self._ensure_state_keys(now_ts)
        self._ensure_daily_rollover(now_ts)

        valid, block_reason = validate_trade_intent(intent)
        if not valid:
            return self._verdict("BLOCK", block_reason, intent, now_ts, details={"block_reason": block_reason})

        sym = str(intent.get("symbol", "") or "").strip().upper()
        snapshot_ok = isinstance(snapshot, dict) and bool(snapshot)
        fail_closed = bool(getattr(self.settings, "fail_closed_on_snapshot_missing", True))
        base_equity = float(getattr(self.settings, "paper_equity_usdt", 200.0) or 200.0)
        equity = float((snapshot or {}).get("equity", base_equity + float(self.state.get("daily_pnl_realized", 0.0) or 0.0)))
        equity = max(1e-9, equity)
        self.state["equity_peak"] = max(float(self.state.get("equity_peak", 0.0) or 0.0), base_equity, equity)

        kill_reason = str(self.state.get("kill_reason", "") or "")
        if kill_reason:
            return self._verdict("BLOCK", "KILLED", intent, now_ts, details={"kill_reason": kill_reason})

        pause_until_ts = int(self.state.get("pause_until_ts", 0) or 0)
        if pause_until_ts > now_ts:
            return self._verdict(
                "PAUSE",
                "PAUSED",
                intent,
                now_ts,
                details={"pause_until_ts": pause_until_ts, "pause_reason": str(self.state.get("pause_reason", ""))},
                emit_event=False,
            )

        if (not snapshot_ok) and fail_closed:
            self.state["pause_until_ts"] = max(int(self.state.get("pause_until_ts", 0) or 0), now_ts + 60)
            self.state["pause_reason"] = "SNAPSHOT_MISSING"
            return self._verdict("PAUSE", "SNAPSHOT_MISSING", intent, now_ts, details={"snapshot_present": False})

        equity_peak = max(1e-9, float(self.state.get("equity_peak", equity) or equity))
        dd_pct = max(0.0, (equity_peak - equity) / equity_peak * 100.0)
        if dd_pct >= float(getattr(self.settings, "max_dd_pct", 12.0) or 12.0):
            self.state["kill_reason"] = "MAX_DD"
            return self._verdict(
                "KILL",
                "MAX_DD",
                intent,
                now_ts,
                details={"equity": equity, "equity_peak": equity_peak, "dd_pct": dd_pct},
            )

        daily_pnl_realized = float(self.state.get("daily_pnl_realized", 0.0) or 0.0)
        daily_loss_limit = float(getattr(self.settings, "daily_loss_limit_pct", 1.0) or 1.0) / 100.0 * equity_peak
        if daily_pnl_realized <= -daily_loss_limit:
            self.state["pause_until_ts"] = self._end_of_utc_day_ts(now_dt)
            self.state["pause_reason"] = "DAILY_STOP"
            return self._verdict(
                "PAUSE",
                "DAILY_STOP",
                intent,
                now_ts,
                details={"daily_pnl_realized": daily_pnl_realized, "daily_loss_limit": daily_loss_limit},
            )

        open_positions = self._open_positions(snapshot)
        open_positions_count = int((snapshot or {}).get("open_positions_count", len(open_positions)) or len(open_positions))
        max_positions = int(getattr(self.settings, "max_concurrent_positions", 2) or 2)
        if max_positions > 0 and open_positions_count >= max_positions:
            return self._verdict(
                "BLOCK",
                "MAX_POSITIONS",
                intent,
                now_ts,
                details={"open_positions_count": open_positions_count, "max_concurrent_positions": max_positions},
                emit_event=False,
            )

        symbol_notional = self._symbol_notional(snapshot, sym, open_positions)
        max_symbol_notional = float(getattr(self.settings, "max_symbol_notional_pct", 30.0) or 30.0) / 100.0 * equity
        if symbol_notional > max_symbol_notional:
            return self._verdict(
                "BLOCK",
                "SYMBOL_EXPOSURE",
                intent,
                now_ts,
                details={"symbol_notional": symbol_notional, "max_symbol_notional": max_symbol_notional},
                emit_event=False,
            )

        cluster_limit = int(getattr(self.settings, "cluster_btc_eth_limit", 1) or 1)
        if cluster_limit > 0 and sym in {"BTCUSDT", "ETHUSDT"}:
            other = "ETHUSDT" if sym == "BTCUSDT" else "BTCUSDT"
            open_symbols = {str(p.get("symbol", "") or "").strip().upper() for p in open_positions if isinstance(p, dict)}
            if other in open_symbols:
                return self._verdict(
                    "BLOCK",
                    "CLUSTER_LIMIT",
                    intent,
                    now_ts,
                    details={"symbol": sym, "other_open": other},
                    emit_event=False,
                )

        max_trades_day = int(getattr(self.settings, "max_trades_day", 12) or 12)
        trade_count_today = int(self.state.get("trade_count_today", 0) or 0)
        if max_trades_day > 0 and trade_count_today >= max_trades_day:
            self.state["pause_until_ts"] = self._end_of_utc_day_ts(now_dt)
            self.state["pause_reason"] = "TRADE_LIMIT_DAY"
            return self._verdict(
                "PAUSE",
                "TRADE_LIMIT_DAY",
                intent,
                now_ts,
                details={"trade_count_today": trade_count_today, "max_trades_day": max_trades_day},
            )

        min_between_global = int(getattr(self.settings, "min_seconds_between_trades", 180) or 180)
        last_trade_ts = int(self.state.get("last_trade_ts", 0) or 0)
        if min_between_global > 0 and last_trade_ts > 0 and (now_ts - last_trade_ts) < min_between_global:
            return self._verdict(
                "DEFER",
                "GLOBAL_COOLDOWN",
                intent,
                now_ts,
                details={"wait_seconds": min_between_global - (now_ts - last_trade_ts)},
                emit_event=False,
            )

        min_between_symbol = int(getattr(self.settings, "min_seconds_between_symbol_trades", 900) or 900)
        last_trade_symbol_ts = dict(self.state.get("last_trade_symbol_ts", {}) or {})
        last_symbol_ts = int(last_trade_symbol_ts.get(sym, 0) or 0)
        if min_between_symbol > 0 and last_symbol_ts > 0 and (now_ts - last_symbol_ts) < min_between_symbol:
            return self._verdict(
                "DEFER",
                "SYMBOL_COOLDOWN",
                intent,
                now_ts,
                details={"wait_seconds": min_between_symbol - (now_ts - last_symbol_ts), "symbol": sym},
                emit_event=False,
            )

        cooldown_until_ts = int(self.state.get("cooldown_until_ts", 0) or 0)
        if cooldown_until_ts > now_ts:
            return self._verdict(
                "DEFER",
                "COOLDOWN",
                intent,
                now_ts,
                details={"cooldown_until_ts": cooldown_until_ts},
                emit_event=False,
            )

        self.state["trade_count_today"] = trade_count_today + 1
        self.state["last_trade_ts"] = now_ts
        last_trade_symbol_ts[sym] = now_ts
        self.state["last_trade_symbol_ts"] = last_trade_symbol_ts
        return self._verdict("ALLOW", "OK", intent, now_ts, details={"equity": equity}, emit_event=False)

    def assess(self, intent: Dict[str, Any]) -> RiskVerdict:
        return self.evaluate(intent=intent, snapshot=None, state=None, now=None)

    def on_fill(self, trade_record: TradeRecord) -> None:
        self._reload_state()
        now_dt = datetime.now(timezone.utc)
        now_ts = int(now_dt.timestamp())
        self._ensure_state_keys(now_ts)
        self._ensure_daily_rollover(now_ts)

        pnl = float(getattr(trade_record, "pnl_usdt", 0.0) or 0.0)
        self.state["daily_pnl_realized"] = float(self.state.get("daily_pnl_realized", 0.0) or 0.0) + pnl
        self.state["daily_pnl_sim"] = float(self.state.get("daily_pnl_sim", 0.0) or 0.0) + pnl
        if pnl < 0:
            self.state["consecutive_losses"] = int(self.state.get("consecutive_losses", 0) or 0) + 1
            cooldown_min = int(getattr(self.settings, "risk_cooldown_minutes", 0) or 0)
            if cooldown_min > 0:
                self.state["cooldown_until_ts"] = now_ts + cooldown_min * 60
                self.state["cooldown_until_utc"] = datetime.fromtimestamp(
                    self.state["cooldown_until_ts"], tz=timezone.utc
                ).isoformat()
        else:
            self.state["consecutive_losses"] = 0
            self.state["cooldown_until_ts"] = 0
            self.state["cooldown_until_utc"] = ""

        base_equity = float(getattr(self.settings, "paper_equity_usdt", 200.0) or 200.0)
        eq_now = base_equity + float(self.state.get("daily_pnl_realized", 0.0) or 0.0)
        self.state["equity_peak"] = max(float(self.state.get("equity_peak", 0.0) or 0.0), base_equity, eq_now)
        self._persist_state()
        self.store.store_risk_event(
            {
                "ts": str(trade_record.close_ts or trade_record.entry_ts or now_dt.isoformat()),
                "event_type": "FILL",
                "type": "LOSS" if pnl < 0 else "WIN",
                "status": "TRIGGERED",
                "reason_code": str(trade_record.close_reason or ""),
                "reason": str(trade_record.close_reason or ""),
                "symbol": str(trade_record.symbol or ""),
                "setup": str(trade_record.setup or ""),
                "direction": str(trade_record.side or ""),
                "pnl_usdt": pnl,
                "details_json": "{}",
            }
        )

    def _verdict(
        self,
        verdict: str,
        reason_code: str,
        intent: Dict[str, Any],
        now_ts: int,
        *,
        details: Optional[Dict[str, Any]] = None,
        emit_event: bool = True,
    ) -> RiskVerdict:
        details = dict(details or {})
        intent_id = str(intent.get("id") or intent.get("intent_id") or "")
        payload = {
            "id": intent_id or f"risk:{now_ts}:{str(intent.get('symbol', '')).upper()}:{reason_code}",
            "ts": int(now_ts),
            "symbol": str(intent.get("symbol", "") or "").upper(),
            "side": str(intent.get("side", intent.get("direction", "")) or "").upper(),
            "setup": str(intent.get("setup", intent.get("strategy", "")) or ""),
            "strategy_id": str(intent.get("strategy_id", intent.get("strategy", intent.get("setup", ""))) or ""),
            "verdict": verdict,
            "risk_verdict": verdict,
            "reason_code": reason_code,
            "block_reason": reason_code if verdict != "ALLOW" else "",
            "status": "OPEN" if verdict == "ALLOW" else "BLOCK",
            "details_json": "{}",
            "details": details,
        }
        self.store.store_trade_intent(payload)
        if emit_event and verdict in {"PAUSE", "KILL"}:
            self.store.store_risk_event(
                {
                    "ts": int(now_ts),
                    "event_type": verdict,
                    "type": verdict,
                    "status": "TRIGGERED",
                    "reason_code": reason_code,
                    "reason": reason_code,
                    "symbol": payload["symbol"],
                    "setup": payload["setup"],
                    "direction": payload["side"],
                    "details_json": "{}",
                    "details": details,
                }
            )
        self._persist_state()
        return RiskVerdict(allowed=(verdict == "ALLOW"), reason=reason_code, meta={"verdict": verdict, **details})

    def _open_positions(self, snapshot: Optional[Dict[str, Any]]) -> list[dict]:
        src = (snapshot or {}).get("open_positions")
        if isinstance(src, list):
            return [p for p in src if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN"]
        st = self.state.get("open_positions")
        if isinstance(st, list):
            return [p for p in st if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN"]
        return []

    def _symbol_notional(self, snapshot: Optional[Dict[str, Any]], symbol: str, open_positions: list[dict]) -> float:
        sym_notional = (snapshot or {}).get("symbol_notional", {})
        if isinstance(sym_notional, dict):
            try:
                return float(sym_notional.get(symbol, 0.0) or 0.0)
            except (TypeError, ValueError):
                return 0.0
        total = 0.0
        for p in open_positions:
            if str(p.get("symbol", "")).upper() != symbol:
                continue
            entry = float(p.get("entry_price", p.get("entry", 0.0)) or 0.0)
            qty = float(p.get("qty_est", p.get("qty", 0.0)) or 0.0)
            notional = float(p.get("notional_usdt", 0.0) or 0.0)
            total += notional if notional > 0 else abs(entry * qty)
        return total

    def _reload_state(self) -> None:
        self.state = dict(self.store.load_paper_state() or {})

    def _persist_state(self) -> None:
        self.store.save_paper_state(self.state)

    def _ensure_state_keys(self, now_ts: int) -> None:
        self.state.setdefault("day_utc", datetime.fromtimestamp(now_ts, tz=timezone.utc).date().isoformat())
        self.state.setdefault("trade_count_today", 0)
        self.state.setdefault("daily_pnl_realized", 0.0)
        self.state.setdefault("daily_pnl_sim", float(self.state.get("daily_pnl_realized", 0.0) or 0.0))
        self.state.setdefault("equity_peak", float(getattr(self.settings, "paper_equity_usdt", 200.0) or 200.0))
        self.state.setdefault("consecutive_losses", 0)
        if "cooldown_until_ts" not in self.state:
            cooldown_utc = str(self.state.get("cooldown_until_utc", "") or "").strip()
            cooldown_ts = 0
            if cooldown_utc:
                try:
                    dt = datetime.fromisoformat(cooldown_utc.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    cooldown_ts = int(dt.timestamp())
                except ValueError:
                    cooldown_ts = 0
            self.state["cooldown_until_ts"] = cooldown_ts
        self.state.setdefault("cooldown_until_utc", "")
        self.state.setdefault("pause_until_ts", 0)
        self.state.setdefault("pause_reason", "")
        self.state.setdefault("kill_reason", "")
        self.state.setdefault("last_trade_ts", 0)
        self.state.setdefault("last_trade_symbol_ts", {})

    def _ensure_daily_rollover(self, now_ts: int) -> None:
        today = datetime.fromtimestamp(now_ts, tz=timezone.utc).date().isoformat()
        if str(self.state.get("day_utc", "")) == today:
            return
        self.state["day_utc"] = today
        self.state["trade_count_today"] = 0
        self.state["daily_pnl_realized"] = 0.0
        self.state["daily_pnl_sim"] = 0.0
        self.state["consecutive_losses"] = 0
        self.state["cooldown_until_ts"] = 0
        self.state["cooldown_until_utc"] = ""
        self.state["pause_until_ts"] = 0
        self.state["pause_reason"] = ""

    def _end_of_utc_day_ts(self, now_dt: datetime) -> int:
        next_day = datetime(now_dt.year, now_dt.month, now_dt.day, tzinfo=timezone.utc) + timedelta(days=1)
        return int(next_day.timestamp())

