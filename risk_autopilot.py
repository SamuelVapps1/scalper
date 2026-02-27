from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Callable, Optional, Tuple

from storage import load_paper_state, save_paper_state


@dataclass(frozen=True)
class SignalIntent:
    symbol: str
    side: str
    strategy: str
    reason: str
    confidence: float
    ts: str


class RiskAutopilot:
    def __init__(
        self,
        *,
        kill_switch_on: bool,
        max_trades_per_day: int,
        max_daily_loss_sim: float,
        max_consecutive_losses: int,
        cooldown_minutes: int,
        max_open_positions: int,
        one_position_per_symbol: bool,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.kill_switch_on = bool(kill_switch_on)
        self.max_trades_per_day = max(0, int(max_trades_per_day))
        self.max_daily_loss_sim = max(0.0, float(max_daily_loss_sim))
        self.max_consecutive_losses = max(0, int(max_consecutive_losses))
        self.cooldown_minutes = max(0, int(cooldown_minutes))
        self.max_open_positions = max(0, int(max_open_positions))
        self.one_position_per_symbol = bool(one_position_per_symbol)
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._state = load_paper_state()
        self._active_intent: Optional[SignalIntent] = None
        self._active_now: Optional[datetime] = None

    @classmethod
    def from_config(cls, config=None):
        """
        Build RiskAutopilot from config values.
        If config is None, imports project config module lazily.
        """
        config_module = config
        if config_module is None:
            import config as config_module

        return cls(
            kill_switch_on=getattr(config_module, "RISK_KILL_SWITCH", False),
            max_trades_per_day=getattr(config_module, "RISK_MAX_TRADES_PER_DAY", 10),
            max_daily_loss_sim=getattr(config_module, "RISK_MAX_DAILY_LOSS_SIM", 100.0),
            max_consecutive_losses=getattr(config_module, "RISK_MAX_CONSECUTIVE_LOSSES", 3),
            cooldown_minutes=getattr(config_module, "RISK_COOLDOWN_MINUTES", 30),
            max_open_positions=getattr(config_module, "MAX_OPEN_POSITIONS", 1),
            one_position_per_symbol=getattr(
                config_module, "RISK_ONE_POSITION_PER_SYMBOL", True
            ),
        )

    def evaluate(self, intent: SignalIntent) -> Tuple[bool, str]:
        from scalper.risk_engine_core import RiskEngine

        self._reload_state()
        self._active_intent = intent
        self._active_now = self._now_fn()
        engine = RiskEngine(self._state, self._settings_proxy(), __import__("storage"))
        snapshot = {
            "equity": float(getattr(self, "paper_equity_usdt", 0.0) or 0.0)
            + float(self._state.get("daily_pnl_realized", self._state.get("daily_pnl_sim", 0.0)) or 0.0),
            "open_positions": list(self._state.get("open_positions", []) or []),
            "open_positions_count": len(
                [
                    p
                    for p in (self._state.get("open_positions", []) or [])
                    if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN"
                ]
            ),
        }
        verdict = engine.evaluate(
            {
                "symbol": intent.symbol,
                "setup": intent.strategy,
                "direction": intent.side,
                "strategy": intent.strategy,
                "side": intent.side,
                "ts": intent.ts,
            },
            snapshot=snapshot,
        )
        return bool(verdict.allowed), str(verdict.reason or "")

    def is_kill_switch_on(self) -> Tuple[bool, str]:
        if self.kill_switch_on:
            return False, "blocked: risk kill switch is ON"
        return True, "ok"

    def check_daily_trade_limit(self) -> Tuple[bool, str]:
        trade_count_today = int(self._state.get("trade_count_today", 0))
        if self.max_trades_per_day > 0 and trade_count_today >= self.max_trades_per_day:
            return (
                False,
                f"blocked: daily trade limit reached ({trade_count_today}/{self.max_trades_per_day})",
            )
        return True, "ok"

    def check_daily_loss_limit(self) -> Tuple[bool, str]:
        daily_pnl_sim = float(self._state.get("daily_pnl_sim", 0.0))
        if self.max_daily_loss_sim > 0 and daily_pnl_sim <= -self.max_daily_loss_sim:
            return (
                False,
                "blocked: daily simulated loss limit reached "
                f"({daily_pnl_sim:.4f} <= -{self.max_daily_loss_sim:.4f})",
            )
        return True, "ok"

    def check_consecutive_losses_limit(self) -> Tuple[bool, str]:
        consecutive_losses = int(self._state.get("consecutive_losses", 0))
        if (
            self.max_consecutive_losses > 0
            and consecutive_losses >= self.max_consecutive_losses
        ):
            return (
                False,
                "blocked: consecutive loss limit reached "
                f"({consecutive_losses}/{self.max_consecutive_losses})",
            )
        return True, "ok"

    def check_cooldown(self) -> Tuple[bool, str]:
        cooldown_until_raw = str(self._state.get("cooldown_until_utc", "") or "").strip()
        if not cooldown_until_raw:
            return True, "ok"
        try:
            cooldown_until = datetime.fromisoformat(cooldown_until_raw)
        except ValueError:
            self._state["cooldown_until_utc"] = ""
            return True, "ok"

        now_utc = self._active_now or self._now_fn()
        if cooldown_until.tzinfo is None:
            cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
        if now_utc < cooldown_until:
            return False, f"BLOCK: COOLDOWN_ACTIVE until {cooldown_until.isoformat()}"
        return True, "ok"

    def check_max_open_positions(self) -> Tuple[bool, str]:
        if self.max_open_positions <= 0:
            return True, "ok"
        open_positions = self._state.get("open_positions")
        if not isinstance(open_positions, list):
            open_positions = []
            self._state["open_positions"] = open_positions
        if len(open_positions) >= self.max_open_positions:
            return False, f"MAX_OPEN_POSITIONS_REACHED ({self.max_open_positions})"
        return True, "ok"

    def check_symbol_already_open(self) -> Tuple[bool, str]:
        if not self.one_position_per_symbol:
            return True, "ok"
        intent = self._active_intent
        if intent is None:
            return True, "ok"

        open_positions = self._state.get("open_positions")
        if not isinstance(open_positions, list):
            open_positions = []
            self._state["open_positions"] = open_positions

        intent_symbol = str(intent.symbol or "").upper()
        if not intent_symbol:
            return True, "ok"

        for pos in open_positions:
            if not isinstance(pos, dict):
                continue
            pos_symbol = str(pos.get("symbol", "")).upper()
            if pos_symbol == intent_symbol:
                return False, "SYMBOL_ALREADY_OPEN"
        return True, "ok"

    def record_allowed_intent(self, intent: SignalIntent) -> None:
        # assess(...) already consumes daily trade budget in RiskEngine.
        self._reload_state()

    def record_paper_close(self, pnl_usdt: float) -> None:
        from scalper.models import TradeRecord
        from scalper.risk_engine_core import RiskEngine

        self._reload_state()
        engine = RiskEngine(self._state, self._settings_proxy(), __import__("storage"))
        engine.on_fill(
            TradeRecord(
                symbol="",
                side="",
                setup="",
                close_ts=self._now_fn().isoformat(),
                pnl_usdt=float(pnl_usdt or 0.0),
                close_reason="paper_close",
            )
        )

    def record_trade_outcome(self, outcome) -> None:
        if isinstance(outcome, dict):
            pnl_value = float(outcome.get("pnl_usdt", 0.0))
        else:
            pnl_value = float(outcome)
        self.record_paper_close(pnl_value)

    def apply_simulated_pnl_update(self, pnl_delta: float) -> None:
        # Backward-compatible alias for existing callers.
        self.record_paper_close(
            pnl_usdt=pnl_delta
        )

    def _reset_daily_if_needed(self) -> None:
        now_utc = self._active_now or self._now_fn()
        today = now_utc.date().isoformat()
        state_day = str(
            self._state.get("day_utc", self._state.get("state_date", ""))
        )
        if state_day == today:
            return
        self._state["day_utc"] = today
        self._state.pop("state_date", None)
        self._state["trade_count_today"] = 0
        self._state["daily_pnl_sim"] = 0.0
        self._state["consecutive_losses"] = 0
        self._state["cooldown_until_utc"] = ""

    def _mark_allowed_intent(self) -> None:
        self._state["trade_count_today"] = int(self._state.get("trade_count_today", 0)) + 1

    def _reload_state(self) -> None:
        self._state = load_paper_state()

    def _settings_proxy(self):
        return SimpleNamespace(
            kill_switch=bool(self.kill_switch_on),
            risk_max_trades_per_day=int(self.max_trades_per_day),
            risk_max_daily_loss_sim=float(self.max_daily_loss_sim),
            risk_max_consecutive_losses=int(self.max_consecutive_losses),
            risk_cooldown_minutes=int(self.cooldown_minutes),
            daily_loss_limit_pct=1.0,
            max_dd_pct=12.0,
            max_trades_day=int(self.max_trades_per_day),
            min_seconds_between_trades=180,
            min_seconds_between_symbol_trades=900,
            max_symbol_notional_pct=30.0,
            cluster_btc_eth_limit=1,
            fail_closed_on_snapshot_missing=True,
            paper_equity_usdt=float(getattr(self, "paper_equity_usdt", 200.0) or 200.0),
            max_open_positions=int(self.max_open_positions),
            max_concurrent_positions=int(self.max_open_positions),
            risk_one_position_per_symbol=bool(self.one_position_per_symbol),
            position_mode="per_symbol" if self.one_position_per_symbol else "global",
        )