from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List

from scalper.risk_engine_core import RiskEngine


@dataclass
class _FakeStore:
    state: Dict[str, Any]
    trade_intents: List[Dict[str, Any]] = field(default_factory=list)
    risk_events: List[Dict[str, Any]] = field(default_factory=list)

    def load_paper_state(self) -> Dict[str, Any]:
        return dict(self.state)

    def save_paper_state(self, state: Dict[str, Any]) -> None:
        self.state = dict(state)

    def store_trade_intent(self, intent: Dict[str, Any]) -> None:
        self.trade_intents.append(dict(intent))

    def store_risk_event(self, event: Dict[str, Any]) -> None:
        self.risk_events.append(dict(event))


def _valid_intent(**overrides: Any) -> Dict[str, Any]:
    base = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "V3",
        "timeframe": "15",
        "bar_ts": "2026-02-27T12:00:00+00:00",
    }
    base.update(overrides)
    return base


def _settings() -> Any:
    return SimpleNamespace(
        paper_equity_usdt=1000.0,
        max_concurrent_positions=2,
        daily_loss_limit_pct=1.0,
        max_dd_pct=12.0,
        max_trades_day=12,
        min_seconds_between_trades=180,
        min_seconds_between_symbol_trades=900,
        max_symbol_notional_pct=30.0,
        cluster_btc_eth_limit=1,
        fail_closed_on_snapshot_missing=True,
        risk_cooldown_minutes=30,
    )


def test_blocks_when_max_concurrent_positions_reached() -> None:
    store = _FakeStore(
        state={
            "day_utc": "2026-02-27",
            "open_positions": [{"symbol": "BTCUSDT", "status": "OPEN"}, {"symbol": "SOLUSDT", "status": "OPEN"}],
            "equity_peak": 1000.0,
        }
    )
    engine = RiskEngine(store.state, _settings(), store)
    verdict = engine.evaluate(
        intent=_valid_intent(symbol="ETHUSDT"),
        snapshot={"equity": 1000.0, "open_positions_count": 2, "open_positions": store.state["open_positions"]},
        now=datetime(2026, 2, 27, 12, 0, tzinfo=timezone.utc),
    )
    assert not verdict.allowed
    assert verdict.reason == "MAX_POSITIONS"


def test_daily_loss_limit_triggers_pause_until_end_of_day() -> None:
    store = _FakeStore(
        state={
            "day_utc": "2026-02-27",
            "daily_pnl_realized": -12.0,
            "equity_peak": 1000.0,
        }
    )
    engine = RiskEngine(store.state, _settings(), store)
    verdict = engine.evaluate(
        intent=_valid_intent(),
        snapshot={"equity": 988.0, "open_positions_count": 0, "open_positions": []},
        now=datetime(2026, 2, 27, 10, 0, tzinfo=timezone.utc),
    )
    assert not verdict.allowed
    assert verdict.reason == "DAILY_STOP"
    assert int(store.state.get("pause_until_ts", 0) or 0) > int(datetime(2026, 2, 27, 10, 0, tzinfo=timezone.utc).timestamp())


def test_max_dd_triggers_kill_and_persists_until_manual_reset() -> None:
    store = _FakeStore(state={"day_utc": "2026-02-27", "equity_peak": 1000.0})
    engine = RiskEngine(store.state, _settings(), store)
    v1 = engine.evaluate(
        intent=_valid_intent(),
        snapshot={"equity": 850.0, "open_positions_count": 0, "open_positions": []},
        now=datetime(2026, 2, 27, 12, 0, tzinfo=timezone.utc),
    )
    assert not v1.allowed
    assert v1.reason == "MAX_DD"
    assert str(store.state.get("kill_reason", "")) == "MAX_DD"

    v2 = engine.evaluate(
        intent=_valid_intent(symbol="ETHUSDT"),
        snapshot={"equity": 1000.0, "open_positions_count": 0, "open_positions": []},
        now=datetime(2026, 2, 27, 12, 1, tzinfo=timezone.utc),
    )
    assert not v2.allowed
    assert v2.reason == "KILLED"


def test_cluster_btc_eth_limit_blocks_other_symbol() -> None:
    store = _FakeStore(
        state={
            "day_utc": "2026-02-27",
            "open_positions": [{"symbol": "BTCUSDT", "status": "OPEN"}],
            "equity_peak": 1000.0,
        }
    )
    engine = RiskEngine(store.state, _settings(), store)
    verdict = engine.evaluate(
        intent=_valid_intent(symbol="ETHUSDT"),
        snapshot={"equity": 1000.0, "open_positions_count": 1, "open_positions": store.state["open_positions"]},
        now=datetime(2026, 2, 27, 12, 0, tzinfo=timezone.utc),
    )
    assert not verdict.allowed
    assert verdict.reason == "CLUSTER_LIMIT"

