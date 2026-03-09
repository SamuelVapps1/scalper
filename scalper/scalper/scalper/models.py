from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Candle:
    timestamp: int = 0
    timestamp_utc: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


@dataclass
class SnapshotTF:
    ema20: float = 0.0
    ema50: float = 0.0
    ema200: float = 0.0
    ema200_slope_10: Optional[float] = None
    atr14: float = 0.0
    open: float = 0.0
    close: float = 0.0
    high: float = 0.0
    low: float = 0.0
    ts: str = ""


@dataclass
class MTFSnapshot:
    frames: Dict[Any, SnapshotTF] = field(default_factory=dict)


@dataclass
class StrategyResult:
    ok: bool
    side: Optional[str] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    reason: str = ""
    debug: Dict[str, Any] = field(default_factory=dict)
    breakout_level: Optional[float] = None

    # Compatibility adapter for legacy dict consumers.
    def to_legacy_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        return out

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_legacy_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_legacy_dict()[key]


@dataclass
class TradeIntent:
    symbol: str
    side: str
    strategy: str
    entry_type: str = "market_sim"
    close: Optional[float] = None
    level_ref: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskVerdict:
    allowed: bool
    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeRecord:
    symbol: str
    side: str
    setup: str
    entry_ts: str = ""
    close_ts: str = ""
    entry_price: float = 0.0
    exit_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    qty: float = 0.0
    pnl_usdt: float = 0.0
    risk_usdt: Optional[float] = None
    r_multiple: Optional[float] = None
    close_reason: str = ""
    debug: Dict[str, Any] = field(default_factory=dict)


def strategy_result_to_legacy_dict(result: StrategyResult) -> Dict[str, Any]:
    return result.to_legacy_dict()


def ensure_strategy_result(value: Any) -> StrategyResult:
    if isinstance(value, StrategyResult):
        return value
    if isinstance(value, dict):
        return StrategyResult(
            ok=bool(value.get("ok", False)),
            side=value.get("side"),
            entry=value.get("entry"),
            sl=value.get("sl"),
            tp=value.get("tp"),
            reason=str(value.get("reason", "") or ""),
            debug=dict(value.get("debug", {}) or {}),
            breakout_level=value.get("breakout_level"),
        )
    return StrategyResult(ok=False, reason="invalid_result_type")

