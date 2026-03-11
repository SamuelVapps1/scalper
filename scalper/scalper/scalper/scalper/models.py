"""
Canonical models for strategy/risk contract.
TradeIntent: required symbol, side, strategy_id, timeframe, bar_ts.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Tuple


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
    intent: Optional["TradeIntent"] = None

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
    """Canonical intent for RiskEngine. Required: symbol, side, strategy_id, timeframe, bar_ts."""
    symbol: str
    side: str
    strategy_id: str
    timeframe: str
    bar_ts: str
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    confidence: float = 0.0
    debug: Optional[Dict[str, Any]] = None
    # Legacy aliases for PaperBroker/consumers
    strategy: str = ""
    setup: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.strategy:
            self.strategy = self.strategy_id
        if not self.setup:
            self.setup = self.strategy_id

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["strategy"] = d.get("strategy") or self.strategy_id
        d["setup"] = d.get("setup") or self.strategy_id
        d["direction"] = self.side
        return d


def validate_trade_intent(intent: Any) -> Tuple[bool, str]:
    """
    Validate intent has required fields. Returns (ok, reason).
    If invalid: (False, "INTENT_MISSING_<field>") or similar.
    """
    if intent is None:
        return False, "INTENT_NONE"
    if isinstance(intent, TradeIntent):
        raw = intent.to_dict()
    elif isinstance(intent, dict):
        raw = intent
    else:
        return False, "INTENT_INVALID_TYPE"

    symbol = str(raw.get("symbol", "") or "").strip().upper()
    if not symbol:
        return False, "INTENT_MISSING_SYMBOL"

    side = str(raw.get("side", raw.get("direction", "")) or "").strip().upper()
    if side not in ("LONG", "SHORT"):
        return False, "INTENT_MISSING_OR_INVALID_SIDE"

    strategy_id = str(raw.get("strategy_id", raw.get("strategy", raw.get("setup", ""))) or "").strip()
    if not strategy_id:
        return False, "INTENT_MISSING_STRATEGY_ID"

    timeframe = str(raw.get("timeframe", "") or "").strip()
    if not timeframe:
        return False, "INTENT_MISSING_TIMEFRAME"

    bar_ts = str(raw.get("bar_ts", raw.get("candle_ts", raw.get("ts", ""))) or "").strip()
    if not bar_ts:
        return False, "INTENT_MISSING_BAR_TS"

    return True, ""


def intent_from_dict(raw: Dict[str, Any]) -> TradeIntent:
    """Build TradeIntent from dict (e.g. from strategy final_intents)."""
    return TradeIntent(
        symbol=str(raw.get("symbol", "") or "").strip().upper(),
        side=str(raw.get("side", raw.get("direction", "")) or "").strip().upper(),
        strategy_id=str(raw.get("strategy_id", raw.get("strategy", raw.get("setup", ""))) or "").strip(),
        timeframe=str(raw.get("timeframe", "") or "").strip() or "15",
        bar_ts=str(raw.get("bar_ts", raw.get("candle_ts", raw.get("ts", ""))) or "").strip(),
        entry=float(raw["entry"]) if raw.get("entry") is not None else None,
        sl=float(raw["sl"]) if raw.get("sl") is not None else None,
        tp=float(raw["tp"]) if raw.get("tp") is not None else None,
        confidence=float(raw.get("confidence", 0) or 0),
        debug=dict(raw.get("debug", {}) or {}) if raw.get("debug") else None,
        meta=dict(raw.get("meta", {}) or {}),
    )


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

