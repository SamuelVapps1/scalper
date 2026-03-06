from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


@dataclass
class Intent:
    """
    Unified signal intent schema used by hybrid strategies.

    This is an internal representation that is later adapted into the
    dict/TradeIntent shapes expected by the scanner/risk/telegram pipeline.
    """

    symbol: str
    tf: str
    side: str  # LONG / SHORT
    setup: str
    confidence: float
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    sl_pct: Optional[float] = None
    tp_pct: Optional[float] = None
    bar_ts_used: str = ""
    reason: str = ""
    risk_reason: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_signal_dict(self) -> Dict[str, Any]:
        """
        Convert to the loose dict shape used in the existing scanner pipeline.
        """
        raw = asdict(self)
        symbol = str(self.symbol or "").upper()
        side = str(self.side or "").upper()
        setup = str(self.setup or "").strip() or "HYBRID"
        bar_ts = str(self.bar_ts_used or "")
        out: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "strategy": setup,
            "setup": setup,
            "reason": self.reason or "",
            "confidence": float(self.confidence or 0.0),
            "entry": self.entry,
            "sl": self.sl,
            "tp": self.tp,
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
            "bar_ts_used": bar_ts,
            "timeframe": str(self.tf or ""),
            "ts": bar_ts,
            "meta": dict(self.meta or {}),
        }
        if self.risk_reason:
            out.setdefault("risk", {})["reason"] = str(self.risk_reason)
        return out
