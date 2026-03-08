from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from scalper.paper_engine import *  # noqa: F401,F403
from scalper.trade_preview import build_trade_preview


def compute_entry_sl_tp_for_display(
    trade_intent: Dict[str, Any],
    candles: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    *,
    sl_atr_mult: float = 1.0,
    tp_atr_mult: float = 1.5,
) -> Optional[Dict[str, float]]:
    preview = build_trade_preview(
        signal=trade_intent,
        market_snapshot=snapshot,
        candles=candles,
        risk_settings=SimpleNamespace(
            paper_sl_atr=float(sl_atr_mult),
            paper_tp_atr=float(tp_atr_mult),
            risk_per_trade_pct=0.0,
            paper_start_equity_usdt=float(snapshot.get("paper_position_usdt", 50.0) or 50.0),
            preview_min_rr=0.1,
            preview_min_atr_pct=0.0,
            preview_max_atr_pct=100.0,
            preview_max_retest_drift_pct=999.0,
            tf_trigger=15,
        ),
        equity_usdt=float(snapshot.get("paper_position_usdt", 50.0) or 50.0),
        for_execution=False,
    )
    if not preview.get("ok"):
        return None
    return {
        "entry": float(preview["entry"]),
        "sl": float(preview["sl"]),
        "tp": float(preview["tp"]),
        "sl_pct": float(preview["sl_pct"]),
        "tp_pct": float(preview["tp_pct"]),
    }
