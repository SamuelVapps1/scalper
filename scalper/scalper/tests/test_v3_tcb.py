from __future__ import annotations

from typing import Any, Dict, List

from scalper.models import StrategyResult
from strategies.strategy_v3_tcb import v3_tcb_evaluate


def _candles_15m(n: int) -> List[Dict[str, Any]]:
    base = 1_700_000_000_000
    bar_ms = 15 * 60 * 1000
    out: List[Dict[str, Any]] = []
    for i in range(n):
        o = 100.0 + i * 0.4
        c = o + 0.2
        out.append(
            {
                "timestamp": base + i * bar_ms,
                "open": o,
                "high": c + 0.4,
                "low": o - 0.4,
                "close": c,
                "volume": 20.0 + i,
            }
        )
    # Force a clear breakout candle.
    out[-1]["open"] = out[-2]["close"] + 0.8
    out[-1]["close"] = out[-1]["open"] + 2.5
    out[-1]["high"] = out[-1]["close"] + 0.3
    out[-1]["low"] = out[-1]["open"] - 0.6
    return out


def test_v3_tcb_missing_snapshot_returns_reason() -> None:
    res = v3_tcb_evaluate(
        symbol="BTCUSDT",
        snapshot_symbol={},
        candles_15m=[],
        candles_5m=None,
        i15=0,
        params={},
    )
    assert isinstance(res, StrategyResult)
    assert not res.ok
    assert res.reason == "v3_missing_4h_slope_inputs"


def test_v3_tcb_basic_long_breakout_sanity() -> None:
    candles_15m = _candles_15m(30)
    i15 = len(candles_15m) - 1
    snapshot = {
        "4H": {"close": 320.0, "ema200": 300.0, "ema200_slope_10": 5.0},
        "1H": {"ema20": 210.0, "ema50": 200.0, "ema200": 180.0, "atr14": 8.0},
    }
    params = {
        "DONCHIAN_N_15M": 20,
        "BODY_ATR_15M": 0.1,
        "TREND_SEP_ATR_1H": 0.5,
        "USE_5M_CONFIRM": False,
    }

    res = v3_tcb_evaluate(
        symbol="BTCUSDT",
        snapshot_symbol=snapshot,
        candles_15m=candles_15m,
        candles_5m=None,
        i15=i15,
        params=params,
    )
    assert isinstance(res, StrategyResult)
    assert res.ok
    assert res.side == "LONG"
    assert res.breakout_level is not None

