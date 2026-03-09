from __future__ import annotations

from typing import Any, Dict, List
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scalper.strategies.rev_swept_rsi import evaluate_reconcile, generate_intents


def _make_base_bars(n: int = 80) -> List[Dict[str, Any]]:
    bars: List[Dict[str, Any]] = []
    px = 100.0
    for i in range(n):
        close = px + (0.08 if i % 2 else -0.07)
        bars.append(
            {
                "ts": f"2026-01-01T00:{i:02d}:00+00:00",
                "open": px,
                "high": max(px, close) + 0.35,
                "low": min(px, close) - 0.35,
                "close": close,
                "ema20": 100.0,
                "ema50": 100.0,
                "ema200": 100.0,
                "ema200_prev10": 100.0,
                "atr14": 0.9,
            }
        )
        px = close
    return bars


def _case_bullish() -> pd.DataFrame:
    rows = _make_base_bars(85)
    # Create lower-low sweep with rejection near close.
    rows[-4]["low"] = 98.4
    rows[-4]["close"] = 99.3
    rows[-3]["low"] = 97.8
    rows[-3]["close"] = 99.1
    rows[-2]["low"] = 97.1
    rows[-2]["close"] = 99.6
    rows[-1]["open"] = 99.4
    rows[-1]["low"] = 96.8
    rows[-1]["high"] = 100.1
    rows[-1]["close"] = 99.95
    return pd.DataFrame(rows)


def _case_bearish() -> pd.DataFrame:
    rows = _make_base_bars(85)
    # Create higher-high sweep with rejection near close.
    rows[-4]["high"] = 101.6
    rows[-4]["close"] = 100.8
    rows[-3]["high"] = 102.0
    rows[-3]["close"] = 100.7
    rows[-2]["high"] = 102.4
    rows[-2]["close"] = 100.5
    rows[-1]["open"] = 100.6
    rows[-1]["high"] = 103.0
    rows[-1]["low"] = 99.8
    rows[-1]["close"] = 100.05
    return pd.DataFrame(rows)


def _settings_stub():
    return type(
        "S",
        (),
        {
            "strategy_v3": type(
                "SV3",
                (),
                {
                    "rev_enabled": True,
                    "rev_sweep_lookback_bars": 30,
                    "rev_sweep_tol_atr": 0.25,
                    "rev_rsi_period": 14,
                    "rev_pivot_left": 3,
                    "rev_pivot_right": 3,
                    "rev_min_rsi_delta": 3.0,
                    "rev_ema200_dist_pct": 0.02,
                    "rev_sl_atr_mult": 0.35,
                    "rev_sl_buffer_atr": 0.10,
                    "rev_entry_mode": "close",
                    "rev_max_trend_sep_atr_1h": 2.0,
                    "rev_require_1h_align": False,
                    "require_1h_ema200_align": False,
                    "bos_lookback_5m": 10,
                },
            )(),
            "risk": type("R", (), {"min_atr_pct_universe": 0.003})(),
        },
    )()


def run_demo() -> None:
    settings = _settings_stub()
    mtf = {
        15: {"close": 100.0, "ema200": 100.0, "atr14": 1.1},
        60: {"close": 100.0, "ema200": 100.0, "atr14": 1.3},
    }
    for name, df in (("bullish_case", _case_bullish()), ("bearish_case", _case_bearish())):
        ctx = {"symbol": "TESTUSDT", "tf": "5", "settings": settings, "mtf_snapshot": mtf, "bar_ts_used": ""}
        rec = evaluate_reconcile(df, ctx)
        intents = generate_intents(df, ctx)
        print(f"[{name}] ok_long={rec.get('ok_long')} ok_short={rec.get('ok_short')} intents={len(intents)}")
        for it in intents:
            print(
                f"  intent side={it.side} setup={it.setup} conf={it.confidence:.2f} "
                f"entry={it.entry:.4f} sl={it.sl:.4f} reason={it.reason}"
            )
        if not intents:
            print("  reasons_long=", rec.get("reasons_long"))
            print("  reasons_short=", rec.get("reasons_short"))


if __name__ == "__main__":
    run_demo()

