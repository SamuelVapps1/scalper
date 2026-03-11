from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from telegram_format import format_signal_alert


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    sample_signal = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "RANGE_BREAKOUT_RETEST_GO",
        "reason": "Range breakout -> retest -> go",
        "confidence": 0.74,
        "confidence_source": "confidence",
        "entry": 65780.0,
        "sl": 65466.33,
        "tp": 66250.49,
        "sl_pct": 0.48,
        "tp_pct": 0.72,
        "bar_ts_used": "2026-03-07T12:15:00+00:00",
    }
    msg = format_signal_alert(sample_signal, {"tf": "15", "telegram_format": "verbose"})
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
