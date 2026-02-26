#!/usr/bin/env python3
"""
Download-only cache warmup: prefills candle cache for replay without running strategy.
Uses the same candle_cache.get_candles loader as replay.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root on path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(1, str(_project_root / "scripts"))

import config  # noqa: E402 - load dotenv before other imports

import candle_cache  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_bool(s: str) -> bool:
    v = str(s).strip().lower()
    return v in ("1", "true", "yes", "on")


def _check_covered(candles: list, start_ms: int, end_ms: int, tf_min: int) -> bool:
    """Check if returned candles cover [start_ms, end_ms] with 1-bar tolerance."""
    if not candles:
        return False
    bar_ms = tf_min * 60 * 1000
    aligned_start = (start_ms // bar_ms) * bar_ms
    aligned_end = (end_ms // bar_ms) * bar_ms
    min_ts = min(c.get("timestamp") or c.get("ts") or 0 for c in candles)
    max_ts = max(c.get("timestamp") or c.get("ts") or 0 for c in candles)
    return min_ts <= aligned_start + bar_ms and max_ts >= aligned_end - bar_ms


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Warm candle cache for replay. Download-only, no strategy."
    )
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--days", type=int, default=365, help="Days of history")
    parser.add_argument("--tfs", type=str, default="240,60,15,5", help="Comma-separated TFs (minutes)")
    parser.add_argument("--use-cache", type=str, default="true", help="Use cache (true/false)")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        _log.error("No symbols provided")
        return 2

    tfs = []
    for s in args.tfs.split(","):
        s = s.strip()
        if s.isdigit():
            tfs.append(int(s))
    if not tfs:
        tfs = [240, 60, 15, 5]

    use_cache = _parse_bool(args.use_cache)

    end_dt = datetime.now(timezone.utc)
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = end_ms - args.days * 24 * 60 * 60 * 1000

    _log.info("Warming cache: symbols=%s days=%d tfs=%s use_cache=%s", symbols, args.days, tfs, use_cache)

    for symbol in symbols:
        for tf_min in tfs:
            timing_out: dict = {}
            t0 = time.perf_counter()
            candles = candle_cache.get_candles(
                symbol=symbol,
                tf=tf_min,
                start_ms=start_ms,
                end_ms=end_ms,
                use_cache=use_cache,
                cache_only=False,
                _timing_out=timing_out,
            )
            elapsed_s = time.perf_counter() - t0
            covered = _check_covered(candles, start_ms, end_ms, tf_min)
            source = timing_out.get("source", "?")
            _log.info(
                "WARM symbol=%s tf=%s bars=%d covered=%s source=%s elapsed_s=%.2f",
                symbol, tf_min, len(candles), covered, source, elapsed_s,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
