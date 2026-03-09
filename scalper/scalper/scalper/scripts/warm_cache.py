#!/usr/bin/env python3
"""
Download-only cache warmup: prefills candle cache for replay without running strategy.
Uses the same candle_cache.get_candles loader as replay.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

# Ensure project root on path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(1, str(_project_root / "scripts"))

from scalper.settings import get_settings  # noqa: E402

import candle_cache  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _parse_bool(s: str) -> bool:
    v = str(s).strip().lower()
    return v in ("1", "true", "yes", "on")


def _cache_bounds(symbol: str, tf_min: int) -> Tuple[Optional[int], Optional[int], int]:
    path = candle_cache._cache_path(symbol, tf_min)
    if not path.exists():
        return None, None, 0
    candles = candle_cache._load_cache_file(path)
    if not candles:
        return None, None, 0
    return int(candles[0]["timestamp"]), int(candles[-1]["timestamp"]), len(candles)


def _strict_covered(cache_min: Optional[int], cache_max: Optional[int], start_aligned: int, end_aligned: int) -> bool:
    if cache_min is None or cache_max is None:
        return False
    return cache_min <= start_aligned and cache_max >= end_aligned


def _delete_symbol_cache(symbol: str) -> None:
    sym_dir = (Path.cwd() / "data" / "candles" / symbol.upper())
    if sym_dir.exists():
        shutil.rmtree(sym_dir, ignore_errors=True)


def _delete_tf_cache(symbol: str, tf_min: int) -> None:
    tf_path = candle_cache._cache_path(symbol, tf_min)
    if tf_path.exists():
        tf_path.unlink(missing_ok=True)


def main() -> int:
    # Bootstrap env/.env exactly once before any cache calls.
    get_settings()
    parser = argparse.ArgumentParser(
        description="Cache repair tool for replay ranges."
    )
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--days", type=int, default=365, help="Days of history")
    parser.add_argument("--end-days-ago", type=int, default=None, help="End date = now - N days (same semantics as replay)")
    parser.add_argument("--tfs", type=str, default="240,60,15,5", help="Comma-separated TFs (minutes or aliases like 60m)")
    parser.add_argument("--rebuild", action="store_true", help="Delete symbol cache before warming and fetch fresh range.")
    parser.add_argument(
        "--rebuild-missing",
        action="store_true",
        help="If coverage is missing after warm, delete tf cache and refetch full range once.",
    )
    parser.add_argument("--cache-only", action="store_true", help="Use only cache (no API calls)")
    fail_group = parser.add_mutually_exclusive_group()
    fail_group.add_argument("--fail-on-missing", dest="fail_on_missing", action="store_true")
    fail_group.add_argument("--no-fail-on-missing", dest="fail_on_missing", action="store_false")
    parser.set_defaults(fail_on_missing=True)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        _log.error("No symbols provided")
        return 2

    tfs: list[int] = []
    for s in args.tfs.split(","):
        s = s.strip()
        if not s:
            continue
        tfs.append(candle_cache._tf_to_min(s))  # Shared parser with cache engine.
    if not tfs:
        tfs = [240, 60, 15, 5]
    # Keep stable order and remove duplicates.
    tfs = list(dict.fromkeys(tfs))

    now_utc_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_days_ago = int(args.end_days_ago or 0)
    day_ms = 24 * 60 * 60 * 1000
    end_ms = now_utc_ms - end_days_ago * day_ms
    start_ms = end_ms - int(args.days) * day_ms

    _log.info(
        "CACHE_REPAIR symbols=%s days=%d end_days_ago=%d tfs=%s cache_only=%s rebuild=%s rebuild_missing=%s fail_on_missing=%s",
        symbols,
        args.days,
        end_days_ago,
        tfs,
        bool(args.cache_only),
        bool(args.rebuild),
        bool(args.rebuild_missing),
        bool(args.fail_on_missing),
    )

    for symbol in symbols:
        if args.rebuild:
            _delete_symbol_cache(symbol)
        for tf_min in tfs:
            start_aligned, end_aligned = candle_cache._align_range(start_ms, end_ms, tf_min)
            timing_out: dict = {}
            t0 = time.perf_counter()
            try:
                candles = candle_cache.get_candles(
                    symbol=symbol,
                    tf=tf_min,
                    start_ms=start_aligned,
                    end_ms=end_aligned,
                    use_cache=True,
                    cache_only=bool(args.cache_only),
                    _timing_out=timing_out,
                )
            except Exception as exc:
                raise RuntimeError(f"WARM failed symbol={symbol} tf={tf_min}: {exc}") from exc
            elapsed_s = time.perf_counter() - t0
            cache_min, cache_max, bars = _cache_bounds(symbol, tf_min)
            covered = _strict_covered(cache_min, cache_max, start_aligned, end_aligned)
            source = timing_out.get("source", "?")
            _log.info(
                "WARM symbol=%s tf=%s covered=%s cache=[%s..%s] target=[%d..%d] bars=%d source=%s elapsed_s=%.2f",
                symbol,
                tf_min,
                covered,
                str(cache_min),
                str(cache_max),
                start_aligned,
                end_aligned,
                bars,
                source,
                elapsed_s,
            )

            if (not covered) and args.rebuild_missing and (not args.cache_only):
                _log.warning("WARM_REBUILD_MISSING symbol=%s tf=%s action=delete_refetch", symbol, tf_min)
                _delete_tf_cache(symbol, tf_min)
                timing_out2: dict = {}
                t1 = time.perf_counter()
                candles = candle_cache.get_candles(
                    symbol=symbol,
                    tf=tf_min,
                    start_ms=start_aligned,
                    end_ms=end_aligned,
                    use_cache=True,
                    cache_only=False,
                    _timing_out=timing_out2,
                )
                elapsed2_s = time.perf_counter() - t1
                cache_min, cache_max, bars = _cache_bounds(symbol, tf_min)
                covered = _strict_covered(cache_min, cache_max, start_aligned, end_aligned)
                source = timing_out2.get("source", "?")
                _log.info(
                    "WARM symbol=%s tf=%s covered=%s cache=[%s..%s] target=[%d..%d] bars=%d source=%s elapsed_s=%.2f",
                    symbol,
                    tf_min,
                    covered,
                    str(cache_min),
                    str(cache_max),
                    start_aligned,
                    end_aligned,
                    bars,
                    source,
                    elapsed2_s,
                )

            if (not covered) and args.fail_on_missing:
                raise RuntimeError(
                    f"WARM_COVERAGE_MISSING symbol={symbol} tf={tf_min} "
                    f"cache=[{cache_min}..{cache_max}] target=[{start_aligned}..{end_aligned}]"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
