"""Tests for V3 dual-profile (RAW + CONSERVATIVE) signal machine."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _intent_fingerprint(symbol: str, strategy: str, side: str, bar_ts: str, profile: str = "") -> str:
    base = f"{symbol}|{strategy}|{side}|{bar_ts}"
    if profile:
        return f"{base}|{profile}"
    return base


def test_dedupe_keys_raw_and_hq_separate():
    """RAW and HQ use different fingerprint keys to avoid cross-profile spam."""
    bar_ts = "2024-01-15T12:00:00+00:00"
    fp_raw = _intent_fingerprint("BTCUSDT", "V3_TREND_BREAKOUT", "LONG", bar_ts, profile="RAW")
    fp_hq = _intent_fingerprint("BTCUSDT", "V3_TREND_BREAKOUT", "LONG", bar_ts, profile="HQ")
    assert fp_raw != fp_hq
    assert "|RAW" in fp_raw
    assert "|HQ" in fp_hq


def test_dedupe_keys_same_profile_same_bar_is_duplicate():
    """Same profile + same bar_ts => same fingerprint => duplicate."""
    bar_ts = "2024-01-15T12:00:00+00:00"
    fp1 = _intent_fingerprint("BTCUSDT", "V3_TREND_BREAKOUT", "LONG", bar_ts, profile="RAW")
    fp2 = _intent_fingerprint("BTCUSDT", "V3_TREND_BREAKOUT", "LONG", bar_ts, profile="RAW")
    assert fp1 == fp2


def test_parse_v3_conservative_params():
    """V3_CONSERVATIVE_PARAMS parses KEY=VAL;KEY2=VAL2."""
    from scalper.strategies.v3_strategy import _parse_v3_conservative_params

    out = _parse_v3_conservative_params("ATR_REGIME_MIN_PCTL=35;USE_5M_CONFIRM=0")
    assert out.get("ATR_REGIME_MIN_PCTL") == 35
    assert out.get("USE_5M_CONFIRM") is False

    out2 = _parse_v3_conservative_params("")
    assert out2 == {}


def test_format_intent_allow_includes_profile():
    """format_intent_allow shows [RAW] or [HQ] or [RAW+HQ] when profile present."""
    from telegram_format import format_intent_allow

    intent = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "strategy": "V3_TREND_BREAKOUT",
        "confidence": 0.70,
        "reason": "V3 breakout",
        "profile": "RAW",
    }
    risk = {"reason": "ok"}
    ctx = {"tf": "15", "bar_ts_used": "2024-01-15T12:00:00+00:00", "profile": "RAW"}
    msg = format_intent_allow(intent, risk, ctx)
    assert "[RAW]" in msg

    intent["profile"] = "HQ"
    intent["meta"] = {"conf_raw": 0.70, "conf_hq": 0.70}
    ctx["profile"] = "HQ"
    ctx["conf_raw"] = 0.70
    ctx["conf_hq"] = 0.70
    msg2 = format_intent_allow(intent, risk, ctx)
    assert "[RAW+HQ]" in msg2
