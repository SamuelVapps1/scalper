"""Tests for watchlist exclude filters and rotation offset."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _mock_bybit_tickers_response(symbols: list[str]) -> dict:
    """Build mock Bybit /market/tickers response for given symbols."""
    rows = []
    for i, sym in enumerate(symbols):
        rows.append({
            "symbol": sym,
            "quoteCoin": "USDT",
            "status": "Trading",
            "lastPrice": "1.0",
            "turnover24h": str(1_000_000 * (100 - i)),
            "volume24h": str(500_000 * (100 - i)),
            "bid1Price": "0.99",
            "ask1Price": "1.01",
        })
    return {"retCode": 0, "result": {"list": rows}}


def test_get_top_linear_excludes_symbols():
    """Excluded symbols (e.g. HYPEUSDT) must not appear in result."""
    universe = ["BTCUSDT", "ETHUSDT", "HYPEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT"]
    with patch("bybit._do_json_request") as mock_req:
        mock_req.return_value = _mock_bybit_tickers_response(universe)
        from bybit import get_top_linear_usdt_symbols

        result = get_top_linear_usdt_symbols(
            10,
            exclude_prefixes=["1000", "10000"],
            exclude_symbols=["HYPEUSDT"],
            exclude_regex="",
        )
        assert "HYPEUSDT" not in result
        assert "1000PEPEUSDT" not in result
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result


def test_get_top_linear_excludes_prefixes():
    """Excluded prefixes (1000, 10000) must exclude 1000PEPEUSDT etc."""
    universe = ["BTCUSDT", "1000PEPEUSDT", "10000FLOKIUSDT", "ETHUSDT"]
    with patch("bybit._do_json_request") as mock_req:
        mock_req.return_value = _mock_bybit_tickers_response(universe)
        from bybit import get_top_linear_usdt_symbols

        result = get_top_linear_usdt_symbols(
            10,
            exclude_prefixes=["1000", "10000"],
            exclude_symbols=[],
            exclude_regex="",
        )
        assert "1000PEPEUSDT" not in result
        assert "10000FLOKIUSDT" not in result
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result


def test_get_top_linear_excludes_regex():
    """Excluded regex must filter matching symbols."""
    universe = ["BTCUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT", "ETHUSDT"]
    with patch("bybit._do_json_request") as mock_req:
        mock_req.return_value = _mock_bybit_tickers_response(universe)
        from bybit import get_top_linear_usdt_symbols

        result = get_top_linear_usdt_symbols(
            10,
            exclude_prefixes=[],
            exclude_symbols=[],
            exclude_regex=r"PEPE|FLOKI|BONK",
        )
        assert "PEPEUSDT" not in result
        assert "FLOKIUSDT" not in result
        assert "BONKUSDT" not in result
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result


def test_excluded_counts_returned():
    """get_last_topn_excluded_counts returns prefix/symbol/regex counts."""
    universe = ["BTCUSDT", "HYPEUSDT", "1000PEPEUSDT", "PIPPINUSDT", "ETHUSDT"]
    with patch("bybit._do_json_request") as mock_req:
        mock_req.return_value = _mock_bybit_tickers_response(universe)
        from bybit import get_top_linear_usdt_symbols, get_last_topn_excluded_counts

        get_top_linear_usdt_symbols(
            10,
            exclude_prefixes=["1000"],
            exclude_symbols=["HYPEUSDT", "PIPPINUSDT"],
            exclude_regex="",
        )
        counts = get_last_topn_excluded_counts()
        assert counts["prefix"] >= 1
        assert counts["symbol"] >= 2


def test_roundrobin_offset_advances():
    """Rotation offset persists and advances across runs."""
    from scalper.storage import get_watchlist_rotation_offset, set_watchlist_rotation_offset

    initial = get_watchlist_rotation_offset()
    set_watchlist_rotation_offset(25)
    assert get_watchlist_rotation_offset() == 25
    set_watchlist_rotation_offset(50)
    assert get_watchlist_rotation_offset() == 50
    set_watchlist_rotation_offset(initial)
    assert get_watchlist_rotation_offset() == initial


def test_market_watchlist_universe_smaller_than_batch_no_duplicates():
    """Universe size 8, batch_n 25 -> selected size must be 8, all unique."""
    universe_8 = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"]
    with patch("bybit.get_top_linear_usdt_symbols") as mock_top:
        mock_top.return_value = universe_8
        with patch("bybit.get_last_topn_excluded_counts") as mock_counts:
            mock_counts.return_value = {"prefix": 0, "symbol": 0, "regex": 0}
            from watchlist import get_watchlist
            import types
            config = types.SimpleNamespace(
                WATCHLIST_MODE="market",
                WATCHLIST=["BTCUSDT"],
                WATCHLIST_UNIVERSE_N=200,
                WATCHLIST_BATCH_N=25,
                WATCHLIST_REFRESH_SECONDS=600,
                WATCHLIST_ROTATE_MODE="roundrobin",
                WATCHLIST_ROTATE_SEED=0,
                WATCHLIST_MIN_PRICE=0.01,
                WATCHLIST_MIN_TURNOVER_24H=0.0,
                WATCHLIST_EXCLUDE_PREFIXES=[],
                WATCHLIST_EXCLUDE_SYMBOLS=[],
                WATCHLIST_EXCLUDE_REGEX="",
                WATCHLIST_MAX_SPREAD_BPS=0.0,
            )
            symbols, mode = get_watchlist(config)
            assert mode == "market"
            assert len(symbols) == 8
            assert len(symbols) == len(set(symbols))
            assert set(symbols) == set(universe_8)


def test_market_watchlist_excludes_and_rotates():
    """Market mode: excluded symbols never appear when bybit returns filtered universe."""
    filtered_universe = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"] * 10
    with patch("bybit.get_top_linear_usdt_symbols") as mock_top:
        mock_top.return_value = filtered_universe
        with patch("bybit.get_last_topn_excluded_counts") as mock_counts:
            mock_counts.return_value = {"prefix": 5, "symbol": 2, "regex": 0}
            from watchlist import get_watchlist
            import types
            config = types.SimpleNamespace(
                WATCHLIST_MODE="market",
                WATCHLIST=["BTCUSDT"],
                WATCHLIST_UNIVERSE_N=50,
                WATCHLIST_BATCH_N=10,
                WATCHLIST_REFRESH_SECONDS=600,
                WATCHLIST_ROTATE_MODE="roundrobin",
                WATCHLIST_ROTATE_SEED=0,
                WATCHLIST_MIN_PRICE=0.01,
                WATCHLIST_MIN_TURNOVER_24H=0.0,
                WATCHLIST_EXCLUDE_PREFIXES=["1000", "10000"],
                WATCHLIST_EXCLUDE_SYMBOLS=["HYPEUSDT"],
                WATCHLIST_EXCLUDE_REGEX="",
                WATCHLIST_MAX_SPREAD_BPS=0.0,
            )
            symbols, mode = get_watchlist(config)
            assert mode == "market"
            assert "HYPEUSDT" not in symbols
            assert not any(s.startswith("1000") for s in symbols)
