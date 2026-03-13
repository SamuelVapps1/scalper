from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _ticker(
    symbol: str,
    *,
    turnover: float = 5_000_000,
    last_price: float = 10.0,
    high_price: float = 11.0,
    low_price: float = 9.0,
) -> dict:
    return {
        "symbol": symbol,
        "quoteCoin": "USDT",
        "status": "Trading",
        "turnover24h": str(turnover),
        "lastPrice": str(last_price),
        "highPrice24h": str(high_price),
        "lowPrice24h": str(low_price),
    }


def _patch_in_memory_watchlist_state(monkeypatch):
    storage = {"state": {}, "offset": 0}

    def _get_state():
        return dict(storage["state"])

    def _set_state(state):
        storage["state"] = dict(state or {})

    def _get_offset():
        return int(storage["offset"])

    def _set_offset(offset):
        storage["offset"] = int(offset or 0)

    monkeypatch.setattr("watchlist.get_watchlist_rotation_state", _get_state)
    monkeypatch.setattr("watchlist.set_watchlist_rotation_state", _set_state)
    monkeypatch.setattr("watchlist.get_watchlist_rotation_offset", _get_offset)
    monkeypatch.setattr("watchlist.set_watchlist_rotation_offset", _set_offset)
    monkeypatch.setattr(
        "watchlist.set_watchlist_transparency",
        lambda *args, **kwargs: None,
    )
    return storage


def _market_cfg(**overrides):
    base = dict(
        WATCHLIST_MODE="market",
        WATCHLIST=["BTCUSDT", "ETHUSDT"],
        WATCHLIST_UNIVERSE_N=200,
        WATCHLIST_BATCH_N=20,
        WATCHLIST_REFRESH_SECONDS=900,
        WATCHLIST_ROTATE_MODE="roundrobin",
        WATCHLIST_ROTATE_SEED=0,
        WATCHLIST_MIN_TURNOVER_24H=2_000_000.0,
        MIN_VOL_PCT=0.2,
        MAX_VOL_PCT=25.0,
        WATCHLIST_MIN_PRICE="",
        WATCHLIST_EXCLUDE_PREFIXES=[],
        WATCHLIST_EXCLUDE_SYMBOLS=[],
        WATCHLIST_EXCLUDE_REGEX="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_roundrobin_rotation_persists_across_instances(monkeypatch):
    from watchlist import WatchlistManager

    _patch_in_memory_watchlist_state(monkeypatch)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT"]
    rows = [_ticker(sym, turnover=10_000_000 - i * 10_000) for i, sym in enumerate(symbols)]
    monkeypatch.setattr("bybit.get_linear_tickers", lambda: rows)
    cfg = _market_cfg(WATCHLIST_UNIVERSE_N=6, WATCHLIST_BATCH_N=2)

    manager_a = WatchlistManager(cfg)
    batch_1 = manager_a.get_current_watchlist(now_ts=1_000_000)
    batch_2 = manager_a.get_current_watchlist(now_ts=1_000_001)

    manager_b = WatchlistManager(cfg)
    batch_3 = manager_b.get_current_watchlist(now_ts=1_000_002)

    assert batch_1 == ["BTCUSDT", "ETHUSDT"]
    assert batch_2 == ["SOLUSDT", "XRPUSDT"]
    assert batch_3 == ["ADAUSDT", "AVAXUSDT"]


def test_market_universe_n_and_batch_n(monkeypatch):
    from watchlist import WatchlistManager

    _patch_in_memory_watchlist_state(monkeypatch)
    universe = [f"SYM{i}USDT" for i in range(10)]
    cfg = _market_cfg(WATCHLIST_UNIVERSE_N=5, WATCHLIST_BATCH_N=3, WATCHLIST_MIN_TURNOVER_24H=0.0)
    monkeypatch.setattr(WatchlistManager, "_fetch_universe", lambda self: list(universe[:5]))

    manager = WatchlistManager(cfg)
    batch = manager.get_current_watchlist(now_ts=2_000_000)

    assert len(batch) == 3
    assert batch == ["SYM0USDT", "SYM1USDT", "SYM2USDT"]


def test_market_filters_turnover_vol_and_price(monkeypatch):
    from watchlist import WatchlistManager

    _patch_in_memory_watchlist_state(monkeypatch)
    rows = [
        _ticker("GOODUSDT", turnover=5_000_000, last_price=100, high_price=110, low_price=95),
        _ticker("LOWTURNUSDT", turnover=10_000, last_price=100, high_price=110, low_price=95),
        _ticker("LOWVOLUSDT", turnover=5_000_000, last_price=100, high_price=100.05, low_price=99.95),
        _ticker("HIGHVOLUSDT", turnover=5_000_000, last_price=100, high_price=200, low_price=1),
        _ticker("LOWPRICEUSDT", turnover=5_000_000, last_price=0.001, high_price=0.002, low_price=0.0005),
    ]
    monkeypatch.setattr("bybit.get_linear_tickers", lambda: rows)
    cfg = _market_cfg(
        WATCHLIST_UNIVERSE_N=20,
        WATCHLIST_BATCH_N=20,
        WATCHLIST_MIN_TURNOVER_24H=2_000_000.0,
        MIN_VOL_PCT=0.2,
        MAX_VOL_PCT=25.0,
        WATCHLIST_MIN_PRICE="1",
    )

    manager = WatchlistManager(cfg)
    batch = manager.get_current_watchlist(now_ts=3_000_000)

    assert batch == ["GOODUSDT"]

