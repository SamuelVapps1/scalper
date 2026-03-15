from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _static_watchlist(config: Any) -> List[str]:
    raw = getattr(config, "WATCHLIST", []) or []
    if isinstance(raw, str):
        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    else:
        symbols = [str(s).strip().upper() for s in raw if str(s).strip()]
    return list(dict.fromkeys(symbols))


def _state_path(config: Any) -> Path:
    raw = str(getattr(config, "ROTATION_STATE_FILE", "data/watchlist_rotation_state.json") or "").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_state(config: Any) -> Dict[str, Any]:
    path = _state_path(config)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(config: Any, state: Dict[str, Any]) -> None:
    path = _state_path(config)
    path.write_text(json.dumps(state, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def _rotate_round_robin(pool: List[str], batch_n: int, offset: int) -> Tuple[List[str], int]:
    if not pool:
        return [], 0
    n = min(max(1, int(batch_n)), len(pool))
    start = offset % len(pool)
    selected = [pool[(start + i) % len(pool)] for i in range(n)]
    return selected, (start + n) % len(pool)


class WatchlistManager:
    def __init__(self, config: Any, logger: Optional[logging.Logger] = None):
        self.config = config
        self.log = logger or logging.getLogger(__name__)

    def _fetch_market_universe(self) -> List[str]:
        from bybit import get_top_linear_usdt_symbols

        universe_n = int(getattr(self.config, "WATCHLIST_UNIVERSE_N", 200) or 200)
        min_price = float(getattr(self.config, "WATCHLIST_MIN_PRICE", 0.0) or 0.0)
        min_turnover = float(getattr(self.config, "WATCHLIST_MIN_TURNOVER_24H", 0.0) or 0.0)
        exclude_prefixes = list(getattr(self.config, "WATCHLIST_EXCLUDE_PREFIXES", []) or [])
        exclude_symbols = list(getattr(self.config, "WATCHLIST_EXCLUDE_SYMBOLS", []) or [])
        exclude_regex = str(getattr(self.config, "WATCHLIST_EXCLUDE_REGEX", "") or "")
        max_spread_bps = float(getattr(self.config, "WATCHLIST_MAX_SPREAD_BPS", 0.0) or 0.0)
        symbols = get_top_linear_usdt_symbols(
            universe_n,
            min_price=min_price,
            min_turnover_24h=min_turnover,
            exclude_prefixes=exclude_prefixes,
            exclude_symbols=exclude_symbols,
            exclude_regex=exclude_regex,
            max_spread_bps=max_spread_bps,
        )
        return list(dict.fromkeys([str(s).strip().upper() for s in symbols if str(s).strip()]))

    def get_watchlist(self) -> Tuple[List[str], str]:
        mode = str(getattr(self.config, "WATCHLIST_MODE", "static") or "static").strip().lower()
        if mode not in {"market", "dynamic"}:
            static_syms = _static_watchlist(self.config)
            self.log.info(
                "WATCHLIST source=static candidates=%d selected=%d",
                len(static_syms),
                len(static_syms),
            )
            return static_syms, "static"

        state = _load_state(self.config)
        now = int(time.time())
        refresh_seconds = max(1, int(getattr(self.config, "WATCHLIST_REFRESH_SECONDS", 600) or 600))
        universe: List[str] = list(state.get("universe", []) or [])
        source = "market"
        if (not universe) or (((int(state.get("universe_ts", 0) or 0) + refresh_seconds) <= now)):
            try:
                universe = self._fetch_market_universe()
                state["universe"] = universe
                state["universe_ts"] = now
            except Exception as exc:
                static_syms = _static_watchlist(self.config)
                self.log.warning("WATCHLIST market fetch failed, fallback static: %s", exc)
                self.log.info(
                    "WATCHLIST source=static candidates=%d selected=%d",
                    len(static_syms),
                    len(static_syms),
                )
                return static_syms, "static"

        if not universe:
            static_syms = _static_watchlist(self.config)
            self.log.info(
                "WATCHLIST source=static candidates=%d selected=%d",
                len(static_syms),
                len(static_syms),
            )
            return static_syms, "static"

        rotate_mode = str(getattr(self.config, "WATCHLIST_ROTATE_MODE", "roundrobin") or "roundrobin").lower()
        batch_n = int(getattr(self.config, "WATCHLIST_BATCH_N", 20) or 20)
        seed = int(getattr(self.config, "WATCHLIST_ROTATE_SEED", 0) or 0)

        if rotate_mode == "seeded_random":
            rnd = random.Random(seed + now // refresh_seconds)
            shuffled = list(universe)
            rnd.shuffle(shuffled)
            selected = shuffled[: min(len(shuffled), max(1, batch_n))]
            next_offset = int(state.get("offset", 0) or 0)
        else:
            selected, next_offset = _rotate_round_robin(universe, batch_n, int(state.get("offset", seed) or 0))
            rotate_mode = "roundrobin"

        state["offset"] = next_offset
        state["last_mode"] = rotate_mode
        _save_state(self.config, state)

        self.log.info(
            "WATCHLIST source=%s candidates=%d selected=%d",
            source,
            len(universe),
            len(selected),
        )
        return selected, "market"


def get_watchlist(config: Any, bybit_client: Optional[Any] = None, logger: Optional[Any] = None):
    _ = bybit_client
    return WatchlistManager(config=config, logger=logger).get_watchlist()


__all__ = ["WatchlistManager", "get_watchlist"]
