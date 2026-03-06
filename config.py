<<<<<<< HEAD
import os
from typing import List

from dotenv import load_dotenv

load_dotenv()

BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")

WATCHLIST_RAW = os.getenv("WATCHLIST", "")
WATCHLIST: List[str] = [s.strip().upper() for s in WATCHLIST_RAW.split(",") if s.strip()]

INTERVAL = os.getenv("INTERVAL", "15")
LOOKBACK = int(os.getenv("LOOKBACK", "300"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "60"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", os.getenv("CHAT_ID", ""))
TELEGRAM_EXIT_ALERTS = os.getenv("TELEGRAM_EXIT_ALERTS", "true").lower() in ("1", "true", "yes")
=======
"""
Temporary compatibility shim.

Prefer `from scalper.settings import get_settings`.
"""

from __future__ import annotations

from scalper.settings import _ENV_BOOTSTRAP_STATE, debug_env, debug_risk_config, get_settings

_s = get_settings()

BYBIT_BASE_URL = _s.bybit.base_url
REQUEST_SLEEP_MS = _s.bybit.request_sleep_ms
EXECUTION_MODE = _s.bybit.execution_mode
EXPLICIT_CONFIRM_EXECUTION = _s.bybit.explicit_confirm_execution

TELEGRAM_BOT_TOKEN = _s.telegram.bot_token
TELEGRAM_CHAT_ID = _s.telegram.chat_id
TELEGRAM_FORMAT = _s.telegram.format
TELEGRAM_MAX_CHARS_COMPACT = _s.telegram.max_chars_compact
TELEGRAM_MAX_CHARS_VERBOSE = _s.telegram.max_chars_verbose
TELEGRAM_SEND_BLOCKED = _s.telegram.send_blocked
TELEGRAM_SEND_DASHBOARD = _s.telegram.send_dashboard
TELEGRAM_POLICY = _s.telegram.policy
TELEGRAM_COMPACT = _s.telegram.compact
TELEGRAM_EARLY_ENABLED = _s.telegram.early_enabled
TELEGRAM_EARLY_MAX_PER_SYMBOL_PER_15M = _s.telegram.early_max_per_symbol_per_15m

INTERVAL = _s.risk.interval
LOOKBACK = _s.risk.lookback
SCAN_SECONDS = _s.risk.scan_seconds
SCAN_CYCLE_TIMEOUT_SECONDS = _s.risk.scan_cycle_timeout_seconds
WATCHLIST = _s.risk.watchlist
WATCHLIST_MODE = _s.risk.watchlist_mode
WATCHLIST_TOP_N = _s.risk.watchlist_top_n
WATCHLIST_REFRESH_MINUTES = _s.risk.watchlist_refresh_minutes
WATCHLIST_REFRESH_MIN = _s.risk.watchlist_refresh_min
WATCHLIST_MIN_PRICE = _s.risk.watchlist_min_price
WATCHLIST_MIN_TURNOVER_24H = _s.risk.watchlist_min_turnover_24h
MIN_TURNOVER_USDT = _s.risk.min_turnover_usdt
WATCHLIST_EXCLUDE_PREFIXES = _s.risk.watchlist_exclude_prefixes
WATCHLIST_EXCLUDE_SYMBOLS = _s.risk.watchlist_exclude_symbols
WATCHLIST_EXCLUDE_REGEX = _s.risk.watchlist_exclude_regex
WATCHLIST_MAX_SPREAD_BPS = _s.risk.watchlist_max_spread_bps
MIN_VOL_PCT = _s.risk.min_vol_pct
MAX_VOL_PCT = _s.risk.max_vol_pct
WATCHLIST_POOL_N = _s.risk.watchlist_pool_n
WATCHLIST_RANK = _s.risk.watchlist_rank
POSITION_MODE = _s.risk.position_mode
MAX_CONCURRENT_POSITIONS = _s.risk.max_concurrent_positions
MAX_OPEN_POSITIONS = _s.risk.max_open_positions
RISK_NOTIFY_BLOCKED_TELEGRAM = _s.risk.risk_notify_blocked_telegram
RISK_KILL_SWITCH = _s.risk.risk_kill_switch
KILL_SWITCH = _s.risk.kill_switch
RISK_MAX_TRADES_PER_DAY = _s.risk.risk_max_trades_per_day
RISK_MAX_DAILY_LOSS_SIM = _s.risk.risk_max_daily_loss_sim
RISK_MAX_CONSECUTIVE_LOSSES = _s.risk.risk_max_consecutive_losses
RISK_COOLDOWN_MINUTES = _s.risk.risk_cooldown_minutes
RISK_ONE_POSITION_PER_SYMBOL = _s.risk.risk_one_position_per_symbol
SIGNAL_DEBUG = _s.risk.signal_debug
KPI_DEBUG = _s.risk.kpi_debug
NOTIFY_BLOCKED = _s.risk.notify_blocked
ALWAYS_NOTIFY_INTENTS = _s.risk.always_notify_intents
HEARTBEAT_MINUTES = _s.risk.heartbeat_minutes
NOTIFY_SCAN_SUMMARY = _s.risk.notify_scan_summary
DISABLE_SCAN_SUMMARY = _s.risk.disable_scan_summary
THRESHOLD_PROFILE = _s.risk.threshold_profile
EARLY_ENABLED = _s.risk.early_enabled
EARLY_TF = _s.risk.early_tf
EARLY_LOOKBACK_5M = _s.risk.early_lookback_5m
EARLY_MIN_CONF = _s.risk.early_min_conf
EARLY_REQUIRE_15M_CONTEXT = _s.risk.early_require_15m_context
EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M = _s.risk.early_max_alerts_per_symbol_per_15m
TF_BIAS = _s.risk.tf_bias
TF_SETUP = _s.risk.tf_setup
TF_TRIGGER = _s.risk.tf_trigger
TF_TIMING = _s.risk.tf_timing
LOOKBACK_4H = _s.risk.lookback_4h
LOOKBACK_1H = _s.risk.lookback_1h
LOOKBACK_15M = _s.risk.lookback_15m
LOOKBACK_5M = _s.risk.lookback_5m
PAPER_POSITION_USDT = _s.risk.paper_position_usdt
PAPER_FEES_BPS = _s.risk.paper_fees_bps
PAPER_EQUITY_USDT = _s.risk.paper_equity_usdt
PAPER_TIMEOUT_BARS = _s.risk.paper_timeout_bars
PAPER_SL_ATR = _s.risk.paper_sl_atr
PAPER_TP_ATR = _s.risk.paper_tp_atr
PAPER_START_EQUITY_USDT = _s.risk.paper_start_equity_usdt
PAPER_SLIPPAGE_PCT = _s.risk.paper_slippage_pct
PAPER_FEE_PCT = _s.risk.paper_fee_pct
SPREAD_BPS = _s.risk.spread_bps
SLIPPAGE_BPS = _s.risk.slippage_bps
RISK_PER_TRADE_PCT = _s.risk.risk_per_trade_pct
DAILY_LOSS_LIMIT_PCT = _s.risk.daily_loss_limit_pct
MAX_DD_PCT = _s.risk.max_dd_pct
MAX_TRADES_DAY = _s.risk.max_trades_day
MIN_SECONDS_BETWEEN_TRADES = _s.risk.min_seconds_between_trades
MIN_SECONDS_BETWEEN_SYMBOL_TRADES = _s.risk.min_seconds_between_symbol_trades
MAX_SYMBOL_NOTIONAL_PCT = _s.risk.max_symbol_notional_pct
CLUSTER_BTC_ETH_LIMIT = _s.risk.cluster_btc_eth_limit
FAIL_CLOSED_ON_SNAPSHOT_MISSING = _s.risk.fail_closed_on_snapshot_missing

STRATEGY_V1 = _s.strategy_v3.strategy_v1
V1_SETUP_BREAKOUT = _s.strategy_v3.v1_setup_breakout
V1_SETUP_TRAP = _s.strategy_v3.v1_setup_trap
V2_TREND_PULLBACK = _s.strategy_v3.v2_trend_pullback
V3_TREND_BREAKOUT = _s.strategy_v3.v3_trend_breakout
DONCHIAN_N_15M = _s.strategy_v3.donchian_n_15m
BODY_ATR_15M = _s.strategy_v3.body_atr_15m
TREND_SEP_ATR_1H = _s.strategy_v3.trend_sep_atr_1h
USE_5M_CONFIRM = _s.strategy_v3.use_5m_confirm
PULLBACK_TOL_ATR = _s.strategy_v3.pullback_tol_atr
TREND_MIN_SEP_ATR = _s.strategy_v3.trend_min_sep_atr
MOMO_MIN_BODY_ATR_5M = _s.strategy_v3.momo_min_body_atr_5m
RETEST_CONFIRM_MODE = _s.strategy_v3.retest_confirm_mode
BOS_LOOKBACK_5M = _s.strategy_v3.bos_lookback_5m
BREAKOUT_STRONG_MARKET = _s.strategy_v3.breakout_strong_market
BREAKOUT_STRONG_BODY_PCT = _s.strategy_v3.breakout_strong_body_pct
BREAKOUT_BUFFER_ATR = _s.strategy_v3.breakout_buffer_atr
TRAP_MIN_WICK_ATR = _s.strategy_v3.trap_min_wick_atr
REQUIRE_1H_EMA200_ALIGN = _s.strategy_v3.require_1h_ema200_align
REQUIRE_5M_EMA20_CONFIRM = _s.strategy_v3.require_5m_ema20_confirm
MIN_ATR_PCT_15M = _s.strategy_v3.min_atr_pct_15m
MAX_ATR_PCT_15M = _s.strategy_v3.max_atr_pct_15m
LOG_V3_TRIGGERS = _s.strategy_v3.log_v3_triggers

BE_AT_R = _s.replay.be_at_r
PARTIAL_TP_AT_R = _s.replay.partial_tp_at_r
TRAIL_AFTER_R = _s.replay.trail_after_r
REPLAY_EXIT_MODE = _s.replay.replay_exit_mode
REPLAY_PROGRESS_EVERY = _s.replay.replay_progress_every

CANDLES_CACHE_TTL_SECONDS = _s.cache.candles_cache_ttl_seconds
CACHE_ONLY_GAP_BARS_MAX = _s.cache.cache_only_gap_bars_max

DASHBOARD_HOST = _s.dashboard.host
DASHBOARD_PORT = _s.dashboard.port
DASHBOARD_TELEGRAM = _s.dashboard.telegram
DASHBOARD_TOP_N = _s.dashboard.top_n
DASHBOARD_INCLUDE_BLOCKED = _s.dashboard.include_blocked
DASHBOARD_INCLUDE_MARKET_SNAPSHOT = _s.dashboard.include_market_snapshot
DASHBOARD_INCLUDE_DEBUG_WHY_NONE = _s.dashboard.include_debug_why_none
>>>>>>> 4cfdd8fe6584fa7b2772b45743f088df40182329
