# CLI and Output Contracts

This document defines stable contracts for backtesting/analysis scripts in this repo:

- `scripts/replay.py`
- `scripts/candle_cache.py` (cache behavior used by replay/warmup)
- `scripts/warm_cache.py`
- `scripts/walkforward.py`

## `scripts/replay.py` Contract

### CLI inputs

Supported flags:

- `--symbols` (comma-separated, normalized to uppercase, no whitespace)
- `--days`
- `--tf-trigger`
- `--tf-timing`
- `--tf-bias`
- `--tf-setup`
- `--step-bars`
- `--no-cache`
- `--cache-only`
- `--cache-days`
- `--end-days-ago`
- `--start-days-ago`

Behavior notes:

- `--cache-only` implies `use_cache=True`.
- If both `--start-days-ago` and `--end-days-ago` are set, effective days are computed as:
  - `days = start_days_ago - end_days_ago`
  - must be `> 0`, otherwise process exits with code `2`.

### Run signature and deterministic `run_id`

Replay builds an in-memory `RUN_SIGNATURE` object and computes:

- `run_id = sha256(json.dumps(RUN_SIGNATURE, sort_keys=True)).hexdigest()[:12]`

When not silent, replay logs:

- `RUN_SIGNATURE[<run_id>]: <json>`

Contract:

- identical effective inputs/config produce the same `run_id`;
- `run_id` is 12 lowercase hex chars;
- output filenames are keyed by this `run_id`.

### Replay CSV output contract

Output path:

- `data/replay_trades_<run_id>.csv`

CSV columns (fixed order):

1. `ts_entry`
2. `ts_exit`
3. `symbol`
4. `setup`
5. `side`
6. `entry`
7. `sl`
8. `tp`
9. `exit_price`
10. `qty`
11. `pnl_usdt`
12. `risk_usdt`
13. `r_multiple`
14. `risk_status`
15. `exit_reason`
16. `partial`

### Replay summary JSON contract

Output path:

- `data/replay_summary_<run_id>.json`

JSON payload shape:

- `run_id`: string
- `overall`: object (KPI aggregate from `compute_paper_kpis`)
- `by_setup`: object keyed by setup name; values are KPI objects
- `exit_reason_counts`: object of `{reason: count}`
- `skip_reasons`: object of `{reason: count}`
- `risk_invalid_count`: integer

Notes:

- If `V3_TREND_BREAKOUT` is enabled and no V3 trades exist, `by_setup["V3_TREND_BREAKOUT"]` is still emitted with zeroed KPI fields.
- In hard exit mode (`REPLAY_EXIT_MODE=hard`), `exit_reason_counts` includes only `SL`, `TP`, `END_OF_DATA`.

## Candle Cache Contract (`scripts/candle_cache.py`)

### On-disk layout

Cache root:

- `data/candles/<SYMBOL>/<TF_MIN>.csv`

Example:

- `data/candles/BTCUSDT/15.csv`

CSV columns:

- `ts`, `open`, `high`, `low`, `close`, `volume`

Properties:

- rows are sorted by ascending timestamp;
- cache writes are skipped for too-small datasets (`< 5` candles);
- cache is trimmed to `cache_days` relative to the current request end.

### Read contract (`get_candles`)

`get_candles(...)` returns list entries with fields:

- `timestamp` (ms)
- `timestamp_utc` (ISO UTC string)
- `open`, `high`, `low`, `close`, `volume` (float-like values)
- `ts` alias is guaranteed on output (same as `timestamp`)

### `cache_only` behavior contract

When `cache_only=True`:

- no API fetch is performed;
- if cache file is missing/empty, it raises `RuntimeError`;
- if requested range is outside cached range:
  - small boundary gaps are clamped if gap bars `<= CACHE_ONLY_GAP_BARS_MAX` (default `12`);
  - larger gaps raise `RuntimeError`.

Coverage rule used for full-hit logic allows 1-bar tolerance on boundaries.

## `scripts/warm_cache.py` Contract

Purpose:

- download/fill candle cache only (no strategy evaluation, no trade outputs).

CLI flags:

- `--symbols`
- `--days`
- `--tfs` (comma-separated TF minutes, defaults to `240,60,15,5`)
- `--use-cache` (`true/false` style parser)

Behavior:

- computes `[start_ms, end_ms]` from `days`;
- for each symbol/TF calls `candle_cache.get_candles(..., cache_only=False)`;
- logs per pair:
  - `WARM symbol=<...> tf=<...> bars=<...> covered=<true|false> source=<cache|api|mixed> elapsed_s=<...>`.

## `scripts/walkforward.py` Contract

### CLI inputs

- `--symbols`
- `--train-days`
- `--test-days`
- `--step-days`
- `--start-days-ago`

### Windowing contract

For each window index `i`:

- `test_start_days_ago = start_days_ago - train_days - i * step_days`
- `test_end_days_ago = test_start_days_ago - test_days`
- iteration stops when `test_end_days_ago < 0`
- windows are emitted oldest-first.

### Replay invocation contract

Each window runs `scripts/replay.py` as a subprocess with:

- `--start-days-ago <test_start_days_ago>`
- `--end-days-ago <test_end_days_ago>`
- fixed TFs (`15/5/240/60`) and `--step-bars 1`

Environment is pinned to V2-only/TP_R-style exits:

- `STRATEGY_V1=0`
- `V2_TREND_PULLBACK=1`
- `V1_SETUP_BREAKOUT=0`
- `V1_SETUP_TRAP=0`
- `BE_AT_R=0`
- `PARTIAL_TP_AT_R=0`
- `TRAIL_AFTER_R=0`

Walkforward extracts replay `run_id` from replay logs (prefer summary export line, fallback to `RUN_SIGNATURE[...]`).

### Walkforward outputs

Walkforward run id:

- `run_id = sha256(json.dumps({symbols, train_days, test_days, step_days, start_days_ago}, sort_keys=True)).hexdigest()[:12]`

Output files:

- `data/walkforward_results_<run_id>.csv`
- `data/walkforward_results_<run_id>.json`

CSV columns (fixed order):

1. `window`
2. `test_end_days_ago`
3. `test_start_days_ago`
4. `run_id` (replay run id for that window)
5. `trades_total`
6. `wins`
7. `losses`
8. `winrate`
9. `expectancy_R`
10. `profit_factor`
11. `avg_win_R`
12. `avg_loss_R`
13. `max_dd_usdt`
14. `top_skip_reason`

JSON structure:

- `run_id`: walkforward run id
- `args`: resolved walkforward args
- `windows`: array of per-window records (includes KPI fields and diagnostic objects like `skip_reasons`, `exit_reason_counts`, `by_setup`)

## Strategy / Risk Engine Contract (Plugin Compatibility)

### TradeIntent (canonical structure)

Required fields for RiskEngine.evaluate():

- `symbol`: str (non-empty, normalized uppercase)
- `side`: str (`LONG` or `SHORT`)
- `strategy_id`: str (or `strategy` / `setup` alias)
- `timeframe`: str (e.g. `"15"`)
- `bar_ts`: str (or `candle_ts` / `ts` alias)

Optional:

- `entry`, `sl`, `tp`: float
- `confidence`: float
- `debug`: dict
- `meta`: dict (for sl_hint, tp_r_mult, etc.)

Validation: `validate_trade_intent(intent)` returns `(ok, reason)`. If invalid, RiskEngine returns BLOCK with reason `INTENT_MISSING_SYMBOL`, `INTENT_MISSING_OR_INVALID_SIDE`, `INTENT_MISSING_STRATEGY_ID`, `INTENT_MISSING_TIMEFRAME`, or `INTENT_MISSING_BAR_TS`.

### StrategyResult (plugin return shape)

Strategies implement `evaluate(symbol, context) -> StrategyResult`:

- `ok`: bool
- `side`: Optional[str]
- `reason`: str (always set when ok=False; never empty for blocks)
- `entry`, `sl`, `tp`: Optional[float]
- `debug`: dict (may contain `evaluated` with `final_intents`)
- `intent`: Optional[TradeIntent] (when ok=True and intent available)

When `ok=True`, `debug.evaluated.final_intents` should contain at least one dict with required TradeIntent fields. The registry enriches with `timeframe` and `bar_ts` from context when building intent.

### Adding a new strategy

1. Create a module in `scalper/strategies/` implementing the Strategy protocol.
2. Implement `evaluate(symbol, context) -> StrategyResult`.
3. When a signal triggers, return `StrategyResult(ok=True, side=..., debug={"evaluated": {"final_intents": [intent_dict], "market_snapshot": {...}}})`.
4. Ensure each intent_dict has: `symbol`, `side`, `strategy` (or `strategy_id`), and optionally `entry`, `sl`, `tp`, `confidence`. The registry adds `timeframe` and `bar_ts` from context.
5. Register in `scalper/strategies/registry.py` `available_strategies()`.

No silent failures: every RiskEngine block has an explicit `reason` string.
