### Scalper PATCH_BASELINE

This file documents the current canonical runtime paths and guardrails before applying larger trading-system patches. It is **descriptive only** and does not change behavior.

---

### Canonical runtime commands

- **Primary scanner entrypoint (paper / DRY RUN only)**:
  - `python -m scalper.scanner --once --paper`
  - `python -m scalper.scanner --loop --paper`
- **Formatter self-test (no Telegram send, stdout only)**:
  - `python -m scalper.scanner --test-telegram-formats`

These commands must continue to work unchanged after future patches.

---

### Canonical modules and responsibilities

- **Scanner / orchestration**
  - `scalper.scanner`
    - CLI argument parsing and `main()`.
    - Resolves watchlist and drives `run_scan_cycle(...)`.
    - Wires together:
      - market data (`scalper.bybit.fetch_klines`)
      - signal engines (`scalper.signals`, `scalper.strategy_engine`)
      - risk engine (`scalper.risk_engine_core.RiskEngine`)
      - trade preview (`scalper.trade_preview.build_trade_preview`)
      - paper execution (`scalper.paper_broker.PaperBroker` and/or `scalper.paper_engine.try_open_position`)
      - dashboard + Telegram formatting (`scalper.telegram_format`, `scalper.notifier`).

- **Signal generation**
  - `scalper.signals`
    - Legacy signal generation utilities (EMA/ATR/MACD, higher timeframe context).
    - Used by the scanner for non-v3 strategy paths and reconciliation tooling.
  - `scalper.strategy_engine` (not enumerated here, but invoked from `scalper.scanner`).
    - V3 trade intent computation (bias-aware, near-miss candidates, etc.).

- **Trade preview / levels (single source of truth)**
  - `scalper.trade_preview.build_trade_preview`
    - Given a validated intent + market snapshot (+ optional MTF snapshot), computes:
      - entry, SL, TP levels
      - ATR source and magnitude
      - level geometry validation and RR filter
      - ATR% and retest drift constraints
      - suggested notional / quantity for previews.
    - **All trade plans and paper executions must continue to flow through this function.**

- **Paper execution**
  - `scalper.paper_broker.PaperBroker`
    - Current canonical paper execution engine when `paper_mode=True` in the scanner.
    - Applies slippage and fees, sizes position from risk config, and persists positions/trades via a store interface (`load_paper_state`, `upsert_paper_position`, `insert_paper_trade`, `delete_paper_position`).
  - `scalper.paper_engine`
    - Legacy paper engine and helper functions (including a preview-driven open path).
    - Still used as a fallback in some scanner code paths; treated as **compatibility layer**.
  - `scalper.paper` / `PaperPosition`
    - Position modeling and mark-to-market updates (`update_and_maybe_close`).

- **Telegram formatting**
  - `scalper.telegram_format`
    - Message formatting only; no network I/O.
    - Key functions: `format_intent_allow`, `format_intent_block`, `format_paper_open`, `format_paper_close`, `format_early_alert`, dashboard formatting helpers.
  - `scalper.notifier`
    - Policy-based Telegram routing and logging.
    - All Telegram sending is behind `send_telegram_with_logging(...)` and respects `TELEGRAM_POLICY`.

- **Settings / config**
  - `scalper.settings`
    - Pydantic-backed settings model.
    - Responsible for loading `.env`, normalizing and validating risk / bybit / telegram / dashboard configuration.
  - `config` (top-level module)
    - Thin, compatibility shim over `scalper.settings`.
    - Exposes constants such as:
      - Bybit: `BYBIT_BASE_URL`, `REQUEST_SLEEP_MS`, `EXECUTION_MODE`, `EXPLICIT_CONFIRM_EXECUTION`.
      - Risk: `INTERVAL`, `LOOKBACK`, `SCAN_SECONDS`, `WATCHLIST_*`, `PAPER_*`, `RISK_PER_TRADE_PCT`, `MAX_OPEN_POSITIONS`, `RISK_KILL_SWITCH`, `KILL_SWITCH`, and v3 strategy knobs.
      - Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_POLICY`, `TELEGRAM_COMPACT`, `TELEGRAM_EARLY_ENABLED`, limits and formatting caps.

- **Market data (public-only)**
  - `scalper.bybit`
    - Uses public Bybit endpoints for klines and tickers (`/v5/market/kline`, `/v5/market/tickers`).
    - No private account or order endpoints are called from the scanner runtime.

---

### Protected feature flags and safety switches

The following flags / settings are considered **safety-critical** and must not be silently changed or bypassed:

- **Execution / kill switches**
  - `config.EXECUTION_MODE`
  - `config.EXPLICIT_CONFIRM_EXECUTION`
  - `config.RISK_KILL_SWITCH`
  - `config.KILL_SWITCH`

- **Risk limits and sizing**
  - `config.PAPER_EQUITY_USDT`
  - `config.PAPER_POSITION_USDT`
  - `config.RISK_PER_TRADE_PCT`
  - `config.MAX_OPEN_POSITIONS`
  - `config.RISK_MAX_TRADES_PER_DAY`
  - `config.RISK_MAX_DAILY_LOSS_SIM`
  - `config.RISK_MAX_CONSECUTIVE_LOSSES`
  - `config.RISK_COOLDOWN_MINUTES`
  - `config.MAX_SYMBOL_NOTIONAL_PCT`
  - Cluster / symbol constraints (e.g. `config.CLUSTER_BTC_ETH_LIMIT`).

- **Paper execution & pricing**
  - `config.PAPER_FEES_BPS`, `config.PAPER_SLIPPAGE_PCT`, `config.PAPER_FEE_PCT`
  - `config.SPREAD_BPS`, `config.SLIPPAGE_BPS`
  - Any sizing or slippage logic in `scalper.paper_broker.PaperBroker` and `scalper.paper_engine`.

- **Notification behavior**
  - `config.TELEGRAM_POLICY`
  - `config.NOTIFY_BLOCKED`, `config.RISK_NOTIFY_BLOCKED_TELEGRAM`
  - `config.NOTIFY_SCAN_SUMMARY`, `config.DISABLE_SCAN_SUMMARY`
  - `config.TELEGRAM_SEND_DASHBOARD`

Changes to these items must be explicit and validated; they must never be altered by accident during refactors.

---

### Legacy-path dangers (do not rely on these for new work)

- There are **duplicate copies** of several runtime-critical modules under nested `scalper/` directories (e.g. `scalper/scalper/scalper/scanner.py`, `paper_engine.py`, `paper_broker.py`, `settings.py`, `config.py`), some of which contain Git merge markers (`<<<<<<< HEAD`, `>>>>>>>`).
- The canonical modules described above are the ones resolved by:
  - `python -m scalper.scanner`
  - `import config`
  - `import scalper.scanner`
- These nested/duplicate files are treated as **legacy artifacts and compatibility shims only**. Future patches **must not**:
  - Introduce new behavior that depends on the conflicting copies.
  - Change their semantics in ways that affect the canonical runtime without a deliberate, tested migration.

The validation harness (`tools/validate_runtime.py`) can be used to surface these duplicates and conflict markers without modifying them.

---

### Validation scaffolding

To guard the current runtime during future patches, use:

- **Lightweight runtime validation script**:
  - `python tools/validate_runtime.py --verbose`
    - Verifies core imports (`config`, `scalper.scanner`, `scalper.signals`, `scalper.trade_preview`, `scalper.paper_engine`, `scalper.paper_broker`, `scalper.telegram_format`, `scalper.bybit`, `scalper.settings`).
    - Checks that settings parse and expose `risk` / `telegram` / `bybit`.
    - Confirms `scalper.scanner.run_scan_cycle` still accepts a `paper_mode` flag.
    - Runs `run_test_telegram_formats(config)` internally (formatter smoke test).
    - Scans the `scalper` package tree for:
      - duplicate critical modules (e.g. multiple `scanner.py`, `paper_engine.py`)
      - files with unresolved merge conflict markers.

- **Optional CLI smoke test wrapper**:
  - `python tools/validate_runtime.py --external-cli --verbose`
    - In addition to the above, runs:
      - `python -m scalper.scanner --test-telegram-formats`
    - This ensures the module-level CLI entrypoint remains wired correctly.

These validation commands are non-invasive: they do **not** enable live trading, and they exercise only DRY RUN / paper paths and formatter logic.

