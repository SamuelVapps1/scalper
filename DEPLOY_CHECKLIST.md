### Scalper Deployment Checklist

This checklist defines the safe path from **paper-only validation** to optional **testnet** and **live** execution. All steps are incremental and reversible.

---

### 1. Canonical integration layer (sanity summary)

- **Entry / SL / TP / RR / sizing / leverage**:
  - Canonical levels and sizing are computed via:
    - `scalper.trade_preview.build_trade_preview` (entry, SL, TP, ATR, RR, qty, notional).
    - `scalper.trade_plan.build_trade_plan` (structure-aware TP, leverage recommendation, plan validity).
  - Paper execution (`PaperBroker` and `paper.update_and_maybe_close`) uses **only** these levels, passed via `preview_for_execution`.
  - Telegram ALLOW / PAPER OPEN messages read their levels and metrics from the same trade plan/preview.
- **No duplicate logic**:
  - Scanner, paper broker, analytics, and execution scaffold all depend on the single trade-plan pipeline:
    - signal → risk engine → trade preview → trade plan → paper open → analytics / (optional future execution).
  - Any previous ad-hoc sizing/levels flows are retained only as compatibility shims; canonical path is the Trade Plan.
- **Execution engine**:
  - `scalper.execution_engine` builds order plans from `TradePlan` only.
  - By default, it is **shadow-only** and not wired into scanner.

---

### 2. Safety defaults (must remain as-is unless deliberately changed)

- **Execution / trading**
  - `EXECUTION_MODE=disabled` (default, recommended for all non-test work).
  - `EXECUTION_DRY_RUN_LOG_ONLY=True` (no real orders even if wired in).
  - `EXECUTION_REQUIRE_TRADE_PLAN_OK=True`.
  - `EXECUTION_REQUIRE_ISOLATED_MARGIN=True`.
  - `EXECUTION_MAX_LEVERAGE` set to a conservative value (e.g. `5`).
  - `KILL_SWITCH=1` and/or `RISK_KILL_SWITCH=1` can hard-block real execution even if mode changes.
- **Ranking / selection**
  - `ranking_enabled=False` by default.
  - `ranking_min_rr=1.0`, `ranking_max_rr=4.0` (soft shaping only when enabled).
  - `ranking_min_atr_pct` / `ranking_max_atr_pct` default to broad, safe ranges.
- **Paper exits and management**
  - `BE_AT_R`, `PARTIAL_TP_AT_R`, `TRAIL_AFTER_R` default to safe (often zero = disabled) unless explicitly configured.
  - Paper only; no live orders are sent from these paths.
- **Telegram**
  - `TELEGRAM_POLICY` can be set to `events` or `both` for observability; does not affect trading logic.

---

### 3. Paper-only validation steps (required before any testnet)

Run these from project root (`/root/scalper`).

1. **Environment & config sanity**
   - `python -m scalper.scanner --validate-env`
   - `python tools/validate_runtime.py --verbose`
2. **One-off paper scan**
   - `python -m scalper.scanner --once --paper`
3. **Continuous paper run (recommended via tmux/screen)**
   - Example:
     - `tmux new -s scalper 'cd /root/scalper && source venv/bin/activate && python -m scalper.scanner --loop --paper'`
4. **Collect performance analytics (after sufficient history)**
   - `python -m scalper.analytics_report --recent-intents 5000 --recent-trades 2000 --json reports/analytics_recent.json`
5. **Run observability wrappers**
   - `./audit_bin/scalper-tmux-read`
   - `./audit_bin/scalper-log-read`
   - `./audit_bin/scalper-paper-read`
   - `./audit_bin/scalper-signals-read`
   - `./audit_bin/scalper-analytics-read`
   - `./audit_bin/scalper-code-read`
   - `./scalper/scalper/audit_bin/scalper-export-audit`

**Paper validation minimums (example thresholds):**

- Sample size:
  - ≥ **500–1000 paper trades** across multiple weeks and market regimes.
- Performance:
  - Profit factor ≥ **1.3**.
  - Winrate ≥ **45%** (for RR around 1.5–2.0); or justified by higher RR.
  - Average realized \(R\) ≥ **0.1** per trade.
  - Max consecutive losses ≤ **8–10** (or consistent with risk appetite).
- Data quality:
  - ATR failure rate (`ATR_UNAVAILABLE`/`ATR_DEGRADED`) ≤ **2–3%** of intents.
  - Degraded-plan block rate ≤ **5–10%** of intents (or clearly explained).
  - Cooldown block rate consistent with risk rules (not dominating decisions).
- Stability:
  - No systematic gaps or crashes in scanner logs.
  - No obviously pathological trades (e.g. absurd leverage, microscopic stops).

These thresholds should be refined jointly with the auditor (Frantisek) before moving on.

---

### 4. Pre-testnet checklist (shadow execution only)

**Configuration (do NOT send real orders yet):**

- Keep:
  - `EXECUTION_MODE=disabled`
  - `EXECUTION_DRY_RUN_LOG_ONLY=True`
  - `KILL_SWITCH=1` and/or `RISK_KILL_SWITCH=1`
- Integrate `scalper.execution_engine` only in a **shadow** fashion:
  - For a subset of ALLOW intents, build `OrderPlan` from `TradePlan` and log Bybit payloads using:
    - `build_order_plan(plan)` and `build_bybit_http_payloads(order)`
  - Log resulting payloads at INFO/DEBUG without calling Bybit.

**Shadow execution validation:**

- For a random sample of paper trades:
  - Confirm:
    - `OrderPlan.entry_price == TradePlan.entry`
    - `OrderPlan.sl_price == TradePlan.stop`
    - `OrderPlan.tp_price == TradePlan.tp`
    - `OrderPlan.qty` matches plan `qty_est` (within rounding).
    - Derived leverage is within `EXECUTION_MAX_LEVERAGE` and `STOP_TOO_TIGHT` / `LEVERAGE_ABSURD` guards behave as expected.

**Additional performance thresholds (before real testnet orders):**

- Paper performance remains acceptable under **recent** data (last 200–300 trades), not just full history:
  - Profit factor ≥ **1.2**.
  - Expectancy positive over last slice.
- Plan integrity:
  - No large fraction of trades with `planned_entry` far from actual fill.
  - Realized \(R\) distribution matches planned \(R\) roughly (no systematic slippage beyond expectations).

Only after the above should you consider enabling real testnet orders.

---

### 5. Testnet execution checklist (real orders, non-production)

**Configuration for testnet (example, KEEP KILL SWITCH ON FOR LIVE):**

- Set:
  - `EXECUTION_MODE=testnet`
  - `EXPLICIT_CONFIRM_EXECUTION=1`
  - `EXECUTION_MAX_LEVERAGE` to a conservative value (e.g. 2–3).
  - `EXECUTION_ALLOW_SYMBOLS` to a tight whitelist (e.g. `BTCUSDT,ETHUSDT`).
  - `EXECUTION_DENY_SYMBOLS` to include all exotic / illiquid pairs.
  - Keep `EXECUTION_REQUIRE_TRADE_PLAN_OK=True`, `EXECUTION_REQUIRE_ISOLATED_MARGIN=True`.
  - Keep `KILL_SWITCH=1` or route testnet via a **separate** account / environment.
- **Important**: initially use testnet in **shadow-only mode** (do not actually send HTTP requests) until Bybit credentials and network behavior are fully validated by a human.

**During initial testnet phase:**

- Send **small**, **rare** test orders only (manually triggered).
- Continuously:
  - Compare testnet fills vs paper fills for the same signals.
  - Inspect Bybit logs / dashboards to confirm TP/SL placements match the trade plan.
- Do not relax kill switches or dry-run flags until:
  - You have verified connectivity, auth, error handling, and rate limiting.

---

### 6. Live execution enablement checklist (ONLY after full audit)

**Preconditions (numbers illustrative; adjust with Frantisek):**

- Long-run paper performance:
  - Profit factor ≥ **1.5** over ≥ **1000** paper trades.
  - Winrate and RR consistent with risk / psychological limits.
  - Max drawdown acceptable for planned capital.
- Stability:
  - No unexplained scanner crashes or extended stalls.
  - No data-quality issues dominating decisions.
- Audit sign-off (Frantisek):
  - Review of:
    - `FRANTISEK_AUDIT_PROMPT.txt` and `FRANTISEK_OPERATING_MODEL.md` outputs.
    - Recent analytics JSON.
    - Sample trade logs and code changes.
  - Explicit written **GO** decision, including rollback plan.

**Config changes for live (small initial size):**

- Flip:
  - `EXECUTION_MODE=live`
  - `EXPLICIT_CONFIRM_EXECUTION=1`
  - `KILL_SWITCH=0`, `RISK_KILL_SWITCH=0` **only** after you are ready to allow live orders.
  - Start with very low notional sizing and tight `EXECUTION_MAX_LEVERAGE`.
- Keep:
  - `EXECUTION_REQUIRE_TRADE_PLAN_OK=True`
  - `EXECUTION_REQUIRE_ISOLATED_MARGIN=True`
  - `EXECUTION_ALLOW_SYMBOLS` as a small whitelist.
  - Monitoring & logging at high verbosity.

Live trading should initially be run in **limited hours** and with **manual supervision**.

---

### 7. Rollback and emergency kill steps

**Immediate rollback (config-only):**

- Set:
  - `EXECUTION_MODE=disabled`
  - `KILL_SWITCH=1`
  - `RISK_KILL_SWITCH=1`
- Restart or reload any long-running processes using these settings.

**Runtime kill (if something goes wrong during a session):**

- Stop scanner loop process (e.g. tmux session):
  - `tmux kill-session -t scalper` (or equivalent, executed by an operator).
- Verify:
  - No active Python `scalper.scanner` processes: `ps aux | grep 'python -m scalper.scanner'`.

**Post-incident analysis:**

- Use:
  - `./audit_bin/scalper-tmux-read`
  - `./audit_bin/scalper-log-read`
  - `./audit_bin/scalper-paper-read`
  - `./audit_bin/scalper-analytics-read`
  - `./scalper/scalper/audit_bin/scalper-export-audit`
- Capture the full audit pack and share with Frantisek / operators for root-cause analysis.

---

### 8. Logs and artifacts to inspect when things fail

- **Runtime issues (crashes, stalls)**
  - `live_loop.log` via `scalper-log-read`
  - tmux output via `scalper-tmux-read`
  - `tools/validate_runtime.py --verbose` output for import / wiring problems.
- **Trade-quality issues (bad exits, odd entries)**
  - `paper_state.json` via `scalper-paper-read` (open positions).
  - `signals_enriched_log.csv` via `scalper-signals-read`.
  - Recent analytics:
    - `python -m scalper.analytics_report --recent-intents 2000 --recent-trades 2000 --json reports/debug_analytics.json`
  - SQLite:
    - `trade_intents`, `trade_records`, `paper_trades` tables (via sqlite tools) for precise fills and RR.
- **Code regressions**
  - `scalper-code-read` (git status/log/diff).
  - Audit export contents (`scalper-export-audit`).

If any metric or behavior crosses defined thresholds, **immediately**:

- Disable execution (`EXECUTION_MODE=disabled`, `KILL_SWITCH=1`, `RISK_KILL_SWITCH=1`).
- Revert recent code/config changes as needed.
- Re-run paper-only validation and analytics before considering re-enabling testnet or live modes.

