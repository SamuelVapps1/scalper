# Scalper Read-Only Audit Layer

**Purpose:** Provide a safe, read-only observability layer for an external AI auditor ("Frantisek") to observe runtime, inspect code, analyze signal quality, and propose optimizations—**without mutating runtime, restarting services, or editing the repo**.

---

## ⚠️ WARNING: READ-ONLY ONLY

- **No runtime mutation.** These tools do NOT send keys, kill sessions, or modify live state.
- **No auto-fix behavior.** The auditor proposes changes; humans apply them.
- **No daemon changes.** Scanner, risk, Telegram, and config behavior are unchanged.

---

## Wrapper Commands (install to `/usr/local/bin`)

Copy scripts from `audit_bin/` to `/usr/local/bin` and make executable:

```bash
sudo cp audit_bin/scalper-* /usr/local/bin/
sudo chmod +x /usr/local/bin/scalper-*
```

| Command | Description |
|---------|-------------|
| `scalper-tmux-read` | Captures tmux pane output from session `scalper` (read-only; no send-keys) |
| `scalper-log-read` | Prints tail of `live_loop.log` |
| `scalper-paper-read` | Prints `paper_state.json` |
| `scalper-signals-read` | Prints `signals_enriched_log.csv` (header + last 999 rows if large) |
| `scalper-export-audit` | Runs `export_readonly_audit.sh` to create a full timestamped audit pack |

---

## Export Script: `export_readonly_audit.sh`

Creates a timestamped pack under `audit_exports/<YYYYMMDD_HHMMSS>/`:

| File | Description |
|------|-------------|
| `tmux_scalper_snapshot.txt` | Tmux pane capture from session `scalper` |
| `live_loop_tail.log` | Last 2000 lines of `live_loop.log` |
| `paper_state.json` | Copy of paper trading state |
| `signals_enriched_log.csv` | Full copy of signals log |
| `signals_enriched_log_tail.csv` | Last 500 lines (lightweight for quick analysis) |
| `filtered_events_tail.txt` | Lines matching ALLOW, BLOCK, PAPER OPEN, PAPER CLOSE, ATR_, PREVIEW_, LEVELS_ |
| `runtime_meta.txt` | hostname, whoami, pwd, tmux sessions, python scanner processes |
| `code_snapshots/*.py` | Copies of scanner, signals, trade_preview, paper_engine, paper_broker, settings, strategy_engine |

**Symlink:** `audit_exports/latest` → most recent export (for quick access without hunting timestamps).

**Environment:** Set `SCALPER_ROOT` if not `/root/scalper`:

```bash
SCALPER_ROOT=/path/to/scalper ./export_readonly_audit.sh
```

---

## What an External Auditor Should Analyze

1. **ALLOW vs PAPER OPEN vs PAPER CLOSE**
   - Compare entry/sl/tp in ALLOW messages vs actual paper open/close events
   - Check for `entry=n/a`, `sl=n/a`, `tp=n/a` in ALLOW (should not appear after preview fix)

2. **Signal Quality**
   - Win rate, TP vs SL ratio, timeout frequency
   - Confidence distribution, strategy mix

3. **Preview / Level Consistency**
   - `PREVIEW_`, `ATR_`, `LEVELS_` log lines
   - Downgrade reasons (ATR_UNAVAILABLE, LEVELS_UNAVAILABLE, etc.)

4. **Code Snapshots**
   - Review `trade_preview.py` for single source of truth
   - Check scanner flow: risk gate → preview → ALLOW/BLOCK → paper open

5. **Proposed Optimizations**
   - Output as recommendations only; no automatic application

---

## Dependencies

- **tmux** (assumed installed)
- **bash** (set -euo pipefail)
- **grep**, **tail**, **head**, **wc**, **cp**, **mkdir**, **date**

No new packages required.
