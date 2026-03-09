#!/usr/bin/env bash
#
# export_readonly_audit.sh
# Creates a timestamped read-only audit export pack for external AI auditor (Frantisek).
# NO MUTATION of runtime, repo, or state. Read-only operations only.
#
set -euo pipefail

SCALPER_ROOT="${SCALPER_ROOT:-/root/scalper}"
EXPORT_BASE="${SCALPER_ROOT}/audit_exports"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
EXPORT_DIR="${EXPORT_BASE}/${TIMESTAMP}"

mkdir -p "${EXPORT_DIR}"
echo "[AUDIT] Creating read-only export at ${EXPORT_DIR}"

# 1. Tmux output snapshot from session "scalper" (if exists)
if tmux has-session -t scalper 2>/dev/null; then
  tmux capture-pane -t scalper -p -S -5000 2>/dev/null > "${EXPORT_DIR}/tmux_scalper_snapshot.txt" || true
  echo "[AUDIT] tmux snapshot captured"
else
  echo "[AUDIT] tmux session 'scalper' not found, skipping" > "${EXPORT_DIR}/tmux_scalper_snapshot.txt"
fi

# 2. Tail of live_loop.log
if [[ -f "${SCALPER_ROOT}/live_loop.log" ]]; then
  tail -n 2000 "${SCALPER_ROOT}/live_loop.log" > "${EXPORT_DIR}/live_loop_tail.log"
  echo "[AUDIT] live_loop.log tail captured"
else
  echo "[AUDIT] live_loop.log not found" > "${EXPORT_DIR}/live_loop_tail.log"
fi

# 3. Copy paper_state.json if present
if [[ -f "${SCALPER_ROOT}/paper_state.json" ]]; then
  cp "${SCALPER_ROOT}/paper_state.json" "${EXPORT_DIR}/paper_state.json"
  echo "[AUDIT] paper_state.json copied"
else
  echo "{}" > "${EXPORT_DIR}/paper_state.json"
  echo "[AUDIT] paper_state.json not found, empty placeholder written"
fi

# 4. Copy signals_enriched_log.csv if present (full + tail)
if [[ -f "${SCALPER_ROOT}/signals_enriched_log.csv" ]]; then
  cp "${SCALPER_ROOT}/signals_enriched_log.csv" "${EXPORT_DIR}/signals_enriched_log.csv"
  tail -n 500 "${SCALPER_ROOT}/signals_enriched_log.csv" > "${EXPORT_DIR}/signals_enriched_log_tail.csv" || true
  echo "[AUDIT] signals_enriched_log.csv copied + tail captured"
else
  touch "${EXPORT_DIR}/signals_enriched_log.csv"
  touch "${EXPORT_DIR}/signals_enriched_log_tail.csv"
  echo "[AUDIT] signals_enriched_log.csv not found"
fi

# 5. Filtered event tail (ALLOW, BLOCK, PAPER OPEN, PAPER CLOSE, ATR_, PREVIEW_, LEVELS_)
if [[ -f "${SCALPER_ROOT}/live_loop.log" ]]; then
  grep -E 'ALLOW|BLOCK|PAPER OPEN|PAPER CLOSE|ATR_|PREVIEW_|LEVELS_' "${SCALPER_ROOT}/live_loop.log" 2>/dev/null | tail -n 5000 > "${EXPORT_DIR}/filtered_events_tail.txt" || true
  echo "[AUDIT] filtered events captured"
else
  echo "[AUDIT] live_loop.log not found, no filtered events" > "${EXPORT_DIR}/filtered_events_tail.txt"
fi

# 6. Code snapshot copies (try scalper/ and scalper/scalper/ layouts)
CODE_SNAPSHOT="${EXPORT_DIR}/code_snapshots"
mkdir -p "${CODE_SNAPSHOT}"

for name in scanner signals trade_preview paper_engine paper_broker settings strategy_engine; do
  for rel in "scalper/${name}.py" "scalper/scalper/${name}.py"; do
    src="${SCALPER_ROOT}/${rel}"
    dest="${CODE_SNAPSHOT}/${name}.py"
    if [[ -f "${src}" ]]; then
      cp "${src}" "${dest}"
      echo "[AUDIT] ${rel} -> ${name}.py"
      break
    fi
  done
  if [[ ! -f "${CODE_SNAPSHOT}/${name}.py" ]]; then
    echo "# File not found: ${name}.py" > "${CODE_SNAPSHOT}/${name}.py"
  fi
done

# 7. Runtime meta summary
{
  echo "timestamp_utc=${TIMESTAMP}"
  echo "hostname=$(hostname)"
  echo "whoami=$(whoami)"
  echo "pwd=$(pwd)"
  echo "tmux_sessions=$(tmux ls 2>/dev/null || true)"
  echo "python_processes=$(ps aux | grep 'python -m scalper.scanner' | grep -v grep || true)"
} > "${EXPORT_DIR}/runtime_meta.txt"
echo "[AUDIT] runtime_meta.txt written"

# 8. Top-level latest symlink
ln -sfn "${EXPORT_DIR}" "${EXPORT_BASE}/latest"
echo "[AUDIT] latest -> ${EXPORT_DIR}"

echo "[AUDIT] Export complete: ${EXPORT_DIR}"
ls -la "${EXPORT_DIR}"
