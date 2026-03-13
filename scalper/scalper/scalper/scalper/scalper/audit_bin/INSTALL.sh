#!/usr/bin/env bash
# Install audit wrappers to /usr/local/bin (requires sudo)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for f in scalper-tmux-read scalper-log-read scalper-paper-read scalper-signals-read scalper-export-audit; do
  sudo cp "${SCRIPT_DIR}/${f}" /usr/local/bin/
  sudo chmod +x "/usr/local/bin/${f}"
  echo "Installed ${f}"
done
echo "Done. Run: scalper-tmux-read, scalper-log-read, scalper-paper-read, scalper-signals-read, scalper-export-audit"
