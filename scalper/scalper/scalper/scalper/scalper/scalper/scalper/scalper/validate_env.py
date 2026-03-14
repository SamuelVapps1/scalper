"""
CLI: validate .env and print normalized config (no secrets).
Usage: python -m scalper.validate_env [ENV_PATH]
Reads ENV_PATH or .env via existing bootstrap; prints normalized config and any errors.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure package on path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Optional: allow ENV_PATH as first arg
def _main():
    args = sys.argv[1:]
    if args and not args[0].startswith("-"):
        os.environ["ENV_PATH"] = args[0]

    from scalper.settings import get_settings, validate_env

    errors: list[str] = []
    try:
        s = get_settings()
    except Exception as e:
        errors.append(f"ValidationError/load: {e}")
        print("ERROR: Failed to load settings")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)

    # Business validation
    ok, missing = validate_env()
    if not ok:
        errors.extend(missing)

    # Normalized config (no secrets)
    def mask(k: str, v: str) -> str:
        if not v:
            return "(empty)"
        if "token" in k.lower() or "secret" in k.lower() or "chat_id" in k.lower():
            return "***"
        return v

    risk = s.risk
    print("--- Normalized config (secrets masked) ---")
    print("risk.watchlist_mode =", risk.watchlist_mode)
    print("risk.watchlist_universe_n =", risk.watchlist_universe_n)
    print("risk.watchlist_batch_n =", risk.watchlist_batch_n)
    print("risk.watchlist_refresh_seconds =", risk.watchlist_refresh_seconds)
    print("risk.watchlist_min_turnover_24h =", risk.watchlist_min_turnover_24h)
    print("risk.watchlist (length) =", len(risk.watchlist))
    print("bybit.execution_mode =", s.bybit.execution_mode)
    print("telegram.bot_token =", mask("bot_token", s.telegram.bot_token or ""))
    print("telegram.chat_id =", mask("chat_id", s.telegram.chat_id or ""))

    if errors:
        print("\n--- Validation / missing ---")
        for err in errors:
            print("  -", err)
        sys.exit(1)
    print("\nOK: env loaded and validated.")
    sys.exit(0)


if __name__ == "__main__":
    _main()
