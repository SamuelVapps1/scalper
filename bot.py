import os

# Optional but recommended: always load .env from repo root.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("ENV_PATH", os.path.join(ROOT, ".env"))

from scalper.scanner import main

if __name__ == "__main__":
    raise SystemExit(main())
