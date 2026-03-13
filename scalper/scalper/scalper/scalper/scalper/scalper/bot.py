import os
from pathlib import Path


def main() -> int:
    env_path = Path(".env").resolve()
    os.environ.setdefault("ENV_PATH", str(env_path))
    from scalper.scanner import main as scanner_main

    return int(scanner_main())


if __name__ == "__main__":
    raise SystemExit(main())
