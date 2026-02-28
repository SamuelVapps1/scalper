from pathlib import Path

from scalper.scanner import Scanner, parse_args, setup_logging
from scalper.settings import get_settings, validate_env


def bootstrap_runtime_dirs() -> None:
    """Create ./runs and ./data if missing. Works on Windows and Linux."""
    for name in ("runs", "data"):
        Path.cwd().joinpath(name).mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    bootstrap_runtime_dirs()

    if getattr(args, "validate_env", False):
        ok, missing = validate_env()
        if ok:
            print("OK")
            return 0
        for key in missing:
            print(key)
        return 1

    get_settings()
    import config

    scanner = Scanner(config, args)
    return scanner.run()


if __name__ == "__main__":
    raise SystemExit(main())
