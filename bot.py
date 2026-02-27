from scalper.scanner import Scanner, parse_args, setup_logging
from scalper.settings import get_settings


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    get_settings()

    import config

    scanner = Scanner(config, args)
    return scanner.run()


if __name__ == "__main__":
    raise SystemExit(main())
