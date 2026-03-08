<<<<<<< HEAD
from pathlib import Path

from scalper.scanner import Scanner, parse_args, setup_logging
from scalper.settings import get_settings, validate_env


def bootstrap_runtime_dirs() -> None:
    """Create ./runs and ./data if missing. Works on Windows and Linux."""
    for name in ("runs", "data"):
        Path.cwd().joinpath(name).mkdir(parents=True, exist_ok=True)
=======
import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

CooldownKey = Tuple[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bybit Signal Bot (DRY RUN only, alerts and logging)."
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a Telegram test message and exit immediately.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one scan cycle and exit.",
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=30,
        help="Cooldown minutes per symbol+setup (default: 30).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO"],
        default="INFO",
        help="Log verbosity (default: INFO).",
    )
    return parser.parse_args()


def setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def is_cooled_down(
    key: CooldownKey,
    now_utc: datetime,
    last_alert_at: Dict[CooldownKey, datetime],
    cooldown_delta: timedelta,
) -> bool:
    previous = last_alert_at.get(key)
    if previous is None:
        return True
    return now_utc - previous >= cooldown_delta


def run_scan_cycle(
    watchlist,
    interval: str,
    lookback: int,
    cooldown_delta: timedelta,
    last_alert_at: Dict[CooldownKey, datetime],
    telegram_token: str,
    telegram_chat_id: str,
) -> None:
    from bybit import fetch_klines
    from signals import generate_signals
    from storage import append_signal
    from telegram_notify import send_telegram

    now_utc = datetime.now(timezone.utc)

    for symbol in watchlist:
        try:
            # DRY RUN only: public market data endpoint.
            candles = fetch_klines(symbol=symbol, interval=interval, limit=lookback)
            detected = generate_signals(symbol=symbol, candles=candles)

            for signal in detected:
                append_signal(signal)

                setup_name = signal["setup"]
                cooldown_key = (symbol, setup_name)
                if is_cooled_down(cooldown_key, now_utc, last_alert_at, cooldown_delta):
                    if telegram_token and telegram_chat_id:
                        msg = (
                            f"Signal: {setup_name}\n"
                            f"Symbol: {symbol}\n"
                            f"Interval: {interval}\n"
                            f"Price: {signal['close']:.4f}\n"
                            f"Reason: {signal['reason']}\n"
                            "Mode: DRY RUN (no orders)"
                        )
                        try:
                            send_telegram(
                                token=telegram_token,
                                chat_id=telegram_chat_id,
                                text=msg,
                            )
                            last_alert_at[cooldown_key] = now_utc
                        except Exception:
                            logging.warning(
                                "Telegram send failed. Check token/chat_id/network."
                            )
                    else:
                        logging.info(
                            "Telegram credentials missing; signal logged only (%s | %s).",
                            symbol,
                            setup_name,
                        )
                else:
                    logging.info("Cooldown active for %s | %s.", symbol, setup_name)
        except Exception as exc:
            logging.exception("Scan failed for %s: %s", symbol, exc)
>>>>>>> 687e22dccb4ca354fd3fb211e4c4c4cb9c7b2313


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
<<<<<<< HEAD
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
=======

    if args.cooldown_minutes <= 0:
        logging.error("--cooldown-minutes must be a positive integer.")
        return 2
>>>>>>> 687e22dccb4ca354fd3fb211e4c4c4cb9c7b2313

    from config import (
        INTERVAL,
        LOOKBACK,
        SCAN_SECONDS,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        WATCHLIST,
    )

    if args.test_telegram:
        from telegram_notify import send_telegram

        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logging.error(
                "Missing Telegram config. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env."
            )
            return 2
        try:
            send_telegram(
                token=TELEGRAM_BOT_TOKEN,
                chat_id=TELEGRAM_CHAT_ID,
                text="✅ Telegram OK (test)",
            )
            logging.info("Telegram test message sent. Exiting --test-telegram mode.")
            return 0
        except Exception:
            logging.error("Telegram test failed. Check token/chat_id/network.")
            return 1

    if not WATCHLIST:
        logging.error("WATCHLIST is empty. Set WATCHLIST in .env.")
        return 2

    cooldown_delta = timedelta(minutes=args.cooldown_minutes)
    last_alert_at: Dict[CooldownKey, datetime] = {}

    logging.info("Starting Bybit Signal Bot in DRY RUN mode (no trading).")

    if args.once:
        run_scan_cycle(
            watchlist=WATCHLIST,
            interval=INTERVAL,
            lookback=LOOKBACK,
            cooldown_delta=cooldown_delta,
            last_alert_at=last_alert_at,
            telegram_token=TELEGRAM_BOT_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
        )
        logging.info("Completed one scan cycle. Exiting --once mode.")
        return 0

    while True:
        run_scan_cycle(
            watchlist=WATCHLIST,
            interval=INTERVAL,
            lookback=LOOKBACK,
            cooldown_delta=cooldown_delta,
            last_alert_at=last_alert_at,
            telegram_token=TELEGRAM_BOT_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
        )
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
