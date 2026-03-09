from __future__ import annotations

import logging
import os
import time
from typing import Literal

_TELEGRAM_CONFIG_WARNED: bool = False
_LAST_TELEGRAM_SENT_AT: float = time.time()


KindCategory = Literal["signals", "events"]


def warn_missing_telegram_once() -> None:
    global _TELEGRAM_CONFIG_WARNED
    if _TELEGRAM_CONFIG_WARNED:
        return
    logging.warning(
        "Telegram disabled: missing TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID; running with logs only."
    )
    _TELEGRAM_CONFIG_WARNED = True


def get_last_telegram_sent_at() -> float:
    return float(_LAST_TELEGRAM_SENT_AT)


def _classify_kind(kind: str, text: str) -> KindCategory:
    k = str(kind or "").strip().lower()
    t = str(text or "").strip().upper()
    if k in {"heartbeat", "scan_summary", "stall", "test", "event", "open", "close", "block"}:
        return "events"
    if k in {"signal", "intent_signal"}:
        return "signals"
    if k == "intent":
        if t.startswith("ALLOW[") or t.startswith("EARLY[") or "CONFIRMED" in t:
            return "signals"
        return "events"
    return "events"


def should_send(policy: str, kind: str, text: str) -> bool:
    normalized = str(policy or "events").strip().lower()
    if normalized == "off":
        return False
    category = _classify_kind(kind, text)
    if normalized == "both":
        return True
    if normalized not in {"signals", "events"}:
        normalized = "events"
    return category == normalized


def send_telegram_with_logging(
    *,
    kind: str,
    token: str,
    chat_id: str,
    text: str,
    strict: bool = False,
) -> bool:
    from telegram_notify import send_telegram

    policy = str(os.getenv("TELEGRAM_POLICY", "events") or "events").lower()
    if not should_send(policy=policy, kind=kind, text=text):
        logging.info("TELEGRAM_SKIP policy=%s kind=%s", policy, kind)
        return False

    token_set = bool(str(token or "").strip())
    chat_value = str(chat_id or "")
    chat_set = bool(chat_value.strip())
    logging.info(
        "TELEGRAM_SEND_ATTEMPT kind=%s token_set=%s chat_set=%s chat_len=%d",
        str(kind),
        token_set,
        chat_set,
        len(chat_value),
    )
    try:
        send_telegram(token=token, chat_id=chat_id, text=text)
        logging.info("TELEGRAM_SEND_OK")
        global _LAST_TELEGRAM_SENT_AT
        _LAST_TELEGRAM_SENT_AT = time.time()
        return True
    except Exception:
        logging.exception("TELEGRAM_SEND_FAIL")
        if strict:
            raise
        return False

