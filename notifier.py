from __future__ import annotations

import logging
import time

_TELEGRAM_CONFIG_WARNED: bool = False
_LAST_TELEGRAM_SENT_AT: float = time.time()


def _policy_allows_kind(policy: str, kind: str) -> bool:
    p = str(policy or "events").strip().lower()
    k = str(kind or "").strip().lower()
    if p == "off":
        return False
    # Backward compatibility: keep legacy periodic behavior as "events-like".
    if p == "periodic":
        return k in {"intent", "scan_summary", "heartbeat", "stall", "test"}
    if p == "events":
        return k in {"intent", "scan_summary", "heartbeat", "stall", "test"}
    if p == "signals":
        return k in {"signal", "stall", "test"}
    if p == "both":
        return k in {"intent", "signal", "scan_summary", "heartbeat", "stall", "test"}
    return k in {"intent", "scan_summary", "heartbeat", "stall", "test"}


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


def send_telegram_with_logging(
    *,
    kind: str,
    token: str,
    chat_id: str,
    text: str,
    strict: bool = False,
) -> bool:
    from telegram_notify import send_telegram

    import config as _cfg

    policy = str(getattr(_cfg, "TELEGRAM_POLICY", "events") or "events")
    if not _policy_allows_kind(policy, kind):
        logging.info("TELEGRAM_POLICY_SKIP policy=%s kind=%s", policy, str(kind))
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
        send_telegram(token=token, chat_id=chat_id, text=text, kind=kind, policy=policy)
        logging.info("TELEGRAM_SEND_OK")
        global _LAST_TELEGRAM_SENT_AT
        _LAST_TELEGRAM_SENT_AT = time.time()
        return True
    except Exception:
        logging.exception("TELEGRAM_SEND_FAIL")
        if strict:
            raise
        return False

