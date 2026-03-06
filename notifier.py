from __future__ import annotations

import logging
import time

_TELEGRAM_CONFIG_WARNED: bool = False
_LAST_TELEGRAM_SENT_AT: float = time.time()
_LAST_FAIL_LOG_BY_KIND: dict[str, float] = {}
_FAIL_LOG_THROTTLE_SEC = 60.0


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

    # Daily alert budget gate (per calendar UTC day).
    budget = int(getattr(_cfg, "TELEGRAM_DAILY_BUDGET", 0) or 0)
    if budget > 0:
        try:
            from storage import get_telegram_alerts_sent_today

            sent_today = get_telegram_alerts_sent_today()
        except Exception:
            sent_today = 0
        if sent_today >= budget:
            logging.warning(
                "TELEGRAM_BUDGET_BLOCK alerts_sent_today=%d budget=%d kind=%s",
                sent_today,
                budget,
                kind,
            )
            return False

    policy = str(getattr(_cfg, "TELEGRAM_POLICY", "events") or "events")
    policy_norm = policy.strip().lower()
    kind_norm = str(kind or "").strip().lower()
    if policy_norm in {"off", "none", "disabled"}:
        logging.info("TELEGRAM_POLICY_SKIP policy=%s kind=%s", policy_norm, kind_norm or "msg")
        return False
    if policy_norm == "periodic" and kind_norm in {"intent", "stall", "test"}:
        logging.info("TELEGRAM_POLICY_SKIP policy=%s kind=%s", policy_norm, kind_norm)
        return False
    if policy_norm == "events" and kind_norm in {"heartbeat", "summary"}:
        logging.info("TELEGRAM_POLICY_SKIP policy=%s kind=%s", policy_norm, kind_norm)
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
    ok = send_telegram(token=token, chat_id=chat_id, text=text, kind=kind, policy=policy)
    if ok:
        logging.info("TELEGRAM_SEND_OK")
        global _LAST_TELEGRAM_SENT_AT
        _LAST_TELEGRAM_SENT_AT = time.time()
        if budget > 0:
            try:
                from storage import increment_telegram_alerts_sent

                new_count = increment_telegram_alerts_sent()
                logging.info(
                    "TELEGRAM_DAILY_COUNTER alerts_sent_today=%d budget=%d kind=%s",
                    new_count,
                    budget,
                    kind,
                )
            except Exception:
                pass
        return True
    now = time.time()
    global _LAST_FAIL_LOG_BY_KIND
    last = _LAST_FAIL_LOG_BY_KIND.get(kind, 0.0)
    if now - last >= _FAIL_LOG_THROTTLE_SEC:
        logging.warning(
            "TELEGRAM_SEND_FAIL kind=%s (timeout/retries exhausted or API error; throttled to once per minute)",
            kind,
        )
        _LAST_FAIL_LOG_BY_KIND[kind] = now
    return False

