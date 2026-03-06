from __future__ import annotations

import logging
import time

_TELEGRAM_CONFIG_WARNED: bool = False
_LAST_TELEGRAM_SENT_AT: float = time.time()
_LAST_FAIL_LOG_BY_KIND: dict[str, float] = {}
_FAIL_LOG_THROTTLE_SEC = 60.0
_LAST_TELEGRAM_STATUS: str = "NOT_SENT"
_LAST_TELEGRAM_META: dict[str, object] = {}


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


def get_last_telegram_status() -> str:
    return str(_LAST_TELEGRAM_STATUS)


def get_last_telegram_meta() -> dict[str, object]:
    return dict(_LAST_TELEGRAM_META)


def send_telegram_with_logging(
    *,
    kind: str,
    token: str,
    chat_id: str,
    text: str,
    strict: bool = False,
) -> bool:
    from telegram_notify import get_last_send_error, send_telegram

    import config as _cfg
    global _LAST_TELEGRAM_STATUS
    global _LAST_TELEGRAM_META
    _LAST_TELEGRAM_STATUS = "NOT_SENT"
    policy = str(getattr(_cfg, "TELEGRAM_POLICY", "events") or "events")
    policy_norm = policy.strip().lower()
    kind_norm = str(kind or "").strip().lower() or "msg"
    _LAST_TELEGRAM_META = {
        "status": _LAST_TELEGRAM_STATUS,
        "kind": kind_norm,
        "policy": policy_norm,
        "policy_allowed": False,
        "policy_reason": "",
        "budget": int(getattr(_cfg, "TELEGRAM_DAILY_BUDGET", 0) or 0),
        "sent_today": None,
    }

    # Policy evaluation:
    # - strict=True bypasses policy checks
    # - "off"/"none"/"disabled" blocks all non-strict sends
    # - heartbeat/summary remain policy-controlled noise channels
    policy_allowed = True
    policy_reason = "allowed"
    if strict:
        policy_allowed = True
        policy_reason = "strict_bypass"
    elif policy_norm in {"off", "none", "disabled"}:
        policy_allowed = False
        policy_reason = "policy_off"
    elif policy_norm == "events" and kind_norm in {"heartbeat", "summary", "scan_summary"}:
        policy_allowed = False
        policy_reason = "events_suppresses_periodic_noise"
    else:
        policy_allowed = True
        policy_reason = "allowed"
    logging.info(
        "TELEGRAM_POLICY_EVAL kind=%s policy=%s allowed=%s reason=%s",
        kind_norm,
        policy_norm,
        bool(policy_allowed),
        policy_reason,
    )
    _LAST_TELEGRAM_META["policy_allowed"] = bool(policy_allowed)
    _LAST_TELEGRAM_META["policy_reason"] = policy_reason
    if not policy_allowed:
        logging.info("TELEGRAM_POLICY_SKIP policy=%s kind=%s", policy_norm, kind_norm)
        _LAST_TELEGRAM_STATUS = "POLICY_SKIP"
        _LAST_TELEGRAM_META["status"] = _LAST_TELEGRAM_STATUS
        return False

    # Daily alert budget gate (per calendar UTC day).
    budget = int(getattr(_cfg, "TELEGRAM_DAILY_BUDGET", 0) or 0)
    sent_today = 0
    if budget > 0:
        try:
            from storage import get_telegram_alerts_sent_today

            sent_today = int(get_telegram_alerts_sent_today() or 0)
        except Exception:
            sent_today = 0
        _LAST_TELEGRAM_META["sent_today"] = sent_today
        if sent_today >= budget:
            logging.warning(
                "TELEGRAM_BUDGET_BLOCK alerts_sent_today=%d budget=%d kind=%s",
                sent_today,
                budget,
                kind_norm,
            )
            _LAST_TELEGRAM_STATUS = "BUDGET_BLOCK"
            _LAST_TELEGRAM_META["status"] = _LAST_TELEGRAM_STATUS
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
        logging.info("TELEGRAM_SEND_SUCCESS kind=%s", kind_norm or "msg")
        _LAST_TELEGRAM_STATUS = "SEND_SUCCESS"
        _LAST_TELEGRAM_META["status"] = _LAST_TELEGRAM_STATUS
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
    err = get_last_send_error()
    err_class = str(err.get("class", "UnknownError"))
    err_short = str(err.get("reason", "unknown"))
    _LAST_TELEGRAM_STATUS = "SEND_FAILURE"
    _LAST_TELEGRAM_META["status"] = _LAST_TELEGRAM_STATUS
    logging.warning(
        "TELEGRAM_SEND_FAILURE kind=%s error_class=%s reason=%s",
        kind_norm or "msg",
        err_class,
        err_short,
    )
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

