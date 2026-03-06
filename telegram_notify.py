<<<<<<< HEAD
import requests

def send_telegram(token: str, chat_id: str, text: str) -> None:
    """
    Send Telegram message with explicit credentials.
    Caller validates inputs and handles logging to avoid leaking secrets.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, json=payload, timeout=2.5)
    response.raise_for_status()
=======
import os
import requests


def send_telegram(token: str, chat_id: str, text: str, *, kind: str = "", policy: str = "") -> None:
    """
    Send Telegram message with explicit credentials.
    Prepends process tag: [pid=<pid> policy=<policy> kind=<kind>]
    Caller validates inputs and handles logging to avoid leaking secrets.
    """
    tag = f"[pid={os.getpid()} policy={policy or 'events'} kind={kind or 'msg'}] "
    payload_text = tag + str(text or "")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": payload_text}
    response = requests.post(url, json=payload, timeout=2.5)
    response.raise_for_status()


def build_signal_message(
    *,
    stage: str,
    symbol: str,
    strategy: str,
    side: str,
    confidence: float,
    bar_ts: str = "",
    bar_ts_15m: str = "",
    bar_ts_5m: str = "",
    interval: str = "",
    reason: str = "",
) -> str:
    stage_norm = str(stage or "").strip().upper()
    if stage_norm in {"EARLY_5M", "EARLY[5M]"}:
        prefix = "⚠️ EARLY[5m]"
    elif stage_norm in {"CONFIRMED_15M", "CONFIRMED[15M]"}:
        prefix = "✅ CONFIRMED[15m]"
    elif stage_norm == "EARLY":
        prefix = "⚠️ EARLY"
    else:
        prefix = "✅ CONFIRMED"
    resolved_bar_ts_15m = str(bar_ts_15m or bar_ts or "").strip()
    resolved_bar_ts_5m = str(bar_ts_5m or "").strip()
    lines = [
        prefix,
        f"symbol={symbol}",
        f"strategy={strategy}",
        f"side={side}",
        f"conf={float(confidence):.2f}",
    ]
    if resolved_bar_ts_15m:
        lines.append(f"bar_ts_15m={resolved_bar_ts_15m}")
    if resolved_bar_ts_5m:
        lines.append(f"bar_ts_5m={resolved_bar_ts_5m}")
    if interval:
        lines.append(f"interval={interval}")
    if reason:
        lines.append(f"reason={reason}")
    lines.append("mode=DRY_RUN")
    return "\n".join(lines)


>>>>>>> 4cfdd8fe6584fa7b2772b45743f088df40182329
