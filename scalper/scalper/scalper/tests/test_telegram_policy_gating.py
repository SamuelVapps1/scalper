from __future__ import annotations

from typing import List

from scalper.notifier import send_telegram_with_logging, should_send


def test_should_send_policy_matrix() -> None:
    assert should_send("off", "intent", "ALLOW[15m] BTCUSDT LONG conf=0.75") is False
    assert should_send("signals", "intent", "ALLOW[15m] BTCUSDT LONG conf=0.75") is True
    assert should_send("signals", "heartbeat", "HEARTBEAT ok") is False
    assert should_send("events", "intent", "BLOCK[15m] BTCUSDT LONG") is True
    assert should_send("events", "intent", "ALLOW[15m] BTCUSDT LONG conf=0.75") is False
    assert should_send("both", "heartbeat", "HEARTBEAT ok") is True
    assert should_send("both", "intent", "ALLOW[15m] BTCUSDT LONG conf=0.75") is True


def test_send_telegram_with_logging_respects_policy(monkeypatch) -> None:
    sent: List[str] = []

    def _fake_send(token: str, chat_id: str, text: str) -> None:
        sent.append(text)

    monkeypatch.setattr("telegram_notify.send_telegram", _fake_send)
    monkeypatch.setattr("scalper.telegram_notify.send_telegram", _fake_send)

    monkeypatch.setenv("TELEGRAM_POLICY", "signals")
    ok_signal = send_telegram_with_logging(
        kind="intent",
        token="x",
        chat_id="y",
        text="ALLOW[15m] BTCUSDT LONG conf=0.75",
    )
    ok_event = send_telegram_with_logging(
        kind="heartbeat",
        token="x",
        chat_id="y",
        text="HEARTBEAT ok",
    )
    assert ok_signal is True
    assert ok_event is False
    assert len(sent) == 1
