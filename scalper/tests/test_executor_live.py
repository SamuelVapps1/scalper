from __future__ import annotations

import hashlib
import hmac
import json
from types import SimpleNamespace
from typing import Any, Dict

import scalper.executor_live as ex


def test_signing_helper_matches_hmac_sha256():
    ts = 1658384314791
    api_key = "test_key"
    recv = 5000
    payload = "category=option&symbol=BTC-29JUL22-25000-C"
    secret = "test_secret"
    sig1 = ex._sign_payload(ts, api_key, recv, payload, secret)
    msg = f"{ts}{api_key}{recv}{payload}"
    sig2 = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
    assert sig1 == sig2


def _make_order_result(status: str = "Filled") -> Dict[str, Any]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {
                    "orderId": "abc123",
                    "orderStatus": status,
                    "avgPrice": "100.5",
                    "cumExecQty": "0.02",
                    "cumExecFee": "0.001",
                }
            ]
        },
        "retExtInfo": {},
        "time": 1,
    }


def test_poll_order_until_filled_happy_path(monkeypatch):
    # Two successive status checks: New -> Filled
    seq = [
        {"ok": True, "status": "New", "executed_qty": None, "executed_price": None, "fee": None, "raw_response": {}},
        {
            "ok": True,
            "status": "Filled",
            "executed_qty": 0.02,
            "executed_price": 100.5,
            "fee": 0.001,
            "raw_response": {},
        },
    ]

    def _fake_get(order_id: str, symbol: str | None = None, category: str = "linear") -> Dict[str, Any]:
        return seq.pop(0)

    monkeypatch.setattr(ex, "get_order_status", _fake_get)
    res = ex.poll_order_until_filled("abc123", symbol="BTCUSDT", timeout_seconds=5, poll_interval=0.01)
    assert res["ok"] is True
    assert res["status"].upper() == "FILLED"
    assert res["executed_qty"] == 0.02


def test_poll_order_until_filled_timeout(monkeypatch):
    def _fake_get(order_id: str, symbol: str | None = None, category: str = "linear") -> Dict[str, Any]:
        return {"ok": True, "status": "New", "executed_qty": None, "executed_price": None, "fee": None, "raw_response": {}}

    monkeypatch.setattr(ex, "get_order_status", _fake_get)
    res = ex.poll_order_until_filled("abc123", symbol="BTCUSDT", timeout_seconds=0, poll_interval=0.01)
    assert res["ok"] is False
    assert res["error"] == "POLL_TIMEOUT"


def test_safe_place_and_confirm_respects_guard(monkeypatch):
    # Fake settings so that guard blocks
    settings = SimpleNamespace(mode="live", confirm_required=True)

    def _settings_from_config():
        return settings

    def _guard(s):
        return SimpleNamespace(allowed=False, reason="EXECUTION_GUARDED")

    monkeypatch.setattr(ex, "ExecutionSettings", SimpleNamespace(from_config=_settings_from_config))
    monkeypatch.setattr(ex, "check_execution_guard", _guard)

    order = SimpleNamespace(
        symbol="BTCUSDT",
        side="LONG",
        qty=0.01,
        entry_price=100.0,
        sl_price=95.0,
        tp_price=110.0,
    )
    res = ex.safe_place_and_confirm_market_order(order)
    assert res["ok"] is False
    assert res["status"] == "guarded"
    assert res["error"] == "EXECUTION_GUARDED"

