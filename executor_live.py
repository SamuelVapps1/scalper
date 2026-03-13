from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import certifi
import requests

from scalper.execution_engine import ExecutionSettings, OrderPlan, check_execution_guard

log = logging.getLogger(__name__)


BYBIT_MAINNET = "https://api.bybit.com"
BYBIT_TESTNET = "https://api-testnet.bybit.com"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _base_url_for_mode(mode: str) -> str:
    env_url = (os.getenv("BYBIT_BASE_URL") or "").strip()
    if env_url:
        return env_url.rstrip("/")
    if mode == "testnet":
        return BYBIT_TESTNET
    return BYBIT_MAINNET


def _get_api_credentials() -> tuple[str, str]:
    key = os.getenv("BYBIT_API_KEY", "").strip()
    secret = os.getenv("BYBIT_API_SECRET", "").strip()
    return key, secret


def _sign_payload(timestamp: int, api_key: str, recv_window: int, payload: str, secret: str) -> str:
    msg = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


@dataclass
class BybitResponse:
    ok: bool
    status: str
    result: Dict[str, Any]
    error: Optional[str]
    raw: Dict[str, Any]


class BybitPrivateClient:
    def __init__(self, mode: str, session: Optional[requests.Session] = None) -> None:
        self.mode = mode
        self.api_key, self.api_secret = _get_api_credentials()
        self.base_url = _base_url_for_mode(mode)
        self.recv_window = int(os.getenv("X_BAPI_RECV_WINDOW", "5000") or 5000)
        self.session = session or requests.Session()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        timeout: float = 10.0,
    ) -> BybitResponse:
        if not self.api_key or not self.api_secret:
            return BybitResponse(False, "MISSING_API_CREDENTIALS", {}, "MISSING_API_CREDENTIALS", {})

        url = self.base_url.rstrip("/") + path
        ts = _now_ms()
        method = method.upper()

        if method == "GET":
            query = "&".join(
                f"{k}={v}"
                for k, v in sorted((params or {}).items())
                if v is not None
            )
            payload_for_sig = query
        else:
            body_dict = body or {}
            payload_for_sig = json.dumps(body_dict, separators=(",", ":"), ensure_ascii=False)

        sign = _sign_payload(ts, self.api_key, self.recv_window, payload_for_sig, self.api_secret)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(ts),
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "X-BAPI-SIGN": sign,
            "Content-Type": "application/json",
        }

        try:
            if method == "GET":
                resp = self.session.get(
                    url,
                    params=params or {},
                    headers=headers,
                    timeout=timeout,
                    verify=certifi.where(),
                )
            else:
                resp = self.session.post(
                    url,
                    params=params or {},
                    data=payload_for_sig,
                    headers=headers,
                    timeout=timeout,
                    verify=certifi.where(),
                )
        except requests.RequestException as exc:
            log.warning("Bybit HTTP error method=%s path=%s err=%s", method, path, exc)
            return BybitResponse(False, "http_error", {}, str(exc), {})

        try:
            data = resp.json()
        except ValueError:
            log.warning("Bybit non-JSON response status=%s text=%s", resp.status_code, resp.text[:512])
            return BybitResponse(False, "decode_error", {}, "NON_JSON_RESPONSE", {})

        ret_code = data.get("retCode")
        if resp.status_code != 200 or ret_code not in (0, "0"):
            err = f"HTTP {resp.status_code} retCode={ret_code} retMsg={data.get('retMsg')}"
            log.warning("Bybit error method=%s path=%s %s", method, path, err)
            return BybitResponse(False, "api_error", data.get("result") or {}, err, data)

        return BybitResponse(True, "ok", data.get("result") or {}, None, data)

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        reduce_only: bool = False,
        category: str = "linear",
    ) -> BybitResponse:
        body = {
            "category": category,
            "symbol": symbol,
            "side": "Buy" if side.upper() == "LONG" else "Sell",
            "orderType": "Market",
            "qty": str(qty),
            "timeInForce": "GoodTillCancel",
        }
        if reduce_only:
            body["reduceOnly"] = True
        return self._request("POST", "/v5/order/create", body=body)

    def get_order_status(
        self,
        order_id: str,
        *,
        symbol: Optional[str] = None,
        category: str = "linear",
    ) -> BybitResponse:
        params: Dict[str, Any] = {"category": category, "orderId": order_id}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/v5/order/realtime", params=params)


def _extract_fill_from_result(result: Dict[str, Any]) -> Dict[str, Any]:
    orders = result.get("list") or []
    if not orders:
        return {}
    o = orders[0]
    try:
        price = float(o.get("avgPrice") or o.get("price") or 0.0)
    except (TypeError, ValueError):
        price = 0.0
    try:
        qty = float(o.get("cumExecQty") or o.get("qty") or 0.0)
    except (TypeError, ValueError):
        qty = 0.0
    try:
        fee = float(o.get("cumExecFee") or 0.0)
    except (TypeError, ValueError):
        fee = 0.0
    return {
        "order_id": str(o.get("orderId") or ""),
        "order_status": str(o.get("orderStatus") or ""),
        "executed_price": price or None,
        "executed_qty": qty or None,
        "fee": fee or None,
    }


def place_market_order(
    symbol: str,
    side: str,
    qty: float,
    *,
    reduce_only: bool = False,
    category: str = "linear",
) -> Dict[str, Any]:
    settings = ExecutionSettings.from_config()
    client = BybitPrivateClient(settings.mode)
    resp = client.place_market_order(symbol, side, qty, reduce_only=reduce_only, category=category)
    info = _extract_fill_from_result(resp.result)
    return {
        "ok": bool(resp.ok),
        "order_id": info.get("order_id"),
        "status": info.get("order_status") or resp.status,
        "executed_price": info.get("executed_price"),
        "executed_qty": info.get("executed_qty"),
        "fee": info.get("fee"),
        "raw_response": resp.raw,
        "error": resp.error,
    }


def get_order_status(
    order_id: str,
    *,
    symbol: Optional[str] = None,
    category: str = "linear",
) -> Dict[str, Any]:
    settings = ExecutionSettings.from_config()
    client = BybitPrivateClient(settings.mode)
    resp = client.get_order_status(order_id, symbol=symbol, category=category)
    info = _extract_fill_from_result(resp.result)
    return {
        "ok": bool(resp.ok),
        "order_id": info.get("order_id") or order_id,
        "status": info.get("order_status") or resp.status,
        "executed_price": info.get("executed_price"),
        "executed_qty": info.get("executed_qty"),
        "fee": info.get("fee"),
        "raw_response": resp.raw,
        "error": resp.error,
    }


def poll_order_until_filled(
    order_id: str,
    *,
    symbol: Optional[str] = None,
    timeout_seconds: int = 30,
    poll_interval: float = 1.0,
    category: str = "linear",
) -> Dict[str, Any]:
    deadline = time.time() + max(1, timeout_seconds)
    last_status: Optional[Dict[str, Any]] = None
    while time.time() < deadline:
        status = get_order_status(order_id, symbol=symbol, category=category)
        last_status = status
        if not status.get("ok"):
            time.sleep(poll_interval)
            continue
        ord_status = str(status.get("status") or "").upper()
        if ord_status in {"FILLED", "PARTIALLY_FILLED"}:
            return status
        if ord_status in {"CANCELED", "CANCELLED", "REJECTED"}:
            return status
        time.sleep(poll_interval)
    if last_status is None:
        last_status = {"ok": False, "status": "timeout", "error": "POLL_TIMEOUT", "raw_response": {}}
    else:
        last_status = dict(last_status)
        last_status["ok"] = False
        last_status["status"] = last_status.get("status") or "timeout"
        last_status["error"] = last_status.get("error") or "POLL_TIMEOUT"
    return last_status


def _compute_live_qty(order: OrderPlan) -> float:
    micro_usdt = float(os.getenv("LIVE_MICRO_USDT", "2") or 2.0)
    if micro_usdt <= 0 or order.entry_price <= 0:
        return max(order.qty, 0.0)
    micro_qty = micro_usdt / max(order.entry_price, 1e-10)
    return max(min(order.qty, micro_qty), 0.0)


def safe_place_and_confirm_market_order(
    order: OrderPlan,
    *,
    symbol: Optional[str] = None,
    timeout_seconds: int = 30,
    poll_interval: float = 1.0,
    category: str = "linear",
) -> Dict[str, Any]:
    settings = ExecutionSettings.from_config()
    guard = check_execution_guard(settings)
    if not guard.allowed:
        return {
            "ok": False,
            "order_id": None,
            "status": "guarded",
            "executed_price": None,
            "executed_qty": None,
            "fee": None,
            "raw_response": {},
            "error": guard.reason or "EXECUTION_GUARDED",
        }

    if settings.mode not in ("testnet", "live"):
        return {
            "ok": False,
            "order_id": None,
            "status": "mode_disabled",
            "executed_price": None,
            "executed_qty": None,
            "fee": None,
            "raw_response": {},
            "error": f"EXECUTION_MODE={settings.mode}",
        }

    live_qty = _compute_live_qty(order)
    if live_qty <= 0:
        return {
            "ok": False,
            "order_id": None,
            "status": "invalid_qty",
            "executed_price": None,
            "executed_qty": None,
            "fee": None,
            "raw_response": {},
            "error": "NON_POSITIVE_QTY",
        }

    client = BybitPrivateClient(settings.mode)
    place_resp = client.place_market_order(order.symbol, order.side, live_qty, reduce_only=False, category=category)
    info = _extract_fill_from_result(place_resp.result)
    order_id = info.get("order_id")
    result: Dict[str, Any] = {
        "ok": bool(place_resp.ok),
        "order_id": order_id,
        "status": info.get("order_status") or place_resp.status,
        "executed_price": info.get("executed_price"),
        "executed_qty": info.get("executed_qty"),
        "fee": info.get("fee"),
        "raw_response": place_resp.raw,
        "error": place_resp.error,
    }
    if not place_resp.ok or not order_id:
        return result

    final_status = poll_order_until_filled(
        order_id,
        symbol=symbol or order.symbol,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        category=category,
    )
    merged = dict(result)
    for k in ("status", "executed_price", "executed_qty", "fee", "error"):
        if final_status.get(k) is not None:
            merged[k] = final_status.get(k)
    merged["ok"] = bool(final_status.get("ok")) and bool(merged.get("executed_qty"))
    merged["raw_response"] = {
        "place": result.get("raw_response"),
        "final": final_status.get("raw_response", final_status),
    }
    return merged

