from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import config
from scalper.trade_plan import TradePlan

log = logging.getLogger(__name__)


EXECUTION_MODES = ("disabled", "paper", "testnet", "live")


def _normalize_mode(raw: Any) -> str:
    mode = str(raw or "disabled").strip().lower()
    return mode if mode in EXECUTION_MODES else "disabled"


@dataclass(frozen=True)
class ExecutionSettings:
    mode: str
    confirm_required: bool
    max_leverage: float
    allow_symbols: List[str]
    deny_symbols: List[str]
    require_trade_plan_ok: bool
    require_isolated_margin: bool
    dry_run_log_only: bool

    @classmethod
    def from_config(cls) -> "ExecutionSettings":
        mode = _normalize_mode(getattr(config, "EXECUTION_MODE", "disabled"))
        confirm_required = bool(getattr(config, "EXPLICIT_CONFIRM_EXECUTION", True))
        max_lev = float(getattr(config, "EXECUTION_MAX_LEVERAGE", 5.0) or 5.0)
        allow_raw = getattr(config, "EXECUTION_ALLOW_SYMBOLS", "") or ""
        deny_raw = getattr(config, "EXECUTION_DENY_SYMBOLS", "") or ""
        allow = [s.strip().upper() for s in str(allow_raw).split(",") if s.strip()]
        deny = [s.strip().upper() for s in str(deny_raw).split(",") if s.strip()]
        require_plan = bool(getattr(config, "EXECUTION_REQUIRE_TRADE_PLAN_OK", True))
        require_iso = bool(getattr(config, "EXECUTION_REQUIRE_ISOLATED_MARGIN", True))
        dry_run = bool(getattr(config, "EXECUTION_DRY_RUN_LOG_ONLY", True))
        return cls(
            mode=mode,
            confirm_required=confirm_required,
            max_leverage=max_lev,
            allow_symbols=allow,
            deny_symbols=deny,
            require_trade_plan_ok=require_plan,
            require_isolated_margin=require_iso,
            dry_run_log_only=dry_run,
        )


@dataclass(frozen=True)
class ExecutionGuardResult:
    allowed: bool
    reason: str


EXECUTION_GUARDED = "EXECUTION_GUARDED"


def check_execution_guard(settings: Optional[ExecutionSettings] = None) -> ExecutionGuardResult:
    """
    Global execution safety guard.
    - Always allow when mode == 'disabled' (no real execution).
    - For 'testnet'/'live', require:
        - KILL_SWITCH == 0
        - RISK_KILL_SWITCH == 0 (if present)
        - confirm_required == True
    """
    if settings is None:
        settings = ExecutionSettings.from_config()
    mode = settings.mode
    if mode in ("disabled", "paper"):
        return ExecutionGuardResult(allowed=True, reason="")

    kill_switch = bool(getattr(config, "KILL_SWITCH", False))
    risk_kill = bool(getattr(config, "RISK_KILL_SWITCH", False))
    if kill_switch or risk_kill or not settings.confirm_required:
        return ExecutionGuardResult(allowed=False, reason=EXECUTION_GUARDED)
    return ExecutionGuardResult(allowed=True, reason="")


@dataclass(frozen=True)
class OrderPlan:
    symbol: str
    side: str
    qty: float
    entry_price: float
    sl_price: float
    tp_price: float
    leverage: float
    margin_mode: str
    trading_stop: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "qty": self.qty,
            "entry_price": self.entry_price,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
            "leverage": self.leverage,
            "margin_mode": self.margin_mode,
            "trading_stop": dict(self.trading_stop),
        }


def _is_symbol_allowed(symbol: str, settings: ExecutionSettings) -> bool:
    s = symbol.upper()
    if settings.allow_symbols and s not in settings.allow_symbols:
        return False
    if settings.deny_symbols and s in settings.deny_symbols:
        return False
    return True


def _safe_leverage(plan: TradePlan, settings: ExecutionSettings) -> Tuple[float, Optional[str]]:
    """
    Derive final leverage from trade plan and settings.
    Refuse if required leverage is absurd (e.g. > 2x configured max or stop distance extremely tight).
    """
    stop_pct = float(plan.stop_distance_pct or 0.0)
    if stop_pct <= 0:
        return 0.0, "INVALID_STOP_DISTANCE"

    planned_lev = float(plan.leverage_recommended or 0.0)
    if planned_lev <= 0:
        # Approximate from stop distance: 1 / stop_fraction
        stop_frac = stop_pct / 100.0
        if stop_frac <= 0:
            return 0.0, "INVALID_STOP_DISTANCE"
        planned_lev = 1.0 / stop_frac

    if planned_lev > settings.max_leverage * 2.0:
        return 0.0, "LEVERAGE_ABSURD"
    if stop_pct < 0.05:
        return 0.0, "STOP_TOO_TIGHT"

    final_lev = min(planned_lev, settings.max_leverage)
    return float(final_lev), None


def build_order_plan(
    plan: TradePlan,
    *,
    settings: Optional[ExecutionSettings] = None,
) -> Optional[OrderPlan]:
    """
    Build a Bybit-compatible order/scenario from a canonical TradePlan.
    Does NOT send any real request; caller can use payloads for shadow-execution logging.
    Returns None when execution is guarded, plan invalid, or config refuses the trade.
    """
    if settings is None:
        settings = ExecutionSettings.from_config()

    guard = check_execution_guard(settings)
    if not guard.allowed:
        log.info("Execution guarded: %s", guard.reason)
        return None

    if settings.require_trade_plan_ok and (not plan.ok or not plan.execution_ready):
        log.info("Execution refused: invalid trade plan ok=%s execution_ready=%s", plan.ok, plan.execution_ready)
        return None
    if plan.degraded:
        log.info("Execution refused: trade plan degraded reason=%s", plan.reason)
        return None

    symbol = plan.symbol.upper()
    if not _is_symbol_allowed(symbol, settings):
        log.info("Execution refused: symbol %s not allowed by EXECUTION_ALLOW/DENY", symbol)
        return None

    side = plan.side.upper()
    if side not in {"LONG", "SHORT"}:
        log.info("Execution refused: invalid side=%s", side)
        return None

    if plan.qty_est <= 0 or plan.entry <= 0 or plan.stop <= 0 or plan.tp <= 0:
        log.info("Execution refused: invalid levels or qty (entry=%s sl=%s tp=%s qty=%s)", plan.entry, plan.stop, plan.tp, plan.qty_est)
        return None

    lev, lev_reason = _safe_leverage(plan, settings)
    if lev_reason is not None:
        log.info("Execution refused: %s", lev_reason)
        return None

    margin_mode = "isolated"
    if settings.require_isolated_margin and margin_mode != "isolated":
        log.info("Execution refused: margin_mode=%s != isolated", margin_mode)
        return None

    trading_stop = {
        "stopLoss": float(plan.stop),
        "takeProfit": float(plan.tp),
    }

    order = OrderPlan(
        symbol=symbol,
        side=side,
        qty=float(plan.qty_est),
        entry_price=float(plan.entry),
        sl_price=float(plan.stop),
        tp_price=float(plan.tp),
        leverage=lev,
        margin_mode=margin_mode,
        trading_stop=trading_stop,
    )

    log.info(
        "EXECUTION_ORDER_PLAN mode=%s symbol=%s side=%s qty=%.6f entry=%.4f sl=%.4f tp=%.4f lev=%.2f dry_run=%s",
        settings.mode,
        symbol,
        side,
        order.qty,
        order.entry_price,
        order.sl_price,
        order.tp_price,
        order.leverage,
        settings.dry_run_log_only or settings.mode in ("disabled", "paper"),
    )
    return order


def build_bybit_http_payloads(order: OrderPlan) -> Dict[str, Dict[str, Any]]:
    """
    Build raw payloads for Bybit REST API (v5) without sending them.
    Used for tests and shadow execution logging.
    """
    # Leverage: POST /v5/position/set-leverage
    lev_payload = {
        "category": "linear",
        "symbol": order.symbol,
        "buyLeverage": str(order.leverage if order.side == "LONG" else 1),
        "sellLeverage": str(order.leverage if order.side == "SHORT" else 1),
    }

    # Market order: POST /v5/order/create
    order_payload = {
        "category": "linear",
        "symbol": order.symbol,
        "side": "Buy" if order.side == "LONG" else "Sell",
        "orderType": "Market",
        "qty": str(order.qty),
        "timeInForce": "GoodTillCancel",
    }

    # Trading stop: POST /v5/position/trading-stop
    ts_payload = {
        "category": "linear",
        "symbol": order.symbol,
        "stopLoss": str(order.sl_price),
        "takeProfit": str(order.tp_price),
    }

    return {
        "set_leverage": lev_payload,
        "create_order": order_payload,
        "trading_stop": ts_payload,
    }

