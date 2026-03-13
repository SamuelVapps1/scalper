from __future__ import annotations

import argparse
import logging
import os
from typing import Any, Dict

import config
from scalper.execution_engine import ExecutionSettings, OrderPlan
from scalper.executor_live import safe_place_and_confirm_market_order


def _build_micro_order(symbol: str, side: str) -> OrderPlan:
    # Tiny synthetic plan: use PAPER_POSITION_USDT and PAPER_SL_ATR/TP_ATR as fallbacks.
    notional = float(getattr(config, "PAPER_POSITION_USDT", 20.0) or 20.0)
    price = float(os.getenv("SMOKE_PRICE", "100"))
    sl_pct = 0.01
    tp_pct = 0.015
    qty = notional / max(price, 1e-10)
    if side.upper() == "LONG":
        sl = price * (1 - sl_pct)
        tp = price * (1 + tp_pct)
    else:
        sl = price * (1 + sl_pct)
        tp = price * (1 - tp_pct)
    return OrderPlan(
        symbol=symbol.upper(),
        side=side.upper(),
        qty=qty,
        entry_price=price,
        sl_price=sl,
        tp_price=tp,
        leverage=1.0,
        margin_mode="isolated",
        trading_stop={"stopLoss": sl, "takeProfit": tp},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bybit live/testnet micro-order smoke tool.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--side", type=str, default="LONG")
    parser.add_argument(
        "--auto-exec",
        action="store_true",
        help="Actually place order (otherwise dry preview only).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    settings = ExecutionSettings.from_config()
    print(f"EXECUTION_MODE={settings.mode}")
    print(f"KILL_SWITCH={getattr(config, 'KILL_SWITCH', False)}")
    print(f"EXPLICIT_CONFIRM_EXECUTION={getattr(config, 'EXPLICIT_CONFIRM_EXECUTION', True)}")

    order = _build_micro_order(args.symbol, args.side)
    print("Planned micro order:")
    print(
        f"symbol={order.symbol} side={order.side} qty={order.qty:.6f} "
        f"entry={order.entry_price:.4f} sl={order.sl_price:.4f} tp={order.tp_price:.4f}"
    )

    if not args.auto_exec:
        print("Dry preview only (no order sent). Use --auto-exec to place order.")
        return

    result: Dict[str, Any] = safe_place_and_confirm_market_order(order)
    print("Execution result:")
    for k, v in result.items():
        if k == "raw_response":
            print(f"{k}=<redacted JSON of length {len(str(v))}>")
        else:
            print(f"{k}={v!r}")


if __name__ == "__main__":
    main()

