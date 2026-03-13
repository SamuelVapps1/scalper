"""
Tests for execution module.
"""
from __future__ import annotations

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

import importlib


class TestExecutionGuard(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {}
        for k in ("EXECUTION_MODE", "KILL_SWITCH", "EXPLICIT_CONFIRM_EXECUTION", "RISK_KILL_SWITCH"):
            self._saved[k] = os.environ.pop(k, None)

    def tearDown(self) -> None:
        for k, v in self._saved.items():
            if v is not None:
                os.environ[k] = v
            elif k in os.environ:
                del os.environ[k]
        import config

        importlib.reload(config)

    def test_disabled_mode_allows(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import config

        importlib.reload(config)
        from scalper.execution_engine import check_execution_guard

        res = check_execution_guard()
        self.assertTrue(res.allowed)
        self.assertEqual(res.reason, "")

    def test_testnet_mode_blocks_when_kill_switch_on(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "1"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "1"
        import config

        importlib.reload(config)
        from scalper.execution_engine import check_execution_guard, EXECUTION_GUARDED

        res = check_execution_guard()
        self.assertFalse(res.allowed)
        self.assertEqual(res.reason, EXECUTION_GUARDED)

    def test_testnet_mode_blocks_when_explicit_confirm_off(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "0"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "0"
        import config

        importlib.reload(config)
        from scalper.execution_engine import check_execution_guard, EXECUTION_GUARDED

        res = check_execution_guard()
        self.assertFalse(res.allowed)
        self.assertEqual(res.reason, EXECUTION_GUARDED)

    def test_testnet_mode_allows_when_guards_ok(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "0"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "1"
        import config

        importlib.reload(config)
        from scalper.execution_engine import check_execution_guard

        res = check_execution_guard()
        self.assertTrue(res.allowed)
        self.assertEqual(res.reason, "")


class TestBuildOrderPlan(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {}
        for k in ("EXECUTION_MODE", "KILL_SWITCH", "EXPLICIT_CONFIRM_EXECUTION"):
            self._saved[k] = os.environ.pop(k, None)

    def tearDown(self) -> None:
        for k, v in self._saved.items():
            if v is not None:
                os.environ[k] = v
            elif k in os.environ:
                del os.environ[k]
        import config

        importlib.reload(config)

    def _plan(self, **overrides: Any) -> "TradePlan":
        from scalper.trade_plan import TradePlan

        base = dict(
            ok=True,
            reason="",
            symbol="BTCUSDT",
            side="LONG",
            strategy="TEST",
            timeframe="15",
            confidence=0.7,
            entry=50000.0,
            stop=49000.0,
            tp=51500.0,
            rr=2.0,
            risk_pct=1.0,
            stop_distance_pct=2.0,
            leverage_recommended=5.0,
            leverage_cap_applied=False,
            position_value_usdt=100.0,
            qty_est=0.01,
            notional_est=500.0,
            resistance_1=None,
            support_1=None,
            atr14_used=100.0,
            atr_source="test",
            degraded=False,
            execution_ready=True,
            bar_ts_used="2026-01-01T12:00:00+00:00",
            notes=[],
        )
        base.update(overrides)
        return TradePlan(**base)

    def test_returns_none_when_execution_guarded(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "1"
        import config

        importlib.reload(config)
        from scalper.execution_engine import build_order_plan

        plan = self._plan()
        order = build_order_plan(plan)
        self.assertIsNone(order)

    def test_returns_order_when_disabled_and_valid_plan(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import config

        importlib.reload(config)
        from scalper.execution_engine import build_order_plan

        plan = self._plan()
        order = build_order_plan(plan)
        self.assertIsNotNone(order)
        assert order is not None
        self.assertEqual(order.symbol, "BTCUSDT")
        self.assertEqual(order.side, "LONG")
        self.assertGreater(order.qty, 0)
        self.assertEqual(order.sl_price, 49000.0)
        self.assertEqual(order.tp_price, 51500.0)

    def test_returns_none_when_invalid_side(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import config

        importlib.reload(config)
        from scalper.execution_engine import build_order_plan

        plan = self._plan(side="INVALID")
        order = build_order_plan(plan)
        self.assertIsNone(order)

    def test_bybit_payload_generation(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import config

        importlib.reload(config)
        from scalper.execution_engine import build_order_plan, build_bybit_http_payloads

        plan = self._plan()
        order = build_order_plan(plan)
        assert order is not None
        payloads = build_bybit_http_payloads(order)
        self.assertIn("set_leverage", payloads)
        self.assertIn("create_order", payloads)
        self.assertIn("trading_stop", payloads)
        self.assertEqual(payloads["create_order"]["symbol"], "BTCUSDT")
        self.assertEqual(payloads["trading_stop"]["stopLoss"], str(order.sl_price))
        self.assertEqual(payloads["trading_stop"]["takeProfit"], str(order.tp_price))


if __name__ == "__main__":
    unittest.main()
