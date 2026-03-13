"""
Tests for execution module.
"""
from __future__ import annotations

import os
import unittest

# Ensure project root on path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


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
        # Force config reload
        import importlib
        import config
        importlib.reload(config)

    def test_disabled_mode_allows(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import importlib
        import config
        importlib.reload(config)
        from execution import check_execution_guard
        allowed, reason = check_execution_guard()
        self.assertTrue(allowed)
        self.assertEqual(reason, "")

    def test_testnet_mode_blocks_when_kill_switch_on(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "1"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "1"
        import importlib
        import config
        importlib.reload(config)
        from execution import check_execution_guard, EXECUTION_GUARDED
        allowed, reason = check_execution_guard()
        self.assertFalse(allowed)
        self.assertEqual(reason, EXECUTION_GUARDED)

    def test_testnet_mode_blocks_when_explicit_confirm_off(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "0"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "0"
        import importlib
        import config
        importlib.reload(config)
        from execution import check_execution_guard, EXECUTION_GUARDED
        allowed, reason = check_execution_guard()
        self.assertFalse(allowed)
        self.assertEqual(reason, EXECUTION_GUARDED)

    def test_testnet_mode_allows_when_guards_ok(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "0"
        os.environ["EXPLICIT_CONFIRM_EXECUTION"] = "1"
        import importlib
        import config
        importlib.reload(config)
        from execution import check_execution_guard
        allowed, reason = check_execution_guard()
        self.assertTrue(allowed)
        self.assertEqual(reason, "")


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
        import importlib
        import config
        importlib.reload(config)

    def test_returns_none_when_execution_guarded(self) -> None:
        os.environ["EXECUTION_MODE"] = "testnet"
        os.environ["KILL_SWITCH"] = "1"
        import importlib
        import config
        importlib.reload(config)
        from execution import build_order_plan
        intent = {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 50000.0,
            "sl_price": 49000.0,
            "tp_price": 51500.0,
        }
        plan = build_order_plan(intent)
        self.assertIsNone(plan)

    def test_returns_plan_when_disabled_and_valid_intent(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import importlib
        import config
        importlib.reload(config)
        from execution import build_order_plan
        intent = {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 50000.0,
            "sl_price": 49000.0,
            "tp_price": 51500.0,
        }
        plan = build_order_plan(intent)
        self.assertIsNotNone(plan)
        self.assertEqual(plan["symbol"], "BTCUSDT")
        self.assertEqual(plan["side"], "LONG")
        self.assertEqual(plan["entry_type"], "market")
        self.assertIn("qty", plan)
        self.assertGreater(plan["qty"], 0)
        self.assertEqual(plan["sl"], 49000.0)
        self.assertEqual(plan["tp"], 51500.0)

    def test_returns_none_when_no_entry_price(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import importlib
        import config
        importlib.reload(config)
        from execution import build_order_plan
        intent = {"symbol": "BTCUSDT", "side": "LONG"}
        plan = build_order_plan(intent)
        self.assertIsNone(plan)

    def test_returns_none_when_invalid_side(self) -> None:
        os.environ["EXECUTION_MODE"] = "disabled"
        import importlib
        import config
        importlib.reload(config)
        from execution import build_order_plan
        intent = {"symbol": "BTCUSDT", "side": "INVALID", "entry_price": 50000.0}
        plan = build_order_plan(intent)
        self.assertIsNone(plan)


if __name__ == "__main__":
    unittest.main()
