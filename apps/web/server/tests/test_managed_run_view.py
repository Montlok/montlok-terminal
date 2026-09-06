import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from managed_run_view import ManagedRunView
from group_views import GroupViews


class ManagedRunViewTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name)
        self.at = datetime.now(timezone.utc).isoformat()
        self.manifest = {"run_id": self.path.name, "group_id": "baseline", "capital_usdt": 123.45,
            "execution": "nautilus_sandbox", "account_type": "CASH", "leverage": 1,
            "market_data": "okx_public_live_websocket_l2", "instruments": {"BTC-USDT.OKX": {}, "ETH-USDT.OKX": {}},
            "signals_sha256": "signal-new", "runner_sha256": "strategy-new", "settings_sha256": "settings-new",
            "worker_sha256": "worker-new", "orders_enabled": True, "pid": 123, "process_create_time": 100.5}
        self.status = {"run_id": self.path.name, "group_id": "baseline", "observed_at": self.at, "trading_state": "REDUCING", "instruments_seen": 2}
        self.view = {"schemaVersion": 1, "runId": self.path.name, "groupId": "baseline", "observed_at": self.at,
            "completed": False, "tradingState": "REDUCING", "orders": [{"id": "owned-order", "status": "CANCELED"}],
            "fills": [{"id": "owned-fill", "quantity": "0.02", "price": "123.4"}],
            "positions": [{"instrument": "BTC-USDT.OKX", "quantity": 0.02}], "strategies": [{"id": "SECTOR-PAPER-001"}],
            "ordersTotal": 1, "fillsTotal": 1, "eventCount": 4, "connected": {"data": True, "exec": True}, "health": {"alerts": []}}
        self.adapter = ManagedRunView(self.path, expected_group_id="baseline")

    def tearDown(self):
        self.temporary.cleanup()

    def publish(self):
        for name, value in (("manifest.json", self.manifest), ("status.json", self.status), ("view.json", self.view)):
            (self.path / name).write_text(json.dumps(value))

    def test_run_budget_native_market_versions_and_actual_state(self):
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=True):
            result = self.adapter.snapshot()
        self.assertEqual(result["tradingState"], "REDUCING")
        self.assertEqual(result["orders"][0]["id"], "owned-order")
        self.assertEqual(result["fillsTotal"], 1)
        config = {row["key"]: row["value"] for row in result["runtimeConfig"]}
        self.assertEqual(config["初始模拟资金 / USDT"], 123.45)
        self.assertEqual(config["行情来源"], "okx_public_live_websocket_l2")
        self.assertEqual(config["策略版本"], "strategy-new")
        self.assertIn("2/2", result["systems"][0]["detail"])
        self.assertNotIn("25", result["systems"][0]["detail"])
        self.assertNotEqual(result["accountId"], "PAPER-OKX-001")

    def test_missing_mismatched_or_partial_views_do_not_claim_zero_orders(self):
        self.publish()
        for damaged in (None, {**self.view, "runId": "run-24h-02"}, {**self.view, "observed_at": "2026-01-01T00:00:00Z"},
                        {key: value for key, value in self.view.items() if key != "fills"}):
            if damaged is None:
                (self.path / "view.json").unlink(missing_ok=True)
            else:
                (self.path / "view.json").write_text(json.dumps(damaged))
            with self.subTest(damaged=damaged), patch.object(self.adapter, "process_alive", return_value=True):
                result = self.adapter.snapshot()
            self.assertFalse(result["sources"]["orders"])
            self.assertIsNone(result["ordersTotal"])
            self.assertIsNone(result["fillsTotal"])
            self.assertEqual(result["orders"], [])
            self.assertIn("view-source", [row["id"] for row in result["exceptions"]])

    def test_finished_and_dead_process_are_not_active(self):
        self.status["trading_state"] = "ACTIVE"
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=False):
            result = self.adapter.snapshot()
        self.assertEqual(result["tradingState"], "UNKNOWN")
        self.assertFalse(result["readyToTrade"])
        self.view["completed"] = True
        (self.path / "view.json").write_text(json.dumps(self.view))
        (self.path / "final.json").write_text(json.dumps(self.status))
        finished = self.adapter.snapshot()
        self.assertEqual(finished["tradingState"], "STOPPED")
        self.assertTrue(finished["sources"]["orders"])
        self.status["error"] = "market connection lost"
        (self.path / "final.json").write_text(json.dumps(self.status))
        self.assertEqual(self.adapter.snapshot()["tradingState"], "ERROR")

    def test_pid_identity_requires_creation_time_and_never_defaults_alive(self):
        self.assertFalse(self.adapter.process_alive({}))
        with patch("managed_run_view.psutil.Process") as process:
            process.return_value.create_time.return_value = 999.0
            process.return_value.is_running.return_value = True
            self.assertFalse(self.adapter.process_alive(self.manifest))
            process.return_value.create_time.return_value = 100.5
            self.assertTrue(self.adapter.process_alive(self.manifest))

    def test_live_inventory_run_is_labeled_and_projected_as_live(self):
        self.manifest.update(execution="okx_live", mode="live", account_id="tokyoreal", capital_usdt=None)
        self.status["trading_state"] = "ACTIVE"
        self.view["tradingState"] = "ACTIVE"
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=True):
            result = self.adapter.snapshot()
            group = GroupViews(self.path, self.adapter, group_id="baseline", group_name="实盘高频").detail("baseline")
        for value in (result, group):
            self.assertEqual(value["mode"], "live")
            self.assertEqual(value["modeLabel"], "OKX 实盘")
            self.assertEqual(value["matchingLabel"], "OKX 现货 · CASH · 1x")
            self.assertEqual(value["accountId"], "tokyoreal")
        self.assertTrue(result["readyToTrade"])

    def test_internally_consistent_foreign_group_is_rejected_against_expected_owner(self):
        self.manifest["group_id"] = "different-group"
        self.status["group_id"] = "different-group"
        self.view["groupId"] = "different-group"
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=True):
            result = self.adapter.snapshot()
        self.assertFalse(result["sources"]["manifest"])
        self.assertFalse(result["sources"]["status"])
        self.assertFalse(result["sources"]["orders"])
        self.assertIsNone(result["groupId"])
        self.assertEqual(result["orders"], [])
        self.assertFalse(result["readyToTrade"])

    def model_run(self):
        self.manifest.update(mode="shadow", orders_enabled=False, model_release="gru-v1", model_sha256="model-hash")
        self.status.update(trading_state="ACTIVE", model={"releaseId": "gru-v1", "modelHash": "model-hash",
            "mode": "shadow", "device": "cpu", "warmupComplete": True, "status": "active", "latencyMs": 2.5,
            "latestTarget": {"asOfNs": 1788600000000000000, "targetFraction": 0.0, "ordersSent": False}})
        self.view.update(tradingState="ACTIVE", model={**self.status["model"]})
        self.publish()

    def test_shadow_model_projection_is_run_bound_and_never_ready_to_trade(self):
        self.model_run()
        with patch.object(self.adapter, "process_alive", return_value=True):
            snapshot = self.adapter.snapshot()
            group = GroupViews(self.path, self.adapter, group_id="baseline", group_name="模型影子组").detail("baseline")
        for result in (snapshot, group):
            self.assertEqual(result["mode"], "shadow")
            self.assertEqual(result["modeLabel"], "影子运行")
            self.assertEqual(result["matchingLabel"], "预测与目标仓位")
            self.assertEqual(result["model"]["modelHash"], "model-hash")
            self.assertEqual(result["model"]["runId"], self.path.name)
            self.assertEqual(result["model"]["groupId"], "baseline")
            self.assertEqual(result["model"]["latencyMs"], 2.5)
        self.assertFalse(snapshot["readyToTrade"])
        self.assertEqual(snapshot["systems"][0]["level"], "HEALTHY")
        self.assertEqual({row["key"]: row["value"] for row in group["version"]}["撮合方式"], "预测与目标仓位")

    def test_model_falls_back_only_to_verified_current_view(self):
        self.model_run()
        self.status.pop("model")
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=True):
            result = self.adapter.snapshot()
        self.assertEqual(result["model"]["releaseId"], "gru-v1")
        self.view["observed_at"] = "2020-01-01T00:00:00+00:00"
        self.publish()
        with patch.object(self.adapter, "process_alive", return_value=True):
            result = self.adapter.snapshot()
        self.assertNotIn("model", result)
        self.assertFalse(result["sources"]["model"])

    def test_foreign_model_hash_or_explicit_run_id_is_not_projected(self):
        self.model_run()
        for fields in ({"modelHash": "different"}, {"releaseId": "different"}, {"runId": "different-run"}, {"groupId": "different-group"}):
            self.status["model"] = {"releaseId": "gru-v1", "modelHash": "model-hash", **fields}
            self.view["model"] = {**self.status["model"]}
            self.publish()
            with patch.object(self.adapter, "process_alive", return_value=True):
                result = self.adapter.snapshot()
                group = GroupViews(self.path, self.adapter, group_id="baseline").detail("baseline")
            self.assertNotIn("model", result)
            self.assertNotIn("model", group)
            self.assertIn("model-source", [row["id"] for row in result["exceptions"]])


if __name__ == "__main__":
    unittest.main()
