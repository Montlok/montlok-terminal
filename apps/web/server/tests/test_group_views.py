import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from group_views import GroupViews


def observation(index, nav=2000, drawdown=0):
    return {"observed_at": datetime.fromtimestamp(1788600000 + index, timezone.utc).isoformat(),
            "nav_usdt": nav, "pnl_usdt": nav - 2000, "max_drawdown": drawdown,
            "fees_usdt": 1.2, "fills": 28}


class GroupViewTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)
        self.paper = Mock()
        self.paper.snapshot.return_value = {
            "tradingState": "ACTIVE", "positions": [{"instrument": "XNVDA-USDT", "quantity": 2}],
            "orders": [{"id": "paper-order"}], "fills": [{"id": "paper-fill"}],
            "systems": [{"label": "行情", "level": "HEALTHY"}],
        }
        self.view = GroupViews(self.path, self.paper)
        self.view.CACHE_SECONDS = 0

    def tearDown(self):
        self.directory.cleanup()

    def write_rows(self, rows):
        (self.path / "equity.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))

    def write_status(self, value=None):
        status = value or observation(1, nav=2003, drawdown=-0.00268)
        status["observed_at"] = datetime.now(timezone.utc).isoformat()
        (self.path / "status.json").write_text(json.dumps(status))
        (self.path / "manifest.json").write_text(json.dumps({"signal_as_of": "2026-09-03", "signals_sha256": "signal-v1"}))

    def test_group_isolation_and_null_unstarted_metrics(self):
        self.write_status()
        baseline = self.view.detail("baseline")
        self.assertEqual(baseline["mode"], "nautilus_sandbox")
        self.assertEqual(baseline["accountId"], "PAPER-OKX-001")
        self.assertEqual(baseline["status"], "running")
        self.assertEqual(baseline["metrics"]["nav"], 2003)
        self.assertAlmostEqual(baseline["metrics"]["maxDrawdownPct"], -0.268)
        self.assertEqual(baseline["orders"], [{"id": "paper-order"}])
        count = self.paper.snapshot.call_count
        enhanced = self.view.detail("enhanced")
        self.assertEqual(self.paper.snapshot.call_count, count)
        self.assertIsNone(enhanced["mode"])
        self.assertIsNone(enhanced["runId"])
        self.assertEqual(enhanced["positions"], [])
        self.assertTrue(all(value is None for value in enhanced["metrics"].values()))
        self.assertEqual(self.view.equity("enhanced")["points"], [])
        self.assertEqual(enhanced["status"], "pending_validation")
        self.assertFalse(enhanced["capabilities"]["start"])

    def test_legacy_baseline_does_not_inherit_unbound_model_or_shadow_labels(self):
        self.write_status()
        manifest = json.loads((self.path / "manifest.json").read_text())
        manifest["mode"] = "shadow"
        (self.path / "manifest.json").write_text(json.dumps(manifest))
        self.paper.snapshot.return_value["model"] = {"releaseId": "foreign", "modelHash": "other"}
        result = self.view.detail("baseline")
        self.assertEqual(result["mode"], "nautilus_sandbox")
        self.assertEqual(result["modeLabel"], "本地模拟")
        self.assertNotIn("model", result)

    def test_stable_names_and_versions_are_independent_of_calendar_day(self):
        self.write_status()
        first = self.view.detail("baseline")
        self.assertEqual(first["name"], "四板块 60/40")
        self.assertEqual(first["signalAsOf"], "2026-09-03")
        self.assertEqual(first["signalVersion"], "signal-v1")
        self.assertNotIn("昨日", json.dumps(self.view.snapshot(), ensure_ascii=False))
        self.assertNotIn("基础组", json.dumps(self.view.snapshot(), ensure_ascii=False))
        self.assertEqual(self.view.detail("enhanced")["name"], "四板块 Alpha 叠加")

    def test_run_times_come_from_manifest_and_snapshot_not_current_clock(self):
        self.write_status()
        observed = datetime.now(timezone.utc).timestamp()
        manifest = {"signal_as_of": "2026-09-03", "signals_sha256": "signal-hash", "runner_sha256": "runner-hash",
                    "run_started_at": datetime.fromtimestamp(observed - 3600, timezone.utc).isoformat(),
                    "stop_at_unix": observed + 82800, "planned_seconds": 86400}
        (self.path / "manifest.json").write_text(json.dumps(manifest))
        status = json.loads((self.path / "status.json").read_text())
        status["observed_at"] = datetime.fromtimestamp(observed, timezone.utc).isoformat()
        (self.path / "status.json").write_text(json.dumps(status))
        first = self.view.detail("baseline")
        self.assertEqual(first["executionVersion"], "runner-hash")
        self.assertAlmostEqual(first["elapsedSeconds"], 3600)
        self.assertEqual(first["scheduledStopAt"], observed + 82800)
        self.assertIsNone(first["completedAt"])
        (self.path / "final.json").write_text(json.dumps(status))
        self.assertAlmostEqual(self.view.detail("baseline")["completedAt"], observed)
        self.assertAlmostEqual(self.view.detail("baseline")["elapsedSeconds"], 3600)

    def test_missing_run_metadata_is_not_replaced_with_now_or_account_data(self):
        self.write_status()
        first = self.view.detail("baseline")
        for field in ("startedAt", "scheduledStopAt", "elapsedSeconds", "completedAt", "executionVersion"):
            self.assertIsNone(first[field])
        enhanced = self.view.detail("enhanced")
        self.assertIsNone(enhanced["signalVersion"])
        self.assertIsNone(enhanced["signalAsOf"])

    def test_managed_metadata_uses_actual_run_budget_source_account_and_versions(self):
        self.write_status()
        manifest = {"run_id": self.path.name, "capital_usdt": 150,
                    "market_data": "okx_public_live_websocket_l2", "fee_assumption": {"maker_bps": 8, "taker_bps": 10},
                    "signals_sha256": "signal-two", "runner_sha256": "strategy-two", "worker_sha256": "worker-two",
                    "settings_sha256": "settings-two", "registry_sha256": "registry-two"}
        (self.path / "manifest.json").write_text(json.dumps(manifest))
        self.paper.snapshot.return_value.update(accountId=f"SANDBOX:{self.path.name}",
            sources={"manifest": True, "status": True, "view": True, "orders": True, "fills": True}, ordersTotal=8, fillsTotal=12)
        detail = self.view.detail("baseline")
        self.assertEqual(detail["accountId"], f"SANDBOX:{self.path.name}")
        self.assertEqual(detail["metrics"]["capital"], 150)
        self.assertEqual(detail["metrics"]["fills"], 12)
        self.assertEqual(detail["ordersTotal"], 8)
        version = {row["key"]: row["value"] for row in detail["version"]}
        self.assertEqual(version["行情来源"], "OKX 公共 WebSocket 订单簿")
        self.assertEqual(version["手续费假设"], "Maker 8 / Taker 10 bps")
        self.assertEqual(version["配置版本"], "settings-two")
        self.assertEqual(version["运行器版本"], "worker-two")

    def test_final_error_stop_and_deadline_completion_are_distinct(self):
        self.write_status()
        status = json.loads((self.path / "status.json").read_text())
        self.paper.snapshot.return_value["tradingState"] = "STOPPED"
        for fields, expected in (({"trading_state": "ERROR", "deadline_reached": True}, "error"),
                                 ({"trading_state": "STOPPED", "deadline_reached": False}, "stopped"),
                                 ({"trading_state": "STOPPED", "deadline_reached": True}, "completed")):
            (self.path / "final.json").write_text(json.dumps({**status, **fields}))
            self.assertEqual(self.view.detail("baseline")["status"], expected)

    def test_missing_projection_is_unknown_and_invalid_status_is_not_a_metric_source(self):
        self.write_status()
        self.paper.snapshot.return_value.update(sources={"view": False, "orders": False, "fills": False},
            orders=[], fills=[], ordersTotal=None, fillsTotal=None)
        detail = self.view.detail("baseline")
        self.assertFalse(detail["sources"]["orders"])
        self.assertIsNone(detail["ordersTotal"])
        self.assertIsNone(detail["metrics"]["fills"])
        self.assertFalse(detail["health"]["detailAvailable"])
        self.paper.snapshot.return_value["sources"].update(status=False, manifest=False)
        detail = self.view.detail("baseline")
        self.assertIsNone(detail["metrics"]["nav"])
        self.assertIsNone(detail["metrics"]["capital"])
        self.assertEqual(detail["status"], "unavailable")

    def test_managed_provenance_uses_only_own_published_metadata(self):
        self.write_status()
        alpha = [{"name": "Published momentum", "rule": "EMA 15/50", "source": "release:42", "asOf": "2026-09-04"}]
        manifest = {"run_id": self.path.name, "group_id": "custom", "group_name": "Published model group",
                    "strategy_description": "Frozen release 42", "alpha_metadata": alpha,
                    "signals_sha256": "own-signal", "runner_sha256": "own-strategy"}
        (self.path / "manifest.json").write_text(json.dumps(manifest))
        self.paper.snapshot.return_value["sources"] = {"manifest": True, "status": True, "view": True}
        adapter = GroupViews(self.path, self.paper, group_id="custom", group_name="Registry label")
        detail = adapter.detail("custom")
        self.assertEqual(detail["name"], "Registry label")
        self.assertEqual(detail["description"], "Frozen release 42")
        self.assertEqual(detail["alpha"], alpha)
        self.assertEqual({row["key"]: row["value"] for row in detail["version"]}["策略组"], "Registry label")
        self.assertNotIn("MA20/100", json.dumps(detail))

    def test_managed_missing_metadata_never_inherits_legacy_baseline_rules(self):
        self.write_status()
        (self.path / "manifest.json").write_text(json.dumps({"run_id": self.path.name, "group_id": "custom"}))
        self.paper.snapshot.return_value["sources"] = {"manifest": True, "status": True, "view": True}
        adapter = GroupViews(self.path, self.paper, group_id="custom")
        detail = adapter.detail("custom")
        self.assertEqual(detail["name"], "custom")
        self.assertEqual(detail["description"], "未记录")
        self.assertEqual(detail["alpha"], [])
        self.assertIsNone({row["key"]: row["value"] for row in detail["version"]}["撮合方式"])
        self.assertNotIn("四板块", json.dumps(detail, ensure_ascii=False))
        adapter.CACHE_SECONDS = 0
        (self.path / "manifest.json").unlink()
        self.paper.snapshot.return_value = {}
        missing = adapter.detail("custom")
        self.assertEqual(missing["alpha"], [])
        self.assertEqual(missing["description"], "未记录")

    def test_no_arbitrary_path_or_exchange_id_is_accepted(self):
        for group_id in ("../../identity", "live", "demo", self.path.name):
            with self.assertRaises(KeyError):
                self.view.detail(group_id)
            with self.assertRaises(KeyError):
                self.view.equity(group_id)
        self.paper.snapshot.assert_not_called()

    def test_summary_cannot_mutate_cached_details(self):
        self.write_status()
        self.view.CACHE_SECONDS = 5
        summary = self.view.snapshot()
        self.assertNotIn("positions", summary["groups"][0])
        summary["groups"][0]["metrics"]["nav"] = 0
        self.assertEqual(self.view.detail("baseline")["metrics"]["nav"], 2003)
        self.assertEqual(self.paper.snapshot.call_count, 1)

    def test_missing_and_malformed_files_do_not_claim_running(self):
        self.paper.snapshot.side_effect = ValueError("bad JSONL")
        detail = self.view.detail("baseline")
        self.assertEqual(detail["status"], "unavailable")
        self.assertIsNone(detail["metrics"]["nav"])
        self.assertFalse(detail["health"]["detailAvailable"])
        self.assertEqual(self.view.equity("baseline")["points"], [])
        (self.path / "status.json").write_text('{"nav_usdt":')
        self.assertEqual(self.view.detail("baseline")["status"], "unavailable")

    def test_stale_and_final_states_are_distinct(self):
        (self.path / "status.json").write_text(json.dumps(observation(1)))
        self.assertEqual(self.view.detail("baseline")["status"], "stale")
        (self.path / "final.json").write_text(json.dumps(observation(1)))
        self.assertEqual(self.view.detail("baseline")["status"], "completed")

    def test_fresh_halted_or_recovery_is_not_reported_as_stopped(self):
        self.write_status()
        for engine_state, expected in (("HALTED", "halted"), ("REDUCING", "reducing"),
                                       ("RECOVERING", "recovering"), ("ERROR", "error"),
                                       ("STOPPED", "stopped"), ("MYSTERY", "unknown")):
            with self.subTest(engine_state=engine_state):
                self.paper.snapshot.return_value["tradingState"] = engine_state
                detail = self.view.detail("baseline")
                self.assertEqual(detail["status"], expected)
                self.assertEqual(detail["health"]["tradingState"], engine_state)

    def test_running_status_does_not_substitute_for_missing_history(self):
        self.write_status()
        self.assertEqual(self.view.detail("baseline")["status"], "running")
        curve = self.view.equity("baseline")
        self.assertFalse(curve["sourceAvailable"])
        self.assertEqual(curve["sourceStatus"], "missing")
        self.assertEqual(curve["sourceIssue"], "未读取到 equity.jsonl")
        self.assertEqual(curve["points"], [])
        self.assertFalse(curve["partial"])
        self.assertEqual(self.view.equity("enhanced")["sourceStatus"], "unconfigured")

    def test_disappeared_history_retains_points_with_explicit_source_loss(self):
        self.write_rows([observation(1), observation(2)])
        original = self.view.equity("baseline")
        self.assertTrue(original["sourceAvailable"])
        self.assertEqual(original["sourceStatus"], "available")
        (self.path / "equity.jsonl").unlink()
        missing = self.view.equity("baseline")
        self.assertEqual(missing["points"], original["points"])
        self.assertFalse(missing["sourceAvailable"])
        self.assertEqual(missing["sourceStatus"], "missing")
        self.assertFalse(missing["partial"])
        self.write_rows([observation(3)])
        restored = self.view.equity("baseline")
        self.assertTrue(restored["sourceAvailable"])
        self.assertIsNone(restored["sourceIssue"])

    def test_empty_history_is_distinct_from_missing_history(self):
        self.write_rows([])
        curve = self.view.equity("baseline")
        self.assertTrue(curve["sourceAvailable"])
        self.assertEqual(curve["sampleCount"], 0)
        self.assertEqual(curve["sourceStatus"], "available")

    def test_incremental_partial_line_and_bad_records(self):
        first, second = json.dumps(observation(1)), json.dumps(observation(2, 2001))
        path = self.path / "equity.jsonl"
        path.write_text(first + "\n{bad json}\n[]\n" + second[:30])
        initial = self.view.equity("baseline")
        self.assertEqual(initial["sampleCount"], 1)
        self.assertEqual(initial["invalidLines"], 2)
        self.assertTrue(initial["partial"])
        with path.open("a") as stream:
            stream.write(second[30:] + "\n")
        result = self.view.equity("baseline")
        self.assertEqual(result["sampleCount"], 2)
        self.assertEqual(result["points"][-1]["value"], 2001)
        self.assertFalse(result["partial"])
        self.assertEqual(self.view.equity("baseline")["sampleCount"], 2)

    def test_nonfinite_nav_and_reversed_timestamps_are_skipped(self):
        self.write_rows([observation(2), observation(1), observation(3, float("nan")), observation(4, True), observation(5, -1)])
        result = self.view.equity("baseline")
        self.assertEqual(result["sampleCount"], 1)
        self.assertEqual(result["invalidLines"], 4)

    def test_truncation_and_replacement_reset_curve(self):
        self.write_rows([observation(1), observation(2), observation(3)])
        self.assertEqual(self.view.equity("baseline")["sampleCount"], 3)
        self.write_rows([observation(10, 2010)])
        self.assertEqual(self.view.equity("baseline")["sampleCount"], 1)
        replacement = self.path / "replacement.jsonl"
        replacement.write_text(json.dumps(observation(20, 2020)) + "\n")
        replacement.replace(self.path / "equity.jsonl")
        result = self.view.equity("baseline")
        self.assertEqual(result["sampleCount"], 1)
        self.assertEqual(result["points"][0]["value"], 2020)

    def test_curve_cache_avoids_rereading_when_polled(self):
        self.write_rows([observation(1)])
        self.view.CACHE_SECONDS = 60
        first = self.view.equity("baseline")
        with (self.path / "equity.jsonl").open("a") as stream:
            stream.write(json.dumps(observation(2)) + "\n")
        self.assertEqual(self.view.equity("baseline"), first)
        self.view.curve_at -= 61
        self.assertEqual(self.view.equity("baseline")["sampleCount"], 2)

    def test_oversized_line_does_not_block_following_observations(self):
        self.view.MAX_LINE = 256
        path = self.path / "equity.jsonl"
        path.write_text("x" * 5000 + "\n" + json.dumps(observation(1)) + "\n")
        self.view.equity("baseline")
        result = self.view.equity("baseline")
        self.assertEqual(result["sampleCount"], 1)
        self.assertEqual(result["invalidLines"], 1)

    def test_bounded_load_preserves_nav_and_recorded_drawdown_extremes(self):
        self.view.MAX_POINTS = 80
        self.view.MAX_BYTES = 64 * 1024
        rows = [observation(i, 2000 + i % 40, -0.005) for i in range(5000)]
        rows[876] = observation(876, 1800, -0.25)
        rows[3999] = observation(3999, 2200, -0.01)
        self.write_rows(rows)
        begin = time.monotonic()
        for _ in range(100):
            result = self.view.equity("baseline")
            self.assertLessEqual(len(result["points"]), self.view.MAX_POINTS)
            if not result["partial"]:
                break
        self.assertFalse(result["partial"])
        self.assertEqual(result["sampleCount"], 5000)
        self.assertEqual(min(point["value"] for point in result["points"]), 1800)
        self.assertEqual(max(point["value"] for point in result["points"]), 2200)
        self.assertEqual(min(point["value"] for point in result["drawdown"]), -25)
        self.assertLess(time.monotonic() - begin, 5)


if __name__ == "__main__":
    unittest.main()
