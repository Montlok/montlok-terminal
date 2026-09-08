import asyncio
import hashlib
import json
import os
import tempfile
import sys
import unittest
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import psutil

from group_runtime import GroupRuntimeClient, GroupSupervisor, LaunchRegistry, encode, money, trusted_file


def worker_module():
    source = Path(__file__).resolve().parents[2] / "engine" / "group_worker.py"
    module_spec = importlib.util.spec_from_file_location("group_worker_transport_test", source)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


FAKE_WORKER = '''import asyncio, importlib.util, json, os, signal, sys
from pathlib import Path
module_spec = importlib.util.spec_from_file_location("fixture_control", "__CONTROL_MODULE__")
control_module = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(control_module)
request_path = Path(sys.argv[sys.argv.index("--request") + 1])
request = json.loads(request_path.read_text())
directory = request_path.parent
(directory / "environment.json").write_text(json.dumps(dict(os.environ)))
async def main():
    stopped = asyncio.Event()
    state = "ACTIVE"
    applied = 0
    def status(_request):
        return {"pid": os.getpid(), "environment": "sandbox", "tradingState": state,
                "connected": {"data": True, "exec": True}, "applied": applied}
    async def change(value):
        nonlocal state, applied
        applied += 1
        state = {"halt": "HALTED", "reduce": "REDUCING", "resume": "ACTIVE"}.get(value["command"], state)
        await asyncio.sleep(0.04)
        return status(value)
    server = control_module.ControlServer(directory / "control.sock", {"status": status, "halt": change, "reduce": change, "resume": change})
    await server.start()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopped.set)
    await stopped.wait()
    await server.stop()
    (directory / "final.json").write_text(json.dumps({"run_id": request["runId"], "group_id": request["groupId"],
                                                    "deadline_reached": False, "error": None}))
    for name in ("orders.csv", "fills.csv", "positions.csv", "account.csv"):
        (directory / name).write_text("id,value\\n")
asyncio.run(main())
'''


class RegistryFixture:
    def setup_registry(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="montlok-groups-", dir="/tmp")
        self.path = Path(self.temporary.name).resolve()
        self.runtime = self.path / "runtime"
        self.runtime.mkdir()
        for name in ("settings.py", "run.py", "node_config.py", "health.py", "commands.py", "control.py", "alerts.py"):
            (self.runtime / name).write_text("# local fixture\n")
        self.worker = self.path / "worker.py"
        control_path = Path(__file__).resolve().parents[2] / "engine" / "control.py"
        self.worker.write_text(FAKE_WORKER.replace("__CONTROL_MODULE__", str(control_path)))
        self.settings = self.path / "settings.json"
        self.settings.write_text(json.dumps({"environment": "sandbox", "instruments": ["BTC-USDT.OKX"],
            "strategies": [], "risk": {"max_notional_per_order": {"BTC-USDT.OKX": 250}}}))
        self.signals = self.path / "signals.json"
        self.signals.write_text(json.dumps({"execution": "nautilus_sandbox", "account_type": "CASH",
            "leverage": 1, "instruments": {"BTC-USDT.OKX": {"weight": 0.10}}}))
        self.strategy = self.path / "strategy.py"
        self.strategy.write_text("class SectorPaper: pass\n")
        self.registry_path = self.path / "registry.json"
        self.spec = {"id": "baseline", "name": "四板块 60/40", "enabled": True,
                     "environment": "sandbox", "defaultBudgetUsdt": "2000", "maxBudgetUsdt": "2000",
                     "maxDurationSeconds": 86400, "runtimePath": str(self.runtime)}
        for label, source in (("settings", self.settings), ("signals", self.signals), ("strategy", self.strategy)):
            self.spec.update({f"{label}Path": str(source), f"{label}Sha256": hashlib.sha256(source.read_bytes()).hexdigest()})
        # macOS framework launchers exec Python.app and rewrite argv[0]. Use the
        # final binary so the exact command-line ownership test remains strict.
        self.document = {"version": 1, "pythonPath": psutil.Process().exe(),
                         "workerPath": str(self.worker), "groups": [self.spec]}
        self.save_registry()

    def save_registry(self):
        self.registry_path.write_text(json.dumps(self.document))
        self.registry = LaunchRegistry(self.registry_path, strict=False)


class RegistryTests(RegistryFixture, unittest.TestCase):
    def setUp(self):
        self.setup_registry()

    def tearDown(self):
        self.temporary.cleanup()

    def test_only_reviewed_sandbox_cash_spot_inputs_are_accepted(self):
        self.assertEqual(self.registry.validate_inputs("baseline")["id"], "baseline")
        for field, bad in (("execution", "live"), ("account_type", "MARGIN"), ("leverage", 3), ("instrument_type", "SWAP")):
            data = json.loads(self.signals.read_text())
            original = data[field] if field in data else None
            data[field] = bad
            self.signals.write_text(json.dumps(data))
            self.spec["signalsSha256"] = hashlib.sha256(self.signals.read_bytes()).hexdigest()
            self.save_registry()
            with self.assertRaises(ValueError):
                self.registry.validate_inputs("baseline")
            if original is None:
                data.pop(field)
            else:
                data[field] = original
            self.signals.write_text(json.dumps(data))

    def test_live_registry_and_unknown_or_disabled_groups_rejected(self):
        with self.assertRaises(ValueError):
            self.registry.spec("../../identity")
        self.spec["environment"] = "live"
        with self.assertRaises(ValueError):
            self.save_registry()
        self.spec.update(environment="sandbox", enabled=False, reason="组合验证未发布")
        self.save_registry()
        with self.assertRaisesRegex(ValueError, "组合验证"):
            self.registry.validate_inputs("baseline")

    def test_inputs_and_registry_changes_invalidate_launch(self):
        self.strategy.write_text("# changed\n")
        with self.assertRaisesRegex(ValueError, "发布版本"):
            self.registry.validate_inputs("baseline")
        self.spec["enabled"] = False
        self.registry_path.write_text(json.dumps(self.document))
        with self.assertRaisesRegex(ValueError, "运行注册已变化"):
            self.registry.validate_inputs("baseline")

    def test_strict_mode_rejects_user_writable_code_and_relative_paths(self):
        with self.assertRaises(ValueError):
            trusted_file(self.registry_path)
        with self.assertRaises(ValueError):
            trusted_file(Path("relative.json"), strict=False)

    def test_money_rejects_nonfinite_negative_precision_and_boolean(self):
        self.assertEqual(money("12.3"), "12.30")
        for value in (True, None, {}, float("nan"), float("inf"), 0, -1, "1.001", "1000000001"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                money(value)

    def test_missing_per_order_limits_is_not_a_valid_launch(self):
        self.settings.write_text(json.dumps({"environment": "sandbox", "instruments": ["BTC-USDT.OKX"]}))
        self.spec["settingsSha256"] = hashlib.sha256(self.settings.read_bytes()).hexdigest()
        self.save_registry()
        with self.assertRaisesRegex(ValueError, "单笔名义上限"):
            self.registry.validate_inputs("baseline")

    def test_verify_only_is_a_strict_deployment_boolean(self):
        self.spec["verifyOnly"] = "true"
        with self.assertRaisesRegex(ValueError, "verifyOnly"):
            self.save_registry()

    def test_transport_defaults_to_native_and_only_root_published_choices_work(self):
        self.assertEqual(self.registry.spec("baseline")["marketTransport"], "native_ws")
        self.spec["marketTransport"] = "public_rest_l2"
        self.save_registry()
        self.assertEqual(self.registry.validate_inputs("baseline")["marketTransport"], "public_rest_l2")
        for value in ("ws_insecure", "http", True, {}, None):
            self.spec["marketTransport"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "marketTransport"):
                self.save_registry()

    def test_rest_factory_is_the_reviewed_release_factory_not_a_new_adapter(self):
        worker = worker_module()
        public_rest = object()
        native = object()
        release = SimpleNamespace(PublicOKXFactory=public_rest)
        self.assertIs(worker.market_factory({}, release, native), native)
        self.assertIs(worker.market_factory({"marketTransport": "public_rest_l2"}, release, native), public_rest)
        with self.assertRaises(ValueError):
            worker.market_factory({"marketTransport": "unsupported"}, release, native)

    def test_connection_and_quotes_are_both_required_for_successful_completion(self):
        worker = worker_module()
        self.assertIsNotNone(worker.readiness_error(False, 100))
        self.assertIsNotNone(worker.readiness_error(True, 0))
        self.assertIsNone(worker.readiness_error(True, 1))

    def test_worker_admission_preserves_group_and_budget_boundaries(self):
        source = Path(__file__).resolve().parents[2] / "engine" / "group_worker.py"
        module_spec = importlib.util.spec_from_file_location("group_worker_under_test", source)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        directory = self.path / "baseline-0123456789abcdef"
        directory.mkdir()
        request_path = directory / "request.json"
        request = {"groupId": "baseline", "action": "start", "registryVersion": self.registry.digest,
                   "budgetUsdt": "100.00", "durationSeconds": 120, "createdAt": 100,
                   "runId": directory.name}
        request_path.write_text(json.dumps(request))
        checked, spec = module.validate_launch(self.registry, request_path)
        self.assertEqual(checked["budgetUsdt"], "100.00")
        self.assertEqual(spec["environment"], "sandbox")
        for fields in ({"environment": "live"}, {"verifyOnly": False}, {"budgetUsdt": "2001"},
                       {"registryVersion": "wrong"}, {"action": "resume"}, {"runId": "baseline-other"}):
            request_path.write_text(json.dumps({**request, **fields}))
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                module.validate_launch(self.registry, request_path)


class SupervisorTests(RegistryFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.setup_registry()
        self.supervisor = GroupSupervisor(self.registry, self.path / "state")
        self.socket = self.path / "supervisor.sock"

    async def asyncTearDown(self):
        if self.supervisor.server:
            self.supervisor.server.close()
            await self.supervisor.server.wait_closed()
        for run_id, process in list(self.supervisor.processes.items()):
            if process.returncode is None:
                process.terminate()
                await process.wait()
        if self.supervisor.tasks:
            await asyncio.gather(*self.supervisor.tasks)
        self.supervisor.db.close()
        self.temporary.cleanup()

    async def launch(self):
        prepared = await self.supervisor.prepare({"groupId": "baseline", "action": "start", "budgetUsdt": "100"})
        result = await self.supervisor.execute(prepared["request"], "operation_start_001")
        self.assertEqual(result["status"], "starting")
        run_id = result["runId"]
        row = self.supervisor.row(run_id, "baseline")
        process = psutil.Process(row["pid"])
        self.assertEqual(process.cmdline(), [str(self.registry.python), str(self.registry.worker), "--registry",
                         str(self.registry.path), "--request", str(self.path / "state" / run_id / "request.json")])
        self.assertEqual(process.create_time(), row["process_created"])
        self.assertEqual(process.uids().real, os.getuid())
        self.assertNotEqual(process.status(), psutil.STATUS_ZOMBIE)
        self.assertIsNotNone(self.supervisor.owned_process(row), dict(row))
        for _ in range(100):
            snapshot = await self.supervisor.status("baseline")
            if snapshot["groups"][0]["runs"][0]["status"] == "running":
                break
            await asyncio.sleep(0.01)
        self.assertEqual(snapshot["groups"][0]["runs"][0]["status"], "running",
                         (self.path / "state" / run_id / "engine.log").read_text())
        return prepared["request"], result

    async def test_preflight_is_non_mutating_and_blocks_paths_modes_and_limits(self):
        prepared = await self.supervisor.prepare({"groupId": "baseline", "action": "start", "budgetUsdt": 100})
        self.assertEqual(prepared["request"]["budgetUsdt"], "100.00")
        self.assertEqual(self.supervisor.rows("baseline"), [])
        for fields in ({"executable": "/bin/sh"}, {"environment": "live"}, {"budgetUsdt": "2000.01"},
                       {"durationSeconds": True}, {"durationSeconds": 59}, {"runId": "run-24h-02"}, {"strategyPath": "evil.py"},
                       {"marketTransport": "public_rest_l2"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                await self.supervisor.prepare({"groupId": "baseline", "action": "start", **fields})

    async def test_registry_reload_updates_launch_metadata_without_restarting_supervisor(self):
        previous = self.supervisor.registry.digest
        self.document["groups"][0]["name"] = "四板块 60/40 · 新版"
        self.registry_path.write_text(json.dumps(self.document))

        result = await self.supervisor.reload_registry()

        self.assertEqual(result["previousRegistryVersion"], previous)
        self.assertNotEqual(result["registryVersion"], previous)
        self.assertEqual(self.supervisor.registry.spec("baseline")["name"], "四板块 60/40 · 新版")

    async def test_registry_reload_preserves_a_running_instance(self):
        _, run = await self.launch()
        previous = self.supervisor.registry.digest
        self.document["groups"][0]["name"] = "不应在运行中切换"
        self.registry_path.write_text(json.dumps(self.document))

        with self.assertRaisesRegex(ValueError, run["runId"]):
            await self.supervisor.reload_registry()

        self.assertEqual(self.supervisor.registry.digest, previous)
        self.assertIsNotNone(self.supervisor.owned_process(self.supervisor.row(run["runId"], "baseline")))

    async def test_start_dedup_controls_and_exact_stop(self):
        request, result = await self.launch()
        repeated = await self.supervisor.execute(request, "operation_start_001")
        self.assertEqual(result, repeated)
        self.assertEqual(len(self.supervisor.rows("baseline")), 1)
        with self.assertRaises(ValueError):
            await self.supervisor.execute({**request, "budgetUsdt": "101.00"}, "operation_start_001")
        with self.assertRaisesRegex(ValueError, "已有运行"):
            await self.supervisor.prepare({"groupId": "baseline", "action": "start"})
        for action, expected in (("halt", "halted"), ("reduce", "reducing"), ("resume", "running"), ("stop", "stopping")):
            prepared = await self.supervisor.prepare({"groupId": "baseline", "action": action, "runId": result["runId"]})
            reply = await self.supervisor.execute(prepared["request"], "operation_" + action + "_001")
            self.assertEqual(reply["status"], expected)
        await asyncio.gather(*self.supervisor.tasks)
        self.assertEqual((await self.supervisor.status("baseline"))["groups"][0]["runs"][0]["status"], "stopped")

    async def test_credential_and_proxy_environment_is_not_inherited(self):
        with patch.dict(os.environ, {"OKX_API_KEY": "do-not-inherit", "OKX_API_SECRET": "do-not-inherit",
                                     "HTTPS_PROXY": "http://private", "PYTHONPATH": "/untrusted"}):
            _, result = await self.launch()
        environment = json.loads((self.path / "state" / result["runId"] / "environment.json").read_text())
        self.assertEqual(environment["OKX_API_KEY"], "")
        self.assertEqual(environment["OKX_API_SECRET"], "")
        self.assertNotIn("HTTPS_PROXY", environment)
        self.assertNotIn("PYTHONPATH", environment)

    async def test_old_run_is_never_adopted(self):
        with self.assertRaisesRegex(ValueError, "不属于"):
            await self.supervisor.prepare({"groupId": "baseline", "action": "stop", "runId": "run-24h-02"})

    async def test_pid_reuse_or_wrong_command_is_not_signalled(self):
        _, result = await self.launch()
        run_id = result["runId"]
        with self.supervisor.db:
            self.supervisor.db.execute("UPDATE runs SET process_created=process_created-10 WHERE id=?", (run_id,))
        with self.assertRaisesRegex(ValueError, "身份不匹配"):
            await self.supervisor.prepare({"groupId": "baseline", "action": "stop", "runId": run_id})
        self.assertIsNone(self.supervisor.processes[run_id].returncode)

    async def test_durable_unconfirmed_operation_is_not_reexecuted(self):
        prepared = await self.supervisor.prepare({"groupId": "baseline", "action": "start"})
        with self.supervisor.db:
            self.supervisor.db.execute("INSERT INTO operations(id,request) VALUES(?,?)",
                                       ("operation_unknown_001", encode(prepared["request"])))
        response = await self.supervisor.execute(prepared["request"], "operation_unknown_001")
        self.assertEqual(response["status"], "unknown")
        self.assertEqual(self.supervisor.rows("baseline"), [])

    async def test_control_timeout_is_unknown_then_readonly_receipt_reconciles_after_restart(self):
        _, run = await self.launch()
        prepared = await self.supervisor.prepare({"groupId": "baseline", "runId": run["runId"], "action": "halt"})
        calls = []
        async def delayed_reply(path, request, timeout):
            calls.append(request)
            if request["command"] == "halt":
                raise TimeoutError("applied but response delayed")
            if request["command"] == "receipt":
                return {"ok": True, "operationId": "operation_timeout_001", "receiptStatus": "completed", "tradingState": "HALTED"}
            row = self.supervisor.row(run["runId"], "baseline")
            return {"ok": True, "pid": row["pid"], "environment": "sandbox", "tradingState": "ACTIVE",
                    "connected": {"data": True, "exec": True}}
        with patch("group_runtime.socket_call", delayed_reply):
            result = await self.supervisor.execute(prepared["request"], "operation_timeout_001")
            self.assertEqual(result["status"], "unknown")
            self.assertEqual(result["receiptStatus"], "unknown")
            # New supervisor object reads the same committed operation journal.
            restored = GroupSupervisor(self.registry, self.supervisor.path)
            try:
                receipt = await restored.receipt("operation_timeout_001")
                self.assertEqual(receipt["receiptStatus"], "completed")
                self.assertEqual(receipt["status"], "halted")
                await restored.execute(prepared["request"], "operation_timeout_001")
            finally:
                restored.db.close()
        self.assertEqual(sum(item["command"] == "halt" for item in calls), 1)
        self.assertTrue(all(item.get("operationId") == "operation_timeout_001" for item in calls if item["command"] != "status"))

    async def test_real_unix_timeout_then_effect_and_worker_journal_recovery(self):
        _, run = await self.launch()
        prepared = await self.supervisor.prepare({"groupId": "baseline", "runId": run["runId"], "action": "halt"})
        original = __import__("group_runtime").socket_call
        async def short_timeout(path, request, timeout):
            return await original(path, request, 0.005 if request["command"] == "halt" else timeout)
        with patch("group_runtime.socket_call", short_timeout):
            result = await self.supervisor.execute(prepared["request"], "real_socket_timeout_001")
        self.assertEqual(result["receiptStatus"], "unknown")
        await asyncio.sleep(0.08)
        receipt = await self.supervisor.receipt("real_socket_timeout_001")
        self.assertEqual(receipt["receiptStatus"], "completed")
        self.assertEqual(receipt["engine"]["tradingState"], "HALTED")
        self.assertEqual(receipt["engine"]["applied"], 1)
        repeated = await self.supervisor.execute(prepared["request"], "real_socket_timeout_001")
        self.assertEqual(repeated, receipt)
        # Simulate supervisor crash before receiving/persisting the worker reply,
        # then stop worker and recover from its read-only durable journal.
        with self.supervisor.db:
            self.supervisor.db.execute("UPDATE operations SET result=NULL WHERE id='real_socket_timeout_001'")
        stop = await self.supervisor.prepare({"groupId": "baseline", "runId": run["runId"], "action": "stop"})
        await self.supervisor.execute(stop["request"], "real_socket_stop_001")
        await asyncio.gather(*self.supervisor.tasks)
        restarted = GroupSupervisor(self.registry, self.supervisor.path)
        try:
            recovered = await restarted.receipt("real_socket_timeout_001")
            self.assertEqual(recovered["receiptStatus"], "completed")
            self.assertEqual(recovered["engine"]["applied"], 1)
            self.assertEqual(restarted.row(run["runId"], "baseline")["state"], "stopped")
        finally:
            restarted.db.close()

    async def test_closed_control_failure_does_not_pollute_normal_stop(self):
        _, run = await self.launch()
        prepared = await self.supervisor.prepare({"groupId": "baseline", "runId": run["runId"], "action": "halt"})
        original = __import__("group_runtime").socket_call
        async def closed(path, request, timeout):
            if request["command"] == "halt":
                return {"ok": False, "receiptStatus": "failed", "operationId": "operation_closed_001", "error": "engine closed"}
            return await original(path, request, timeout)
        with patch("group_runtime.socket_call", closed):
            reply = await self.supervisor.execute(prepared["request"], "operation_closed_001")
        self.assertEqual(reply["receiptStatus"], "failed")
        prepared = await self.supervisor.prepare({"groupId": "baseline", "runId": run["runId"], "action": "stop"})
        await self.supervisor.execute(prepared["request"], "operation_stop_001")
        await asyncio.gather(*self.supervisor.tasks)
        row = self.supervisor.row(run["runId"], "baseline")
        self.assertEqual(row["state"], "stopped")
        self.assertTrue(json.loads(row["result"])["stopRequested"])
        self.assertNotIn("error", json.loads(row["result"]))

    async def test_failed_spawn_is_not_reported_as_started_or_retried(self):
        calls = []

        async def denied(*arguments, **keywords):
            calls.append(arguments)
            raise PermissionError("fixture executable denied")

        self.supervisor.process_factory = denied
        prepared = await self.supervisor.prepare({"groupId": "baseline", "action": "start"})
        first = await self.supervisor.execute(prepared["request"], "operation_failed_001")
        second = await self.supervisor.execute(prepared["request"], "operation_failed_001")
        self.assertEqual(first["status"], "failed")
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.supervisor.rows("baseline")[0]["state"], "failed")

    async def test_unix_client_and_unavailable_service(self):
        await self.supervisor.start(self.socket)
        client = GroupRuntimeClient(self.socket)
        status = await client.status("baseline")
        self.assertTrue(status["groups"][0]["capabilities"]["start"])
        self.assertEqual(self.socket.stat().st_mode & 0o777, 0o600)
        prepared = await client.prepare({"groupId": "baseline", "action": "start"})
        self.assertEqual(prepared["mode"], "nautilus_sandbox")
        with self.assertRaisesRegex(ValueError, "尚未配置"):
            await GroupRuntimeClient(None).status()
        with self.assertRaisesRegex(ValueError, "未连接"):
            await GroupRuntimeClient(self.path / "missing.sock").status()

    async def test_socket_path_cannot_overwrite_regular_file(self):
        self.socket.write_text("preserve")
        with self.assertRaises(ValueError):
            await self.supervisor.start(self.socket)
        self.assertEqual(self.socket.read_text(), "preserve")

    async def test_exit_state_requires_final_reports_and_preserves_observed_errors(self):
        cases = [("error", 0, True, True, None, "failed"),
                 ("stopping", 0, False, True, None, "stopped"),
                 ("stopping", 0, False, False, None, "interrupted"),
                 ("stopping", 1, False, True, None, "failed"),
                 ("running", 0, True, True, None, "completed"),
                 ("running", 0, False, True, None, "interrupted"),
                 ("recovering", 0, False, True, None, "interrupted"),
                 ("running", 0, True, True, "export failed", "failed")]
        for index, (state, code, deadline, reports, error, expected) in enumerate(cases):
            run_id = f"baseline-case{index}"
            directory = self.supervisor.path / run_id
            directory.mkdir()
            (directory / "final.json").write_text(json.dumps({"run_id": run_id, "group_id": "baseline",
                "deadline_reached": deadline, "error": error}))
            if reports:
                for name in ("orders.csv", "fills.csv", "positions.csv", "account.csv"):
                    (directory / name).write_text("id,value\n")
            with self.supervisor.db:
                self.supervisor.db.execute("INSERT INTO runs(id,group_id,state,budget,duration,created,result) VALUES(?,?,?,?,?,?,?)",
                    (run_id, "baseline", state, "100", 120, 1, json.dumps({"engine": {"tradingState": "ERROR" if state == "error" else state.upper()}})))

            class Finished:
                async def wait(self):
                    return code

            await self.supervisor.watch(run_id, Finished())
            row = self.supervisor.row(run_id, "baseline")
            with self.subTest(case=index):
                self.assertEqual(row["state"], expected)
                if state == "error":
                    self.assertIn("engine", json.loads(row["result"]))

    async def test_only_proven_unstarted_shadow_stop_can_omit_reports(self):
        final_base = {"stop_reason": "stopped_before_model_ready", "node_started": False,
                      "orders_enabled": False, "error": None, "deadline_reached": False}
        cases = [({}, True, 0, True, "stopped"),
                 ({"node_started": None}, True, 0, True, "interrupted"),
                 ({"orders_enabled": True}, True, 0, True, "interrupted"),
                 ({"stop_reason": "duration_elapsed"}, True, 0, True, "interrupted"),
                 ({}, False, 0, True, "interrupted"),
                 ({}, True, 1, True, "failed"),
                 ({"error": "warmup failure"}, True, 0, True, "failed"),
                 ({}, True, 0, False, "interrupted"),
                 ({"group_id": "other"}, True, 0, True, "interrupted")]
        for index, (changes, requested, code, model, expected) in enumerate(cases):
            run_id = f"baseline-unstarted-{index}"
            directory = self.supervisor.path / run_id
            directory.mkdir()
            final = {"run_id": run_id, "group_id": "baseline", **final_base, **changes}
            (directory / "final.json").write_text(json.dumps(final))
            result = {"stopRequested": requested, **({"modelHash": "fixed-weights", "ordersEnabled": False} if model else {})}
            with self.supervisor.db:
                self.supervisor.db.execute("INSERT INTO runs(id,group_id,state,budget,duration,created,result) VALUES(?,?,?,?,?,?,?)",
                    (run_id, "baseline", "stopping" if requested else "starting", "100", 120, 1, json.dumps(result)))
            class Finished:
                async def wait(self):
                    return code
            await self.supervisor.watch(run_id, Finished())
            row = self.supervisor.row(run_id, "baseline")
            evidence = json.loads(row["result"])
            with self.subTest(case=index):
                self.assertEqual(row["state"], expected)
                self.assertFalse(evidence["reportsAvailable"])
                if expected == "stopped":
                    self.assertTrue(evidence["stoppedBeforeModelReady"])
                    self.assertFalse(evidence["deadlineReached"])

    async def test_recovering_alive_run_still_blocks_second_launch(self):
        _, result = await self.launch()
        row = self.supervisor.row(result["runId"], "baseline")
        async def recovery_status(*args, **kwargs):
            return {"ok": True, "environment": "sandbox", "pid": row["pid"], "tradingState": "RECOVERING",
                    "connected": {"data": True, "exec": True}}
        with patch("group_runtime.socket_call", recovery_status):
            status = await self.supervisor.status("baseline")
            self.assertEqual(status["groups"][0]["runs"][0]["status"], "recovering")
            self.assertFalse(status["groups"][0]["capabilities"]["start"])
            with self.assertRaisesRegex(ValueError, "已有运行"):
                await self.supervisor.prepare({"groupId": "baseline", "action": "start"})

    async def test_zero_market_worker_writes_error_final_instead_of_clean_completion(self):
        worker = worker_module()
        directory = self.path / "baseline-zero-market"
        directory.mkdir()
        ended = asyncio.Event()

        class Frame:
            def to_csv(self, path):
                path.write_text("id,value\n")

        class Node:
            trader = Mock()

            async def run_async(self):
                await ended.wait()

            async def stop_async(self):
                ended.set()

        node = Node()
        for name in ("orders", "fills", "positions", "account"):
            getattr(node.trader, f"generate_{name}_report").return_value = Frame()
        strategy = SimpleNamespace(quote_count=0, snapshot=lambda: {"observed_at": "2026-09-06T00:00:00+00:00", "quote_count": 0})
        commands = SimpleNamespace(settings=SimpleNamespace(trader_id="ZERO-001"), notifier=Mock())
        control = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
        request = {"runId": directory.name, "groupId": "baseline", "registryVersion": self.registry.digest, "durationSeconds": 0}
        identifiers = SimpleNamespace(Venue=lambda value: value)
        with patch.dict(sys.modules, {"nautilus_trader.model.identifiers": identifiers}), patch.object(worker, "write_view"):
            with self.assertRaisesRegex(RuntimeError, "从未同时连接成功"):
                await worker.supervise(node, strategy, commands, control, {}, request, self.registry.spec("baseline"), directory)
        final = json.loads((directory / "final.json").read_text())
        self.assertTrue(final["error"])
        self.assertEqual(final["trading_state"], "ERROR")
        self.assertFalse(final["connected_once"])


if __name__ == "__main__":
    unittest.main()
