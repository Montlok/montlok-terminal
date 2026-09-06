import asyncio
import copy
import hashlib
import io
import json
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import psutil

from artifacts import ArtifactStore
from group_runtime import GroupSupervisor, LaunchRegistry
from model_releases import ModelReleaseStore
from test_group_runtime import FAKE_WORKER, RegistryFixture


class ModelGroupTests(RegistryFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.setup_registry()
        self.model_program = self.path / "model-program" / "engine"
        self.model_program.mkdir(parents=True)
        self.model_worker = self.model_program / "model_group_worker.py"
        control_path = Path(__file__).resolve().parents[2] / "engine" / "control.py"
        worker = FAKE_WORKER.replace("__CONTROL_MODULE__", str(control_path))
        worker = worker.replace('"applied": applied}', '"applied": applied, "warmupComplete": (directory / "warmup").exists(), '
            '"marketReady": (directory / "warmup").exists(), '
            '"manifestSha256": sys.argv[sys.argv.index("--manifest-sha256") + 1], '
            '"modelHash": json.loads(Path(sys.argv[sys.argv.index("--release-manifest") + 1]).read_text())["model"]["sha256"]}')
        self.model_worker.write_text(worker)
        for name in ("model_release.py", "model_client.py", "model_inference_worker.py", "model_market_actor.py",
                     "model_strategy.py", "model_probe.py", "commands.py", "control.py"):
            (self.model_program / name).write_text("# root-installed fixture\n")
        self.guard = self.path / "model-guard"
        self.guard.write_text("# root-installed guard fixture\n")
        self.guard.chmod(0o700)
        self.runner_root = self.path / "reviewed-runners"
        (self.runner_root / "recent_btc").mkdir(parents=True)
        self.source = b"# reviewed source never imported from upload\n"
        for name in ("train_gru.py", "prepare.py"):
            (self.runner_root / "recent_btc" / name).write_bytes(self.source)
        self.artifacts = ArtifactStore(self.path / "artifacts")
        self.models = ModelReleaseStore(self.path / "models", self.artifacts)
        self.manifest = {"schemaVersion": 1, "releaseId": "gru-v1", "runnerId": "gru_v1", "family": "recent_btc_gru",
            "modelVersion": "research/v1", "model": {"path": "model.pt", "sha256": hashlib.sha256(b"weights").hexdigest()},
            "sources": [{"path": "source/" + name, "sha256": hashlib.sha256(self.source).hexdigest()} for name in ("train_gru.py", "prepare.py")],
            "featureContract": {"names": ["return"], "sequenceBars": 16, "barSeconds": 900, "requiredMarkets": ["BTC-USDT"],
                "mean": [0], "scale": [1], "clip": [-8, 8]},
            "outputContract": {"kind": "simple_return", "targetScale": 100, "horizons": [4], "horizonUnit": "15m_bars",
                "selectedHorizon": 4, "selectedHorizonUnit": "15m_bars"},
            "runtime": {"device": "cpu", "maxBatchSize": 8, "maxQueueSize": 16, "timeoutMs": 2000, "maxInputAgeMs": 1200000},
            "policy": {"allowedModes": ["shadow"], "thresholdBps": 20, "maxTargetFraction": 0.1}, "provenance": {"independentTestMetrics": None}}
        self.publication = self.publish(self.manifest, "1")
        self.document["modelRuntime"] = {"storePath": str(self.models.root), "workerPath": str(self.model_worker),
            "pythonPath": psutil.Process().exe(), "guardBinary": str(self.guard), "runnerRoot": str(self.runner_root),
            "settingsPath": str(self.settings), "settingsSha256": hashlib.sha256(self.settings.read_bytes()).hexdigest(),
            "runtimePath": str(self.runtime), "mode": "shadow", "maxBudgetUsdt": "100", "maxDurationSeconds": 120}
        self.registry_path.write_text(json.dumps(self.document))
        self.probe = lambda _: {"torch": True, "numpy": True, "nautilus": True, "nativeBridge": True, "dataAbi": True, "cuda": False, "cudaBf16": False, "mamba3": False}
        self.registry = LaunchRegistry(self.registry_path, strict=False, capability_probe=self.probe)
        self.group_id = "model-" + self.publication["manifestSha256"][:16]
        self.supervisor = GroupSupervisor(self.registry, self.path / "runs")

    def publish(self, manifest, version):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("model.pt", b"weights")
            for name in ("train_gru.py", "prepare.py"):
                archive.writestr("source/" + name, self.source)
        artifact = self.artifacts.register_stream([buffer.getvalue()], kind="model", name="group-model", version=version, filename="model.zip")
        request = self.models.prepare("publish", {"artifactId": artifact["id"]})["request"]
        return self.models.execute("publish", request, "model_group_publish_" + version, "test-admin")

    async def asyncTearDown(self):
        for process in list(self.supervisor.processes.values()):
            if process.returncode is None:
                process.terminate()
                await process.wait()
        if self.supervisor.tasks:
            await asyncio.gather(*self.supervisor.tasks)
        self.supervisor.db.close()
        self.temporary.cleanup()

    async def test_publication_is_discovered_without_start_and_status_does_not_hash_weights(self):
        with patch("group_runtime.file_hash", side_effect=AssertionError("status must not hash model files")):
            status = await self.supervisor.status(self.group_id)
        group = status["groups"][0]
        self.assertTrue(group["capabilities"]["start"])
        self.assertEqual(group["mode"], "shadow")
        self.assertFalse(group["ordersEnabled"])
        self.assertTrue(group["warmupRequired"])
        self.assertEqual(self.supervisor.rows(self.group_id), [])

    async def test_model_command_identity_warmup_and_rollback_preserve_running_version(self):
        prepared = await self.supervisor.prepare({"groupId": self.group_id, "action": "start", "budgetUsdt": "10"})
        run = await self.supervisor.execute(prepared["request"], "model_group_start_001")
        directory = self.supervisor.path / run["runId"]
        for _ in range(100):
            if (directory / "control.sock").exists():
                break
            await asyncio.sleep(0.01)
        row = self.supervisor.row(run["runId"], self.group_id)
        self.assertEqual(psutil.Process(row["pid"]).cmdline(), self.registry.command(self.group_id, directory / "request.json"))
        self.assertEqual((await self.supervisor.status(self.group_id))["groups"][0]["runs"][0]["status"], "starting")
        (directory / "warmup").write_text("complete")
        self.assertEqual((await self.supervisor.status(self.group_id))["groups"][0]["runs"][0]["status"], "running")
        second = copy.deepcopy(self.manifest)
        second.update(releaseId="gru-v2", modelVersion="research/v2")
        publication = self.publish(second, "2")
        self.registry.refresh_models()
        self.assertFalse(self.registry.spec(self.group_id)["enabled"])
        self.assertIsNotNone(self.supervisor.owned_process(row))
        self.assertEqual(json.loads(self.supervisor.row(run["runId"], self.group_id)["result"])["modelHash"], self.publication["modelHash"])
        rollback = self.models.prepare("rollback", {"releaseId": self.manifest["releaseId"]})["request"]
        self.models.execute("rollback", rollback, "model_group_rollback_001", "test-admin")
        self.registry.refresh_models()
        self.assertTrue(self.registry.spec(self.group_id)["enabled"])
        self.assertFalse(self.registry.spec("model-" + publication["manifestSha256"][:16])["enabled"])
        stop = await self.supervisor.prepare({"groupId": self.group_id, "action": "stop", "runId": run["runId"]})
        await self.supervisor.execute(stop["request"], "model_group_stop_001")
        await asyncio.gather(*self.supervisor.tasks)
        self.assertEqual(self.supervisor.row(run["runId"], self.group_id)["state"], "stopped")
        finished = await self.supervisor.run_status(self.supervisor.row(run["runId"], self.group_id))
        self.assertFalse(finished["engine"]["warmupComplete"])
        self.assertFalse(finished["engine"]["marketReady"])
        self.assertEqual(finished["engine"]["model"]["status"], "stopped")

    async def test_preflight_rejects_changed_weights_host_runner_or_http_paths(self):
        for fields in ({"workerPath": "/tmp/evil.py"}, {"guardBinary": "/bin/sh"}, {"modelMode": "sandbox"}, {"releaseManifest": "/private/file"}):
            with self.assertRaises(ValueError):
                await self.supervisor.prepare({"groupId": self.group_id, "action": "start", **fields})
        weights = self.models.manifest_path(self.manifest["releaseId"]).parent / "model.pt"
        weights.chmod(0o600)
        weights.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "hash"):
            await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})
        weights.write_bytes(b"weights")
        (self.runner_root / "recent_btc/train_gru.py").write_text("# tampered host runner")
        with self.assertRaisesRegex(ValueError, "runner"):
            await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})

    async def test_cuda_release_not_startable_on_cpu_only_node(self):
        manifest = copy.deepcopy(self.manifest)
        manifest.update(releaseId="gru-cuda", modelVersion="research/cuda")
        manifest["runtime"]["device"] = "cuda"
        publication = self.publish(manifest, "cuda")
        group_id = "model-" + publication["manifestSha256"][:16]
        status = await self.supervisor.status(group_id)
        self.assertFalse(status["groups"][0]["capabilities"]["start"])
        self.assertIn("CUDA", status["groups"][0]["capabilities"]["reason"])
        with self.assertRaisesRegex(ValueError, "CUDA"):
            await self.supervisor.prepare({"groupId": group_id, "action": "start"})

    async def test_root_model_configuration_cannot_enable_sandbox_orders(self):
        self.document["modelRuntime"]["mode"] = "sandbox"
        self.registry_path.write_text(json.dumps(self.document))
        with self.assertRaisesRegex(ValueError, "shadow"):
            LaunchRegistry(self.registry_path, strict=False, capability_probe=self.probe)

    async def test_old_native_wheel_without_bridge_never_advertises_start(self):
        self.registry.node_capabilities["nativeBridge"] = False
        status = await self.supervisor.status(self.group_id)
        self.assertFalse(status["groups"][0]["capabilities"]["start"])
        self.assertIn("数据桥", status["groups"][0]["capabilities"]["reason"])
        with self.assertRaisesRegex(ValueError, "数据桥"):
            await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})

    async def test_incompatible_data_abi_never_advertises_start(self):
        self.registry.node_capabilities["dataAbi"] = False
        status = await self.supervisor.status(self.group_id)
        self.assertFalse(status["groups"][0]["capabilities"]["start"])
        self.assertIn("数据兼容", status["groups"][0]["capabilities"]["reason"])
        with self.assertRaisesRegex(ValueError, "数据兼容"):
            await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})

    async def test_model_deadline_cleanup_is_stopping_then_completed(self):
        prepared = await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})
        run = await self.supervisor.execute(prepared["request"], "model_deadline_start_001")
        directory = self.supervisor.path / run["runId"]
        row = self.supervisor.row(run["runId"], self.group_id)
        with patch("group_runtime.socket_call", side_effect=OSError("control closed")), patch("group_runtime.time.time", return_value=row["created"] + row["duration"] + 1):
            status = await self.supervisor.run_status(row)
        self.assertEqual(status["status"], "stopping")
        self.assertTrue(status["automaticShutdown"])
        self.supervisor.processes[run["runId"]].terminate()
        await asyncio.gather(*self.supervisor.tasks)
        (directory / "final.json").write_text(json.dumps({"run_id": run["runId"], "group_id": self.group_id,
            "deadline_reached": True, "error": None}))
        for name in ("orders.csv", "fills.csv", "positions.csv", "account.csv"):
            (directory / name).write_text("header\n")
        self.supervisor.update(run["runId"], "stopping", {"modelHash": self.publication["modelHash"], "automaticShutdown": True})
        class Finished:
            async def wait(self):
                return 0
        await self.supervisor.watch(run["runId"], Finished())
        self.assertEqual(self.supervisor.row(run["runId"], self.group_id)["state"], "completed")

    async def test_rolling_publication_does_not_bypass_global_model_slots(self):
        self.registry.model_runtime["maxConcurrentRuns"] = 1
        prepared = await self.supervisor.prepare({"groupId": self.group_id, "action": "start"})
        await self.supervisor.execute(prepared["request"], "capacity_first_model_001")
        second = copy.deepcopy(self.manifest)
        second.update(releaseId="gru-new-slot", modelVersion="research/new-slot")
        publication = self.publish(second, "slot2")
        group_id = "model-" + publication["manifestSha256"][:16]
        status = (await self.supervisor.status(group_id))["groups"][0]
        self.assertTrue(status["ready"])
        self.assertFalse(status["capabilities"]["start"])
        self.assertEqual(status["capacity"], {"used": 1, "limit": 1, "available": 0})
        self.assertIn("1/1", status["capabilities"]["reason"])
        with self.assertRaisesRegex(ValueError, "槽位"):
            await self.supervisor.prepare({"groupId": group_id, "action": "start"})
        baseline = (await self.supervisor.status("baseline"))["groups"][0]
        self.assertTrue(baseline["capabilities"]["start"])

    async def test_distinct_published_runners_concurrently_claim_only_one_slot(self):
        second = copy.deepcopy(self.manifest)
        second.update(releaseId="rdt-slot", runnerId="rdt4quant_v1", family="rdt4quant_multiasset", modelVersion="research/rdt-slot")
        contract = second.pop("featureContract")
        contract.update(domain="crypto", instrument="BTC-USDT", assetId=0, yScale=[1])
        second["domainContracts"] = {"crypto:BTC-USDT": contract}
        second["runtime"]["device"] = "cuda"
        second["outputContract"] = {"kind": "log_return_bps_quantiles", "quantiles": [.1, .5, .9], "horizons": {"crypto": [15]},
            "horizonUnit": {"crypto": "minutes"}, "selectedHeadByDomain": {"crypto": 0}, "depth": 4}
        publication = self.publish(second, "slot-rdt")
        second_id = "model-" + publication["manifestSha256"][:16]
        self.registry.model_runtime.update(maxConcurrentRuns=1, contractKey={"rdt4quant_v1": "crypto:BTC-USDT"})
        # Capacity test only: both workers are inert local subprocess fixtures;
        # no CUDA inference or real venue connectivity is claimed here.
        with patch.object(self.registry, "validate_model_inputs", side_effect=lambda spec: spec):
            first = (await self.supervisor.prepare({"groupId": self.group_id, "action": "start"}))["request"]
            second = (await self.supervisor.prepare({"groupId": second_id, "action": "start"}))["request"]
            results = await asyncio.gather(
                self.supervisor.execute(first, "slot_concurrent_gru_001"),
                self.supervisor.execute(second, "slot_concurrent_rdt_001"), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, dict) and result.get("status") == "starting" for result in results), 1)
        self.assertEqual(sum(isinstance(result, ValueError) and "槽位" in str(result) for result in results), 1)
        self.assertEqual(self.supervisor.model_capacity()["used"], 1)


if __name__ == "__main__":
    unittest.main()
