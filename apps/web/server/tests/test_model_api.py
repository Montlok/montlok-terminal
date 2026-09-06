"""Model publication HTTP contract, using real stores and no model execution."""
import asyncio
import copy
import hashlib
import io
import json
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiohttp import CookieJar
from aiohttp.test_utils import TestClient, TestServer

from app import Operator
from artifacts import ArtifactStore
from model_releases import ModelReleaseStore


class ModelAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="model-api-")
        here = Path(__file__).resolve().parent
        self.operator = Operator(SimpleNamespace(
            state_dir=Path(self.directory.name), credentials=None,
            catalog=here.parent / "catalog", paper_bridge=here / "paper_stub.py",
            paper_run=Path("test-run"), static_dir=here, node="unused", mcp="unused",
            allowed_origins=["http://127.0.0.1:18081"],
        ))
        self.operator.profiles.save({"id": "demo", "name": "Test", "mode": "demo", "site": "global",
            "apiKey": "fixture-key", "secret": "fixture-secret", "passphrase": "fixture-phrase"})
        self.operator.profiles.select("demo")
        self.assertIsInstance(self.operator.artifacts, ArtifactStore)
        self.assertIsInstance(self.operator.model_releases, ModelReleaseStore)
        self.operator.group_runtime.status = AsyncMock(return_value={"available": True, "groups": []})
        self.operator.group_runtime.prepare = AsyncMock()
        self.operator.group_runtime.execute = AsyncMock()
        application = self.operator.application()
        application.cleanup_ctx.clear()  # Never launch exchange/background services.
        self.client = TestClient(TestServer(application), cookie_jar=CookieJar(unsafe=True))
        await self.client.start_server()
        session = await (await self.client.get("/api/session")).json()
        self.headers = {"X-Operator-CSRF": session["csrf"]}
        self.weight = b"opaque-fixture-weight-not-loaded"
        self.manifest = {"schemaVersion": 1, "releaseId": "gru-api-v1", "runnerId": "gru_v1",
            "family": "recent_btc_gru", "modelVersion": "api/v1",
            "model": {"path": "model.pt", "sha256": hashlib.sha256(self.weight).hexdigest()}, "sources": [],
            "featureContract": {"names": ["return"], "sequenceBars": 3, "barSeconds": 900,
                "requiredMarkets": ["BTC-USDT"], "mean": [0], "scale": [1], "clip": [-8, 8]},
            "outputContract": {"kind": "simple_return", "targetScale": 100, "horizons": [4], "horizonUnit": "15m_bars", "selectedHorizon": 4, "selectedHorizonUnit": "15m_bars"},
            "runtime": {"device": "cpu", "maxBatchSize": 1, "maxQueueSize": 2, "timeoutMs": 2000, "maxInputAgeMs": 1000},
            "policy": {"allowedModes": ["shadow"], "thresholdBps": 20, "maxTargetFraction": 0.1},
            "provenance": {"independentTestMetrics": None, "evidence": "test artifact only"}}
        self.upload_count = 0
        self.artifact = self.upload()

    async def asyncTearDown(self):
        await self.client.close()
        self.operator.database.close()
        self.directory.cleanup()

    def upload(self, manifest=None):
        self.upload_count += 1
        content = io.BytesIO()
        with zipfile.ZipFile(content, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest or self.manifest))
            archive.writestr("model.pt", self.weight)
        return self.operator.artifacts.register_stream([content.getvalue()], kind="model", name="api-model",
            version=str(self.upload_count), filename="model.zip")

    async def prepare(self, action, arguments):
        response = await self.client.post("/api/prepare",
            json={"kind": "model_release", "name": action, "arguments": arguments}, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        return await response.json()

    async def execute(self, ticket):
        response = await self.client.post("/api/execute", json={"id": ticket["id"]}, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        return await response.json()

    async def publish(self, artifact=None):
        ticket = await self.prepare("publish", {"artifactId": (artifact or self.artifact)["id"]})
        result = await self.execute(ticket)
        self.assertEqual(result["status"], "completed", result)
        return self.operator.model_releases.get(result["result"]["releaseId"])

    def assert_no_runtime_action(self):
        self.operator.group_runtime.prepare.assert_not_awaited()
        self.operator.group_runtime.execute.assert_not_awaited()

    def group(self, release, **overrides):
        return {"kind": "model", "groupId": "gru_shadow", "releaseId": release["releaseId"],
            "manifestSha256": release["manifestSha256"], "ready": True, "device": "cpu",
            "nodeCapabilities": {"torch": True, "numpy": True, "nautilus": True, "cuda": False},
            "capabilities": {"start": True, "reason": None}, **overrides}

    async def test_read_only_listing_and_detail_allow_viewer_without_csrf_but_require_session(self):
        release = await self.publish()
        for session in self.operator.sessions.values():
            session["role"] = "viewer"
        for path in ("/api/model-releases", f"/api/model-releases/{release['releaseId']}"):
            response = await self.client.get(path)
            self.assertEqual(response.status, 200, await response.text())
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            self.assertNotIn("fixture-secret", await response.text())
        self.client.session.cookie_jar.clear()
        self.assertEqual((await self.client.get("/api/model-releases")).status, 401)
        self.assert_no_runtime_action()

    async def test_validate_requires_confirmation_and_returns_preview_without_publication(self):
        ticket = await self.prepare("validate", {"artifactId": self.artifact["id"]})
        self.assertEqual(ticket["modelPreview"]["validation"], "artifact_valid")
        self.assertEqual(ticket["operation"]["arguments"], {"artifactId": self.artifact["id"]})
        self.assertGreater(ticket["expiresAt"], time.time())
        self.assertEqual(self.operator.model_releases.list(), [])
        self.assertEqual(self.operator.database.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 0)
        result = await self.execute(ticket)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["result"]["status"], "validated")
        self.assertEqual(result["result"]["validation"], "artifact_valid")
        self.assertFalse(result["result"]["deployReady"])
        self.assertFalse(result["result"]["started"])
        self.assertEqual(result["result"]["manifest"]["policy"]["allowedModes"], ["shadow"])
        self.assertEqual(self.operator.model_releases.list(), [])
        self.assert_no_runtime_action()

    async def test_publish_freezes_hash_and_active_release_and_is_idempotent_without_starting(self):
        ticket = await self.prepare("publish", {"artifactId": self.artifact["id"]})
        arguments = ticket["operation"]["arguments"]
        self.assertEqual(arguments["manifestSha256"], ticket["modelPreview"]["release"]["manifestSha256"])
        self.assertIn("expectedActiveReleaseId", arguments)
        self.assertEqual(self.operator.model_releases.list(), [])
        replies = await asyncio.gather(*[self.execute(ticket) for _ in range(4)])
        self.assertTrue(all(item["status"] == "completed" for item in replies))
        self.assertTrue(all(item["result"]["started"] is False for item in replies))
        self.assertEqual(len(self.operator.model_releases.list()), 1)
        self.assertEqual(self.operator.database.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 1)
        self.assert_no_runtime_action()

    async def test_validate_publish_rollback_require_csrf_and_admin_at_prepare_and_execute(self):
        release = await self.publish()
        other_manifest = copy.deepcopy(self.manifest)
        other_manifest.update(releaseId="gru-api-v2", modelVersion="api/v2")
        other = self.upload(other_manifest)
        for action, arguments in (("validate", {"artifactId": self.artifact["id"]}),
                                  ("publish", {"artifactId": other["id"]}),
                                  ("rollback", {"releaseId": release["releaseId"]})):
            with self.subTest(action=action):
                operation = {"kind": "model_release", "name": action, "arguments": arguments}
                response = await self.client.post("/api/prepare", json=operation)
                self.assertEqual(response.status, 403)
                self.assertEqual((await response.json())["code"], "SESSION_CSRF_MISMATCH")
                ticket = await self.prepare(action, arguments)
                response = await self.client.post("/api/execute", json={"id": ticket["id"]})
                self.assertEqual(response.status, 403)
                for session in self.operator.sessions.values():
                    session["role"] = "viewer"
                self.assertEqual((await self.client.post("/api/prepare", json=operation, headers=self.headers)).status, 403)
                self.assertEqual((await self.client.post("/api/execute", json={"id": ticket["id"]}, headers=self.headers)).status, 403)
                for session in self.operator.sessions.values():
                    session["role"] = "admin"
        self.assertEqual(len(self.operator.model_releases.list()), 1)
        self.assert_no_runtime_action()

    async def test_missing_expired_and_foreign_confirmation_do_not_publish(self):
        response = await self.client.post("/api/execute", json={"id": "no-confirmation"}, headers=self.headers)
        self.assertEqual(response.status, 400)
        expired = await self.prepare("publish", {"artifactId": self.artifact["id"]})
        self.operator.confirmations[expired["id"]]["expires"] = time.time() - 1
        self.assertEqual((await self.client.post("/api/execute", json={"id": expired["id"]}, headers=self.headers)).status, 400)
        foreign = await self.prepare("publish", {"artifactId": self.artifact["id"]})
        self.operator.confirmations[foreign["id"]]["session"] = "other-session"
        self.assertEqual((await self.client.post("/api/execute", json={"id": foreign["id"]}, headers=self.headers)).status, 400)
        self.assertEqual(self.operator.model_releases.list(), [])
        self.assert_no_runtime_action()

    async def test_rollback_is_explicit_frozen_and_does_not_start(self):
        first = await self.publish()
        second_manifest = copy.deepcopy(self.manifest)
        second_manifest.update(releaseId="gru-api-v2", modelVersion="api/v2")
        second = await self.publish(self.upload(second_manifest))
        ticket = await self.prepare("rollback", {"releaseId": first["releaseId"], "manifestSha256": first["manifestSha256"]})
        self.assertEqual(ticket["operation"]["arguments"]["expectedActiveReleaseId"], second["releaseId"])
        self.assertEqual(self.operator.model_releases.active_id("gru_v1"), second["releaseId"])
        result = await self.execute(ticket)
        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["result"]["started"])
        self.assertEqual(self.operator.model_releases.active_id("gru_v1"), first["releaseId"])
        self.assert_no_runtime_action()

    async def test_stale_active_binding_fails_without_changing_current_release(self):
        stale = await self.prepare("publish", {"artifactId": self.artifact["id"]})
        other_manifest = copy.deepcopy(self.manifest)
        other_manifest.update(releaseId="gru-other", modelVersion="other/v1")
        other = await self.publish(self.upload(other_manifest))
        result = await self.execute(stale)
        self.assertEqual(result["status"], "error")
        self.assertEqual(self.operator.model_releases.active_id("gru_v1"), other["releaseId"])
        self.assert_no_runtime_action()

    async def test_unconfirmed_query_and_untrusted_paths_cannot_publish(self):
        operation = {"kind": "model_release", "name": "publish", "arguments": {"artifactId": self.artifact["id"]}}
        self.assertEqual((await self.client.post("/api/query", json=operation, headers=self.headers)).status, 400)
        for extra in ({"entrypoint": "/tmp/runner.py"}, {"device": "cuda"}, {"baseUrl": "https://remote.example"}):
            response = await self.client.post("/api/prepare", json={**operation, "arguments": {**operation["arguments"], **extra}}, headers=self.headers)
            self.assertEqual(response.status, 400)
        self.assertEqual(self.operator.model_releases.list(), [])
        self.assert_no_runtime_action()

    async def test_flat_matching_group_reports_node_readiness_not_start_capacity(self):
        release = await self.publish()
        group = self.group(release, capabilities={"start": False, "reason": "已有运行实例"})
        self.operator.group_runtime.status.return_value = {"available": True, "groups": [group]}
        for path in ("/api/model-releases", f"/api/model-releases/{release['releaseId']}"):
            result = await (await self.client.get(path)).json()
            result = result["releases"][0] if "releases" in result else result
            self.assertTrue(result["deployReady"])
            self.assertEqual(result["groupId"], "gru_shadow")
            self.assertTrue(result["nodeCompatibility"]["ready"])
            self.assertEqual(result["nodeCompatibility"]["device"], "cpu")
            self.assertEqual(result["nodeCompatibility"]["nodeId"], "local")
            self.assertFalse(result["started"])
        self.assert_no_runtime_action()

    async def test_stale_or_foreign_group_metadata_does_not_mark_release_deployable(self):
        release = await self.publish()
        for override in ({"manifestSha256": "0" * 64}, {"releaseId": "other-release"}, {"kind": "static"}):
            with self.subTest(override=override):
                self.operator.group_runtime.status.return_value = {"available": True, "groups": [self.group(release, **override)]}
                result = await (await self.client.get(f"/api/model-releases/{release['releaseId']}")).json()
                self.assertFalse(result["deployReady"])
        self.assert_no_runtime_action()

    async def test_unavailable_supervisor_and_device_mismatch_remain_not_deployable(self):
        release = await self.publish()
        reason = "目标节点缺少预装依赖"
        self.operator.group_runtime.status.return_value = {"available": True, "groups": [self.group(release, ready=False,
            capabilities={"start": False, "reason": reason})]}
        result = await (await self.client.get(f"/api/model-releases/{release['releaseId']}")).json()
        self.assertFalse(result["deployReady"])
        self.assertEqual(result["nodeCompatibility"]["reason"], reason)
        self.operator.group_runtime.status.return_value = {"available": False, "groups": [self.group(release)]}
        result = await (await self.client.get(f"/api/model-releases/{release['releaseId']}")).json()
        self.assertFalse(result["deployReady"])
        self.operator.group_runtime.status.side_effect = ValueError("supervisor offline")
        result = await (await self.client.get(f"/api/model-releases/{release['releaseId']}")).json()
        self.assertFalse(result["deployReady"])
        self.assert_no_runtime_action()

    async def test_rdt_integer_head_contract_and_unavailable_cuda_are_reported_truthfully(self):
        manifest = copy.deepcopy(self.manifest)
        feature = manifest.pop("featureContract")
        feature.update(domain="crypto", instrument="BTC-USDT", assetId=0, yScale=[2, 3])
        manifest.update(releaseId="rdt-api-v1", runnerId="rdt4quant_v1", family="rdt4quant_multiasset",
            domainContracts={"crypto:BTC-USDT": feature})
        manifest["runtime"]["device"] = "cuda"
        manifest["outputContract"] = {"kind": "log_return_bps_quantiles", "quantiles": [0.1, 0.5, 0.9],
            "horizons": {"crypto": [15, 60]}, "horizonUnit": {"crypto": "minutes"},
            "selectedHeadByDomain": {"crypto": 1}, "depth": 4}
        release = await self.publish(self.upload(manifest))
        reason = "此模型需要 CUDA BF16，当前节点未检测到兼容 GPU"
        self.operator.group_runtime.status.return_value = {"available": True, "groups": [self.group(release,
            ready=False, device="cuda", capabilities={"start": False, "reason": reason})]}
        result = await (await self.client.get(f"/api/model-releases/{release['releaseId']}")).json()
        self.assertEqual(result["status"], "published")
        self.assertEqual(result["validation"], "artifact_valid")
        self.assertIs(type(result["manifest"]["outputContract"]["selectedHeadByDomain"]["crypto"]), int)
        self.assertEqual(result["manifest"]["outputContract"]["selectedHeadByDomain"]["crypto"], 1)
        self.assertFalse(result["deployReady"])
        self.assertFalse(result["nodeCompatibility"]["ready"])
        self.assertEqual(result["nodeCompatibility"]["reason"], reason)
        self.assertFalse(result["started"])
        self.assert_no_runtime_action()

    async def test_unknown_publication_receipt_recovers_saved_store_result_without_reexecution(self):
        ticket = await self.prepare("publish", {"artifactId": self.artifact["id"]})
        session_id = next(iter(self.operator.sessions))
        owner = self.operator.sessions[session_id]["operator"]
        # Crash window: store committed publication, but BFF has not saved its reply.
        saved = self.operator.model_releases.execute("publish", ticket["operation"]["arguments"], ticket["id"], owner)
        self.operator.confirmations.pop(ticket["id"])
        self.operator.database.execute(
            "INSERT INTO operations (id,at,profile,action,parameters,status,result,session,owner) VALUES (?,?,?,?,?,?,?,?,?)",
            (ticket["id"], time.time(), "demo", "model_release:publish", json.dumps(ticket["operation"]["arguments"]),
             "unknown", "null", session_id, owner))
        self.operator.database.commit()
        manifest = self.operator.model_releases.manifest_path(saved["releaseId"])
        modified = manifest.stat().st_mtime_ns
        response = await self.client.get(f"/api/operations/{ticket['id']}")
        self.assertEqual(response.status, 200, await response.text())
        receipt = await response.json()
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["result"]["operationId"], saved["operationId"])
        self.assertFalse(receipt["result"]["started"])
        with self.operator.model_releases.connect() as database:
            self.assertEqual(database.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 1)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM releases").fetchone()[0], 1)
        self.assertEqual(manifest.stat().st_mtime_ns, modified)
        self.assert_no_runtime_action()

    async def test_unknown_publication_without_store_receipt_remains_unknown_and_does_not_publish(self):
        session_id = next(iter(self.operator.sessions))
        owner = self.operator.sessions[session_id]["operator"]
        identifier = "model_no_committed_receipt"
        self.operator.database.execute(
            "INSERT INTO operations (id,at,profile,action,parameters,status,result,session,owner) VALUES (?,?,?,?,?,?,?,?,?)",
            (identifier, time.time(), "demo", "model_release:publish", json.dumps({"artifactId": self.artifact["id"]}),
             "unknown", "null", session_id, owner))
        self.operator.database.commit()
        response = await self.client.get(f"/api/operations/{identifier}")
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual((await response.json())["status"], "unknown")
        self.assertEqual(self.operator.model_releases.list(), [])
        self.assert_no_runtime_action()


if __name__ == "__main__":
    unittest.main()
