import concurrent.futures
import copy
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from artifacts import ArtifactError, ArtifactStore
from model_releases import ModelReleaseConflict, ModelReleaseError, ModelReleaseStore, validate_manifest


def checksum(data):
    return hashlib.sha256(data).hexdigest()


class ModelReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="model-releases-", dir="/private/tmp")
        self.root = Path(self.temporary.name)
        self.artifacts = ArtifactStore(self.root / "artifacts")
        self.store = ModelReleaseStore(self.root / "published", self.artifacts)
        self.weight = b"opaque-model-weight-not-loaded"
        self.source = f"raise RuntimeError('uploaded source must never execute')\n".encode()
        self.manifest = {"schemaVersion": 1, "releaseId": "gru-reviewed-v1", "runnerId": "gru_v1",
            "family": "recent_btc_gru", "modelVersion": "reviewed/v1",
            "model": {"path": "model.pt", "sha256": checksum(self.weight)},
            "sources": [{"path": "source/train_gru.py", "sha256": checksum(self.source)}],
            "featureContract": {"names": ["return", "volume"], "sequenceBars": 16, "barSeconds": 900,
                "requiredMarkets": ["BTC-USDT", "BTC-USDT-SWAP"], "mean": [0, 1], "scale": [1, 2], "clip": [-8, 8]},
            "outputContract": {"kind": "simple_return", "targetScale": 100, "horizons": [4], "horizonUnit": "15m_bars",
                "selectedHorizon": 4, "selectedHorizonUnit": "15m_bars"},
            "runtime": {"device": "cpu", "maxBatchSize": 16, "maxQueueSize": 32, "timeoutMs": 2000, "maxInputAgeMs": 1200000},
            "policy": {"allowedModes": ["shadow", "sandbox"], "thresholdBps": 20, "maxTargetFraction": 0.1},
            "provenance": {"independentTestMetrics": None, "evidence": "artifact-only validation"}}
        self.counter = 0

    def tearDown(self):
        self.temporary.cleanup()

    def upload(self, manifest=None, files=None, *, kind="model"):
        manifest = manifest or self.manifest
        self.counter += 1
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            for name, data in (files or {"model.pt": self.weight, "source/train_gru.py": self.source}).items():
                archive.writestr(name, data)
        return self.artifacts.register_stream([buffer.getvalue()], kind=kind, name="model-release", version=str(self.counter), filename="model.zip")

    def publish(self, artifact, operation="model_publish_operation_001"):
        preview = self.store.prepare("publish", {"artifactId": artifact["id"]})
        return self.store.execute("publish", preview["request"], operation, "admin")

    def test_validate_never_executes_source_or_starts_and_publish_requires_confirmed_request(self):
        artifact = self.upload()
        validated = self.store.validate(artifact["id"])
        self.assertEqual(validated["validation"], "artifact_valid")
        self.assertFalse(validated["deployReady"])
        self.assertEqual(validated["checks"]["sourceExecution"], "never")
        self.assertEqual(self.store.list(), [])
        self.assertEqual(list((self.store.root / "releases").iterdir()), [])
        with self.assertRaisesRegex(ModelReleaseError, "规范化"):
            self.store.execute("publish", {"artifactId": artifact["id"]}, "model_unconfirmed_001", "admin")
        result = self.publish(artifact)
        self.assertEqual(result["receiptStatus"], "completed")
        self.assertFalse(result["started"])
        released = self.store.get(self.manifest["releaseId"])
        self.assertTrue(released["active"])
        self.assertFalse(released["deployReady"])
        self.assertEqual(checksum(self.store.manifest_path(released["releaseId"]).read_bytes()), released["manifestSha256"])

    def test_publish_idempotence_concurrency_and_operation_payload_collision(self):
        artifact = self.upload()
        request = self.store.prepare("publish", {"artifactId": artifact["id"]})["request"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: self.store.execute("publish", request, "model_concurrent_001", "admin"), range(6)))
        self.assertEqual(len(self.store.list()), 1)
        self.assertTrue(all(result == results[0] for result in results))
        with self.assertRaises(ModelReleaseConflict):
            self.store.execute("publish", request, "model_concurrent_001", "other-admin")

    def test_immutable_model_version_and_explicit_rollback(self):
        first = self.publish(self.upload())
        changed = copy.deepcopy(self.manifest)
        changed["policy"]["thresholdBps"] = 21
        with self.assertRaises(ModelReleaseConflict):
            self.publish(self.upload(changed), "model_mutation_001")
        changed.update(releaseId="gru-reviewed-v2", modelVersion="reviewed/v2")
        second = self.publish(self.upload(changed), "model_publish_operation_002")
        self.assertEqual(second["previousReleaseId"], first["releaseId"])
        preview = self.store.prepare("rollback", {"releaseId": first["releaseId"]})
        rolled = self.store.execute("rollback", preview["request"], "model_rollback_operation_001", "admin")
        self.assertEqual(rolled["releaseId"], first["releaseId"])
        self.assertFalse(rolled["started"])
        self.assertEqual(self.store.active_id("gru_v1"), first["releaseId"])
        with self.assertRaises(ModelReleaseError):
            self.store.prepare("rollback", {"releaseId": "previous"})

    def test_confirmation_binds_previous_active_and_manifest_hash(self):
        first = self.upload()
        stale = self.store.prepare("publish", {"artifactId": first["id"]})["request"]
        other = copy.deepcopy(self.manifest)
        other.update(releaseId="gru-other", modelVersion="other/v1")
        self.publish(self.upload(other))
        with self.assertRaises(ModelReleaseConflict):
            self.store.execute("publish", stale, "model_stale_001", "admin")
        with self.assertRaises(ModelReleaseConflict):
            self.store.prepare("publish", {"artifactId": first["id"], "manifestSha256": "0" * 64})

    def test_restart_restores_active_release_and_operation_receipt(self):
        result = self.publish(self.upload())
        restarted = ModelReleaseStore(self.store.root, self.artifacts)
        self.assertEqual(restarted.active_id("gru_v1"), result["releaseId"])
        self.assertEqual(restarted.receipt(result["operationId"]), result)

    def test_delete_journal_supports_readonly_supervisor_without_sidecar_files(self):
        result = self.publish(self.upload())
        self.store.root.chmod(0o500)
        db = None
        try:
            db = sqlite3.connect(self.store.database.as_uri() + "?mode=ro", uri=True)
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(db.execute("SELECT release_id FROM active").fetchone()[0], result["releaseId"])
            self.assertFalse(Path(str(self.store.database) + "-wal").exists())
            self.assertFalse(Path(str(self.store.database) + "-shm").exists())
        finally:
            if db:
                db.close()
            self.store.root.chmod(0o700)

    def test_existing_wal_catalog_safely_migrates_without_losing_receipts(self):
        result = self.publish(self.upload())
        prior = sqlite3.connect(self.store.database)
        try:
            prior.execute("PRAGMA journal_mode=WAL")
            prior.execute("UPDATE releases SET actor='migration-owner'")
            prior.commit()
        finally:
            prior.close()
        migrated = ModelReleaseStore(self.store.root, self.artifacts)
        self.assertEqual(migrated.receipt(result["operationId"]), result)
        self.assertEqual(migrated.get(result["releaseId"])["publishedBy"], "migration-owner")
        with migrated.connect() as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")

    def test_wal_migration_does_not_interrupt_an_existing_reader(self):
        result = self.publish(self.upload())
        holder = sqlite3.connect(self.store.database)
        holder.execute("PRAGMA journal_mode=WAL")
        holder.execute("BEGIN")
        holder.execute("SELECT * FROM releases").fetchall()
        original = ModelReleaseStore.connect
        @contextmanager
        def short_wait(store):
            with original(store) as db:
                db.execute("PRAGMA busy_timeout=5")
                yield db
        try:
            with patch.object(ModelReleaseStore, "connect", short_wait):
                with self.assertRaisesRegex(ModelReleaseError, "无法安全切换"):
                    ModelReleaseStore(self.store.root, self.artifacts)
            self.assertEqual(holder.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(holder.execute("SELECT release_id FROM active").fetchone()[0], result["releaseId"])
        finally:
            holder.rollback()
            holder.close()
        migrated = ModelReleaseStore(self.store.root, self.artifacts)
        self.assertEqual(migrated.receipt(result["operationId"]), result)

    def test_file_corruption_and_missing_file_rejected(self):
        for files in ({"model.pt": b"wrong", "source/train_gru.py": self.source}, {"model.pt": self.weight}):
            with self.subTest(files=list(files)), self.assertRaises(ModelReleaseError):
                self.store.validate(self.upload(files=files)["id"])
        artifact = self.upload()
        target = self.artifacts.root / "objects" / artifact["sha256"]
        target.chmod(0o600)
        target.write_bytes(b"corrupt")
        with self.assertRaises(ModelReleaseError):
            self.store.validate(artifact["id"])

    def test_runner_source_path_live_mode_and_runtime_injection_rejected(self):
        changes = [lambda m: m.update(runnerId="uploaded_python"),
                   lambda m: m.update(entrypoint="source/train_gru.py"),
                   lambda m: m["model"].update(path="../../outside.pt"),
                   lambda m: m["model"].update(path="/tmp/model.pt"),
                   lambda m: m["runtime"].update(pythonPath="/bin/sh"),
                   lambda m: m["policy"].update(allowedModes=["live"]),
                   lambda m: m["featureContract"].update(scale=[0, 2]),
                   lambda m: m["featureContract"].update(names=["same", "same"]),
                   lambda m: m["featureContract"].update(names=["name with spaces", "volume"]),
                   lambda m: m["featureContract"].update(names=["非ASCII", "volume"]),
                   lambda m: m["featureContract"].update(sequenceBars=True),
                   lambda m: m["outputContract"].update(horizons=[]),
                   lambda m: m["outputContract"].pop("selectedHorizon"),
                   lambda m: m["outputContract"].pop("selectedHorizonUnit"),
                   lambda m: m["outputContract"].update(selectedHorizon=1),
                   lambda m: m["provenance"].pop("independentTestMetrics"),
                   lambda m: m["runtime"].update(timeoutMs=300001)]
        for mutate in changes:
            manifest = copy.deepcopy(self.manifest)
            mutate(manifest)
            with self.subTest(manifest=manifest), self.assertRaises(ModelReleaseError):
                validate_manifest(manifest)

    def test_wrong_json_types_rejected_as_validation_errors(self):
        for group, field in (("runtime", "device"), ("policy", "allowedModes"), ("outputContract", "kind")):
            manifest = copy.deepcopy(self.manifest)
            manifest[group][field] = [{}]
            with self.subTest(group=group), self.assertRaises(ModelReleaseError):
                validate_manifest(manifest)

    def test_rdt_contract_requires_device_and_explicit_matching_output_head(self):
        manifest = copy.deepcopy(self.manifest)
        manifest.update(runnerId="rdt4quant_v1", family="rdt4quant_multiasset", releaseId="rdt-reviewed-v1")
        contract = manifest.pop("featureContract")
        contract.update(domain="crypto", instrument="BTC-USDT", assetId=0, yScale=[2, 3])
        manifest["domainContracts"] = {"crypto:BTC-USDT": contract}
        manifest["runtime"]["device"] = "cuda"
        manifest["outputContract"] = {"kind": "log_return_bps_quantiles", "quantiles": [0.1, 0.5, 0.9],
            "horizons": {"crypto": [15, 60]}, "horizonUnit": {"crypto": "minutes"},
            "selectedHeadByDomain": {"crypto": 1}, "depth": 4}
        self.assertEqual(validate_manifest(manifest)["runtime"]["device"], "cuda")
        validated = self.store.validate(self.upload(manifest)["id"])
        self.assertEqual(validated["validation"], "artifact_valid")
        self.assertFalse(validated["deployReady"])
        self.assertEqual(validated["checks"]["deviceAvailability"], "not_checked")
        for mutate in (lambda m: m["runtime"].update(device="cpu"),
                       lambda m: m["outputContract"].pop("selectedHeadByDomain"),
                       lambda m: m["outputContract"].update(selectedHeadByDomain={"crypto": 2}),
                       lambda m: m["outputContract"].update(horizons=[15, 60], horizonUnit="minutes"),
                       lambda m: m["outputContract"].update(quantiles=[.2, .5, .8]),
                       lambda m: m["outputContract"].update(quantiles=[.1, .5]),
                       lambda m: m["domainContracts"]["crypto:BTC-USDT"].update(yScale=[2])):
            invalid = copy.deepcopy(manifest)
            mutate(invalid)
            with self.assertRaises(ModelReleaseError):
                validate_manifest(invalid)

    def test_feature_bounds_match_native_model_guard(self):
        manifest = copy.deepcopy(self.manifest)
        contract = manifest["featureContract"]
        contract.update(names=[f"feature_{i}" for i in range(1024)], mean=[0] * 1024, scale=[1] * 1024, sequenceBars=256)
        validate_manifest(manifest)
        contract["sequenceBars"] = 257
        with self.assertRaisesRegex(ModelReleaseError, "矩阵过大"):
            validate_manifest(manifest)
        contract.update(names=[f"feature_{i}" for i in range(1025)], mean=[0] * 1025, scale=[1] * 1025, sequenceBars=16)
        with self.assertRaisesRegex(ModelReleaseError, "特征名称"):
            validate_manifest(manifest)

    def test_zip_traversal_and_symlink_rejected(self):
        with self.assertRaises(ArtifactError):
            self.upload(files={"../outside.pt": self.weight})
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("manifest.json", json.dumps(self.manifest))
            link = zipfile.ZipInfo("model.pt")
            link.create_system = 3
            link.external_attr = (0o120777 << 16)
            archive.writestr(link, "../../private")
        with self.assertRaises(ArtifactError):
            self.artifacts.register_stream([buffer.getvalue()], kind="model", name="evil", version="1", filename="evil.zip")

    def test_json_manifest_references_registered_hashes_without_filesystem_access(self):
        weights = self.artifacts.register_stream([self.weight], kind="model", name="weights", version="1", filename="weights.onnx")
        manifest = copy.deepcopy(self.manifest)
        manifest["sources"] = []
        manifest["model"]["artifactId"] = weights["id"]
        raw = json.dumps(manifest).encode()
        artifact = self.artifacts.register_stream([raw], kind="model", name="json-release", version="1", filename="manifest.json")
        result = self.publish(artifact)
        self.assertEqual(result["modelHash"], checksum(self.weight))
        self.assertEqual((self.store.manifest_path(result["releaseId"]).parent / "model.pt").read_bytes(), self.weight)

    def test_rollback_detects_published_file_tampering(self):
        first = self.publish(self.upload())
        model = self.store.manifest_path(first["releaseId"]).parent / "model.pt"
        model.chmod(0o600)
        model.write_bytes(b"changed")
        request = self.store.prepare("rollback", {"releaseId": first["releaseId"]})["request"]
        with self.assertRaises(ModelReleaseError):
            self.store.execute("rollback", request, "model_bad_rollback_001", "admin")

    def test_publication_capacity_is_bounded(self):
        self.store.max_total_bytes = 8
        with self.assertRaises(ModelReleaseError):
            self.publish(self.upload())
        self.assertEqual(self.store.list(), [])


if __name__ == "__main__":
    unittest.main()
