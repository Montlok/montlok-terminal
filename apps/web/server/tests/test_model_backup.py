import hashlib
import io
import json
import sqlite3
import tarfile
import unittest

import test_model_releases as releases_fixture
import test_group_backup as groups_fixture
from test_artifact_backup import helper_from_deployment_script

add_models = helper_from_deployment_script("backup.py", "add_models")
verify_models = helper_from_deployment_script("check_backup.py", "verify_models")
add_groups = helper_from_deployment_script("backup.py", "add_groups")
verify_groups = helper_from_deployment_script("check_backup.py", "verify_groups")


class ModelBackupTests(unittest.TestCase):
    def setUp(self):
        self.fixture = releases_fixture.ModelReleaseTests()
        self.fixture.setUp()
        self.store = self.fixture.store
        self.publication = self.fixture.publish(self.fixture.upload())

    def tearDown(self):
        self.fixture.tearDown()

    def backup(self):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            summary = add_models(archive, self.store.root)
        self.assertEqual(summary["modelReleases"], 1)
        return buffer.getvalue()

    def test_published_index_weights_metadata_and_active_pointer_are_consistent(self):
        (self.store.root / "incoming/partial").write_text("not published")
        with tarfile.open(fileobj=io.BytesIO(self.backup()), mode="r:gz") as archive:
            result = verify_models(archive)
            self.assertEqual(result["modelReleases"], 1)
            self.assertFalse(any("incoming" in name or name.endswith(("-wal", "-shm", ".sock")) for name in archive.getnames()))
            db = sqlite3.connect(":memory:")
            try:
                db.deserialize(archive.extractfile("state/models/releases.sqlite").read())
                self.assertEqual(db.execute("SELECT release_id FROM active").fetchone()[0], self.publication["releaseId"])
                self.assertEqual(db.execute("SELECT result FROM operations").fetchone()[0], json.dumps(self.publication, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            finally:
                db.close()

    def test_corrupt_weights_and_missing_manifest_are_rejected_before_backup(self):
        model = self.store.manifest_path(self.publication["releaseId"]).parent / "model.pt"
        model.chmod(0o600)
        model.write_bytes(b"tampered")
        with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
            self.backup()

    def test_checker_rejects_tampered_or_missing_model_file(self):
        original = self.backup()
        target = "state/models/releases/" + self.publication["manifestSha256"] + "/model.pt"
        for mutation in ("missing", "changed"):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=io.BytesIO(original), mode="r:gz") as source, tarfile.open(fileobj=buffer, mode="w:gz") as output:
                for item in source:
                    content = source.extractfile(item).read()
                    if item.name == target:
                        if mutation == "missing":
                            continue
                        content = b"x" * len(content)
                    output.addfile(item, io.BytesIO(content))
            with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as archive:
                with self.assertRaises(ValueError):
                    verify_models(archive)

    def test_root_model_runtime_backup_includes_guard_runner_and_referenced_model_store(self):
        group = groups_fixture.GroupBackupTests()
        group.setUp()
        try:
            model_program = group.program / "engine"
            for filename in ("model_group_worker.py", "model_release.py", "model_client.py", "model_inference_worker.py",
                             "model_strategy.py", "model_market_actor.py", "model_probe.py", "commands.py", "control.py"):
                (model_program / filename).write_text("# host program\n")
            for filename in ("model_releases.py", "artifacts.py"):
                (group.program / "server" / filename).write_text("# host server\n")
            runner = group.program / "runner/recent_btc"
            runner.mkdir(parents=True)
            (runner / "train_gru.py").write_bytes(self.fixture.source)
            guard = group.program / "guard"
            guard.write_text("compiled guard fixture")
            settings = group.root / "model-settings.json"
            settings.write_text('{"environment":"sandbox"}')
            registry = json.loads(group.registry.read_text())
            registry["modelRuntime"] = {"storePath": str(self.store.root), "workerPath": str(model_program / "model_group_worker.py"),
                "pythonPath": registry["pythonPath"],
                "guardBinary": str(guard), "runnerRoot": str(runner.parent), "settingsPath": str(settings),
                "settingsSha256": hashlib.sha256(settings.read_bytes()).hexdigest(), "runtimePath": str(group.program / "hardened")}
            group.registry.write_text(json.dumps(registry))
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
                summary = add_groups(archive, group.registry, group.runs)
            with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as archive:
                self.assertEqual(verify_groups(archive), summary)
                self.assertIn("group-runtime/program/model/guard-binary", archive.getnames())
                self.assertIn("group-runtime/program/model/runner/recent_btc/train_gru.py", archive.getnames())
        finally:
            group.tearDown()


if __name__ == "__main__":
    unittest.main()
