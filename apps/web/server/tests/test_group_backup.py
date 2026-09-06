import hashlib
import io
import json
import sqlite3
import tarfile
import tempfile
import unittest
from pathlib import Path

from test_artifact_backup import helper_from_deployment_script

add_groups = helper_from_deployment_script("backup.py", "add_groups")
verify_groups = helper_from_deployment_script("check_backup.py", "verify_groups")


class GroupBackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.program = self.root / "program"
        for directory in (self.program / "server", self.program / "engine", self.program / "hardened"):
            directory.mkdir(parents=True)
        self.worker = self.program / "engine/group_worker.py"
        self.worker.write_text("# worker release\n")
        for filename in ("group_runtime.py", "managed_run_view.py"):
            (self.program / "server" / filename).write_text("# server release\n")
        for filename in ("settings.py", "run.py", "node_config.py", "health.py", "commands.py", "control.py", "alerts.py"):
            (self.program / "hardened" / filename).write_text("# hardened release\n")
        group = {"id": "baseline", "environment": "sandbox", "runtimePath": str(self.program / "hardened")}
        for label in ("settings", "signals", "strategy"):
            path = self.root / label
            path.write_text("# release " + label)
            group.update({label + "Path": str(path), label + "Sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        self.registry = self.root / "registry.json"
        self.registry.write_text(json.dumps({"version": 1, "groups": [group], "pythonPath": str(self.root / "venv/bin/python"),
                                            "workerPath": str(self.worker)}))
        self.runs = self.root / "runs"
        self.runs.mkdir()
        self.database = sqlite3.connect(self.runs / "runs.sqlite")
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.execute("CREATE TABLE runs(id TEXT, group_id TEXT, state TEXT, pid INTEGER, process_created REAL)")
        self.database.execute("INSERT INTO runs VALUES('baseline-123456789abcdef0','baseline','running',132044,1788620000.5)")
        self.database.commit()
        self.run = self.runs / "baseline-123456789abcdef0"
        self.run.mkdir()
        for filename in ("request.json", "manifest.json", "status.json", "view.json", "equity.jsonl", "engine.log"):
            (self.run / filename).write_text('{"runId":"baseline-123456789abcdef0"}\n')
        (self.run / "engine.pid").write_text("132044")
        (self.run / "control.sock").write_text("excluded placeholder")

    def tearDown(self):
        self.database.close()
        self.temporary.cleanup()

    def backup(self):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            summary = add_groups(archive, self.registry, self.runs)
        self.assertEqual(summary, {"groups": 1, "groupRuns": 1})
        return buffer.getvalue()

    def test_consistent_evidence_backup_disarms_recovery_not_running_database(self):
        with tarfile.open(fileobj=io.BytesIO(self.backup()), mode="r:gz") as archive:
            self.assertEqual(verify_groups(archive), {"groups": 1, "groupRuns": 1})
            names = archive.getnames()
            self.assertFalse(any(name.endswith((".sock", ".pid", "-wal", "-shm")) for name in names))
            self.assertIn("group-runtime/program/engine/group_worker.py", names)
            self.assertIn("group-runtime/releases/baseline/hardened/node_config.py", names)
            recovered = sqlite3.connect(":memory:")
            try:
                recovered.deserialize(archive.extractfile("group-runtime/state/runs.sqlite").read())
                self.assertEqual(recovered.execute("SELECT state,pid,process_created FROM runs").fetchone(), ("interrupted", None, None))
            finally:
                recovered.close()
            notes = json.loads(archive.extractfile("group-runtime/recovery.json").read())
            self.assertTrue(notes["runs"][0]["liveFilesMayHavePartialTail"])
            self.assertFalse(notes["dependencies"]["restoreStartsProcesses"])
        self.assertEqual(self.database.execute("SELECT state,pid FROM runs").fetchone(), ("running", 132044))

    def test_missing_or_changed_release_is_rejected(self):
        (self.root / "signals").write_text("changed")
        with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
            self.backup()

    def test_checker_detects_missing_or_corrupt_run_evidence(self):
        original = self.backup()
        target = "group-runtime/runs/baseline-123456789abcdef0/engine.log"
        for mutation in ("missing", "corrupt"):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=io.BytesIO(original), mode="r:gz") as source, tarfile.open(fileobj=buffer, mode="w:gz") as result:
                for item in source:
                    content = source.extractfile(item).read()
                    if item.name == target:
                        if mutation == "missing":
                            continue
                        content = b"x" * len(content)
                    result.addfile(item, io.BytesIO(content))
            with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as archive:
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    verify_groups(archive)

    def test_worker_operation_journal_snapshot_preserves_receipts_without_replay(self):
        journal = sqlite3.connect(self.run / "control-operations.sqlite")
        try:
            journal.execute("PRAGMA journal_mode=WAL")
            journal.execute("CREATE TABLE operations(id TEXT PRIMARY KEY,status TEXT,result TEXT)")
            journal.execute("INSERT INTO operations VALUES('halt-finished','completed','{\"tradingState\":\"HALTED\"}')")
            journal.execute("INSERT INTO operations VALUES('halt-interrupted','processing',NULL)")
            journal.commit()
            with tarfile.open(fileobj=io.BytesIO(self.backup()), mode="r:gz") as archive:
                self.assertEqual(verify_groups(archive), {"groups": 1, "groupRuns": 1})
                copied = sqlite3.connect(":memory:")
                try:
                    copied.deserialize(archive.extractfile("group-runtime/runs/baseline-123456789abcdef0/control-operations.sqlite").read())
                    self.assertEqual(copied.execute("SELECT status FROM operations WHERE id='halt-interrupted'").fetchone()[0], "unknown")
                    self.assertEqual(copied.execute("SELECT status FROM operations WHERE id='halt-finished'").fetchone()[0], "completed")
                finally:
                    copied.close()
            self.assertEqual(journal.execute("SELECT status FROM operations WHERE id='halt-interrupted'").fetchone()[0], "processing")
        finally:
            journal.close()


if __name__ == "__main__":
    unittest.main()
