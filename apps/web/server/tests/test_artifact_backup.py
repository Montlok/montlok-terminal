import ast
import io
import sqlite3
import tarfile
import tempfile
import unittest
from pathlib import Path

from artifacts import ArtifactStore


def helper_from_deployment_script(filename, function):
    """Load just imports and the pure helper, never the root-only backup CLI."""
    path = Path(__file__).resolve().parents[2] / "deploy" / filename
    source = ast.parse(path.read_text(), filename=str(path))
    source.body = [node for node in source.body if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))]
    namespace = {}
    exec(compile(source, str(path), "exec"), namespace)
    return namespace[function]


add_artifacts = helper_from_deployment_script("backup.py", "add_artifacts")
verify_artifacts = helper_from_deployment_script("check_backup.py", "verify_artifacts")


class ArtifactBackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = ArtifactStore(self.root / "state/artifacts")
        self.content = b'{"window":20}'
        self.artifact = self.store.register_stream([self.content], kind="factor", name="factor", version="1", filename="factor.json")

    def tearDown(self):
        self.temporary.cleanup()

    def backup(self):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            add_artifacts(archive, self.root)
        return buffer.getvalue()

    def test_backup_has_consistent_index_and_exact_content_without_incoming(self):
        (self.store.root / "incoming/uncommitted").write_bytes(b"private partial upload")
        (self.store.root / "objects" / ("f" * 64)).write_bytes(b"not in catalog")
        self.store.register_stream([self.content], kind="factor", name="factor", version="2", filename="factor.json")
        with tarfile.open(fileobj=io.BytesIO(self.backup()), mode="r:gz") as archive:
            self.assertEqual(verify_artifacts(archive), {"artifacts": 2, "artifactBytes": len(self.content)})
            self.assertEqual(len(archive.getmembers()), 2)
            blob = archive.getmember("state/artifacts/objects/" + self.artifact["sha256"])
            self.assertEqual(blob.mode, 0o400)
            database = sqlite3.connect(":memory:")
            try:
                database.deserialize(archive.extractfile("state/artifacts/catalog.sqlite").read())
                self.assertEqual(database.execute("SELECT count(*) FROM artifacts").fetchone()[0], 2)
            finally:
                database.close()

    def test_backup_refuses_corrupted_or_symlink_content(self):
        path = self.store.root / "objects" / self.artifact["sha256"]
        path.chmod(0o600)
        path.write_bytes(b"x" * len(self.content))
        with self.assertRaises(RuntimeError):
            self.backup()
        path.unlink()
        path.symlink_to(self.store.database)
        with self.assertRaises(OSError):
            self.backup()

    def test_checker_rejects_missing_and_tampered_blobs(self):
        original = self.backup()
        for mutation in ("missing", "tampered", "duplicate"):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=io.BytesIO(original), mode="r:gz") as source, tarfile.open(fileobj=buffer, mode="w:gz") as output:
                for item in source:
                    content = source.extractfile(item).read()
                    if "/objects/" in item.name:
                        if mutation == "missing":
                            continue
                        if mutation == "tampered":
                            content = b"x" * len(content)
                    output.addfile(item, io.BytesIO(content))
                    if mutation == "duplicate" and "/objects/" in item.name:
                        output.addfile(item, io.BytesIO(content))
            with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as archive:
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    verify_artifacts(archive)

    def test_legacy_archive_without_artifact_library_still_verifies(self):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz"):
            pass
        with tarfile.open(fileobj=io.BytesIO(buffer.getvalue()), mode="r:gz") as archive:
            self.assertEqual(verify_artifacts(archive), {"artifacts": 0, "artifactBytes": 0})

    def test_backup_refuses_library_with_missing_index(self):
        self.store.database.unlink()
        with self.assertRaises(RuntimeError):
            self.backup()


if __name__ == "__main__":
    unittest.main()
