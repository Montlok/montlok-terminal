import asyncio
import hashlib
import io
import json
import stat
import tempfile
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from artifacts import ArtifactConflict, ArtifactError, ArtifactStore, ArtifactTooLarge


def archive(entries, compression=zipfile.ZIP_STORED):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=compression) as output:
        for name, body in entries:
            output.writestr(name, body)
    return data.getvalue()


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name) / "artifacts"
        self.store = ArtifactStore(self.root)
        self.fields = {"kind": "factor", "name": "动量因子", "version": "1.0.0", "filename": "factor.json"}

    def tearDown(self):
        self.directory.cleanup()

    def register(self, content=b'{"window":20}', **changes):
        return self.store.register_stream([content], **{**self.fields, **changes})

    def test_registration_is_content_addressed_private_and_pending_validation(self):
        content = b'{"window":20}'
        result = self.register(content)
        self.assertEqual(result["sha256"], hashlib.sha256(content).hexdigest())
        self.assertEqual(result["validation"], "pending_validation")
        self.assertEqual(result["status"], "registered")
        self.assertEqual((self.root / "objects" / result["sha256"]).read_bytes(), content)
        self.assertEqual(stat.S_IMODE((self.root / "objects" / result["sha256"]).stat().st_mode), 0o400)
        self.assertEqual(list((self.root / "incoming").iterdir()), [])
        self.assertEqual(self.store.get(result["id"]), result)
        self.assertEqual(self.store.list("factor"), [result])
        self.assertEqual(self.store.list("model"), [])
        self.assertNotIn("path", result)

    def test_version_is_immutable_and_same_bytes_are_idempotent(self):
        first = self.register()
        self.assertEqual(self.register(), first)
        with self.assertRaises(ArtifactConflict):
            self.register(b'{"window":60}')
        second = self.register(b'{"window":60}', version="1.1.0")
        self.assertNotEqual(first["sha256"], second["sha256"])
        self.assertEqual(len(self.store.list()), 2)
        self.assertEqual(self.store.get(first["id"])["version"], "1.0.0")

    def test_concurrent_identical_registrations_remain_single_record(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _index: self.register(), range(8)))
        self.assertEqual(len({item["id"] for item in results}), 1)
        self.assertEqual(len(self.store.list()), 1)

    def test_invalid_metadata_and_top_level_extensions(self):
        for changes in ({"kind": "live"}, {"name": ""}, {"name": "x\n"}, {"version": "../v1"},
                        {"filename": "../model.json"}, {"filename": "C:\\x.json"},
                        {"filename": "model.pkl"}, {"kind": "strategy", "filename": "weights.onnx"}):
            with self.subTest(changes=changes), self.assertRaises(ArtifactError):
                self.register(**changes)
        self.assertEqual(self.store.list(), [])
        self.assertEqual(list((self.root / "incoming").iterdir()), [])

    def test_model_json_manifest_registration_is_not_publication(self):
        result = self.register(kind="model", filename="manifest.json")
        self.assertEqual(result["format"], "json")
        self.assertEqual(result["validation"], "pending_validation")
        self.assertEqual(result["status"], "registered")

    def test_invalid_or_empty_json_does_not_leave_registered_objects(self):
        for content in (b"", b"not JSON", b"42", b'{"value":NaN}', b'\xff\xfe'):
            with self.subTest(content=content), self.assertRaises(ArtifactError):
                self.register(content)
        self.assertEqual(list((self.root / "objects").iterdir()), [])

    def test_stream_size_limit_and_interrupted_upload_cleanup(self):
        self.store.max_bytes = 10
        with self.assertRaises(ArtifactTooLarge):
            self.register(b'{"window":20}')
        def chunks():
            yield b"{"
            raise RuntimeError("request interrupted")
        with self.assertRaises(RuntimeError):
            self.store.register_stream(chunks(), **self.fields)
        self.assertEqual(list((self.root / "incoming").iterdir()), [])

    def test_json_definition_has_separate_memory_limit(self):
        self.store.MAX_JSON_BYTES = 8
        with self.assertRaises(ArtifactTooLarge):
            self.register()

    def test_valid_strategy_zip_is_not_extracted_or_imported(self):
        payload = archive([("manifest.json", '{"entrypoint":"strategy.py"}'),
                           ("strategy.py", "raise RuntimeError('must never execute')")])
        result = self.register(payload, kind="strategy", filename="strategy.zip")
        self.assertEqual(result["checks"]["entries"], 2)
        self.assertEqual(result["validation"], "pending_validation")
        self.assertFalse((self.root / "strategy.py").exists())
        self.assertEqual(len(list((self.root / "objects").iterdir())), 1)

    def test_zip_traversal_absolute_and_duplicate_paths_are_rejected(self):
        for entries in ([('../outside.py', 'x')], [('/absolute.py', 'x')], [('C:/file.py', 'x')],
                        [('dir\\file.py', 'x')], [('A.json', '{}'), ('a.json', '{}')]):
            with self.subTest(entries=entries), self.assertRaises(ArtifactError):
                self.register(archive(entries), kind="strategy", filename="bundle.zip")

    def test_zip_symlinks_and_special_files_are_rejected(self):
        for filetype in (stat.S_IFLNK, stat.S_IFIFO):
            member = zipfile.ZipInfo("link")
            member.create_system = 3
            member.external_attr = (filetype | 0o777) << 16
            with self.subTest(filetype=filetype), self.assertRaises(ArtifactError):
                self.register(archive([(member, "../outside")]), filename="bundle.zip")

    def test_zip_bomb_file_count_and_expansion_limits(self):
        with self.assertRaises(ArtifactTooLarge):
            self.register(archive([("data.bin", b"0" * 100000)], zipfile.ZIP_DEFLATED), filename="bomb.zip")
        self.store.MAX_ARCHIVE_FILES = 2
        with self.assertRaises(ArtifactTooLarge):
            self.register(archive([(f"{i}.json", "{}") for i in range(3)]), filename="many.zip")
        self.store.MAX_ARCHIVE_BYTES = 10
        with self.assertRaises(ArtifactTooLarge):
            self.register(archive([("data.bin", b"x" * 20)]), filename="expanded.zip")

    def test_corrupt_archive_rejected(self):
        payload = bytearray(archive([("data.bin", b"content")]))
        offset = payload.index(b"content")
        payload[offset] ^= 0x01
        with self.assertRaises(ArtifactError):
            self.register(payload, filename="corrupt.zip")

    def test_quota_limits_and_blob_deduplication(self):
        self.store.max_total_bytes = len(b'{"window":20}')
        self.register()
        self.register(version="1.0.1")
        self.assertEqual(len(list((self.root / "objects").iterdir())), 1)
        with self.assertRaises(ArtifactTooLarge):
            self.register(b'{"window":60}', version="2.0")
        self.store.max_artifacts = 2
        with self.assertRaises(ArtifactTooLarge):
            self.register(version="3.0")

    def test_opaque_model_is_registered_but_not_marked_structurally_valid(self):
        result = self.register(b"opaque protobuf bytes", kind="model", filename="model.onnx")
        self.assertEqual(result["checks"]["structure"], "pending_validation")
        self.assertEqual(result["validation"], "pending_validation")
        header = json.dumps({"__metadata__": {"source": "test"}}).encode()
        safe = len(header).to_bytes(8, "little") + header
        result = self.register(safe, kind="model", filename="model.safetensors", version="2.0")
        self.assertEqual(result["checks"]["headerBytes"], len(header))

    def test_async_stream_and_invalid_lookup(self):
        async def chunks():
            yield b'{"win'
            yield b'dow":20}'
        result = asyncio.run(self.store.register_async(chunks(), **self.fields))
        self.assertEqual(result["bytes"], 13)
        for identifier in ("../catalog.sqlite", "a" * 32):
            with self.assertRaises(KeyError):
                self.store.get(identifier)
        with self.assertRaises(ArtifactError):
            self.store.list("unknown")


if __name__ == "__main__":
    unittest.main()
