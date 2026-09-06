"""Immutable artifact registration. Stored uploads are never imported or executed.

HTTP integration authenticates admin/CSRF before consuming multipart content.
``register_async`` accepts byte chunks from the file part; metadata fields are
kind/name/version/filename. The catalog records validation=pending_validation:
container checks are not model evaluation, strategy approval or activation.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import stat
import struct
import tempfile
import time
import unicodedata
import uuid
import zipfile
from contextlib import closing
from pathlib import Path, PurePosixPath


class ArtifactError(ValueError):
    status = 400


class ArtifactConflict(ArtifactError):
    status = 409


class ArtifactTooLarge(ArtifactError):
    status = 413


class ArtifactStore:
    FORMATS = {".json", ".onnx", ".safetensors", ".zip"}
    KINDS = {"factor": {".json", ".zip"}, "model": {".json", ".onnx", ".safetensors", ".zip"},
             "strategy": {".json", ".zip"}}
    CHUNK_BYTES = 64 * 1024
    MAX_ARCHIVE_FILES = 256
    MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
    MAX_ARCHIVE_RATIO = 100
    MAX_HEADER_BYTES = 8 * 1024 * 1024
    MAX_JSON_BYTES = 4 * 1024 * 1024

    def __init__(self, root: Path, max_bytes=64 * 1024 * 1024, max_artifacts=256, max_total_bytes=1024 * 1024 * 1024):
        self.root = Path(root)
        self.max_bytes, self.max_artifacts, self.max_total_bytes = max_bytes, max_artifacts, max_total_bytes
        for path in (self.root, self.root / "objects", self.root / "incoming"):
            if path.is_symlink():
                raise ArtifactError("文件库路径不能是符号链接")
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.chmod(0o700)
        self.database = self.root / "catalog.sqlite"
        if self.database.is_symlink():
            raise ArtifactError("文件目录索引不能是符号链接")
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS artifacts (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, version TEXT NOT NULL,
                filename TEXT NOT NULL, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL, format TEXT NOT NULL,
                created_at REAL NOT NULL, checks TEXT NOT NULL, UNIQUE(kind,name,version))""")
        self.database.chmod(0o600)

    def metadata(self, *, kind, name, version, filename):
        if not isinstance(kind, str) or kind not in self.KINDS:
            raise ArtifactError("文件类型须为 factor、model 或 strategy")
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80 or re.search(r"[\x00-\x1f\x7f]", name):
            raise ArtifactError("名称须为 1–80 个可见字符")
        if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+\-]{0,63}", version):
            raise ArtifactError("版本须为 1–64 个字母、数字或 . _ + -")
        if (not isinstance(filename, str) or not filename or len(filename) > 180
                or any(char in filename for char in "/\\:") or re.search(r"[\x00-\x1f\x7f]", filename)):
            raise ArtifactError("文件名无效")
        suffix = Path(filename).suffix.lower()
        if suffix not in self.FORMATS or suffix not in self.KINDS[kind]:
            raise ArtifactError("因子/策略支持 JSON、ZIP；模型支持 JSON manifest、ONNX、safetensors、ZIP")
        return {"kind": kind, "name": unicodedata.normalize("NFC", name.strip()), "version": version,
                "filename": filename, "format": suffix[1:]}

    def _incoming(self):
        fd, filename = tempfile.mkstemp(prefix="upload-", dir=self.root / "incoming")
        return os.fdopen(fd, "wb"), Path(filename)

    def _write_chunk(self, stream, digest, count, chunk):
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise ArtifactError("上传流必须为字节")
        count += len(chunk)
        if count > self.max_bytes:
            raise ArtifactTooLarge(f"单文件不能超过 {self.max_bytes // (1024 * 1024)} MiB")
        stream.write(chunk)
        digest.update(chunk)
        return count

    def register_stream(self, chunks, **fields):
        metadata = self.metadata(**fields)
        stream, incoming = self._incoming()
        digest, count = hashlib.sha256(), 0
        try:
            with stream:
                for chunk in chunks:
                    count = self._write_chunk(stream, digest, count, chunk)
                stream.flush()
                os.fsync(stream.fileno())
            return self._finish(incoming, metadata, digest.hexdigest(), count)
        finally:
            incoming.unlink(missing_ok=True)

    async def register_async(self, chunks, **fields):
        metadata = self.metadata(**fields)
        stream, incoming = self._incoming()
        digest, count = hashlib.sha256(), 0
        try:
            with stream:
                async for chunk in chunks:
                    count = self._write_chunk(stream, digest, count, chunk)
                stream.flush()
                os.fsync(stream.fileno())
            # Shield cleanup from request cancellation while the validator still
            # owns the file; wait for the bounded worker before removing it.
            task = asyncio.create_task(asyncio.to_thread(self._finish, incoming, metadata, digest.hexdigest(), count))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        finally:
            incoming.unlink(missing_ok=True)

    def register_file(self, path: Path, **fields):
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            raise ArtifactError("上传源须为普通文件")
        with path.open("rb") as stream:
            return self.register_stream(iter(lambda: stream.read(self.CHUNK_BYTES), b""), **fields)

    @staticmethod
    def _json(data):
        def invalid(_value):
            raise ValueError("Non-finite JSON number")
        try:
            value = json.loads(data, parse_constant=invalid)
        except (ValueError, UnicodeError, RecursionError) as error:
            raise ArtifactError("JSON 格式无效") from error
        if not isinstance(value, (dict, list)):
            raise ArtifactError("JSON 须为对象或数组")
        return value

    def _archive(self, path):
        try:
            # Check entry count before ZipFile allocates the central directory.
            with path.open("rb") as stream:
                stream.seek(max(0, path.stat().st_size - 65557))
                tail = stream.read(65557)
            end = tail.rfind(b"PK\x05\x06")
            if end < 0 or len(tail) - end < 22:
                raise ArtifactError("ZIP 目录无效")
            _signature, disk, directory_disk, disk_entries, entries, directory_bytes, _offset, comment = struct.unpack_from("<4s4H2LH", tail, end)
            if end + 22 + comment != len(tail) or disk or directory_disk or disk_entries != entries:
                raise ArtifactError("ZIP 目录或分卷格式无效")
            if entries > self.MAX_ARCHIVE_FILES or directory_bytes > self.MAX_ARCHIVE_FILES * 2048:
                raise ArtifactTooLarge("ZIP 条目数量或目录大小超过限制")
            with zipfile.ZipFile(path) as archive:
                infos = archive.infolist()
                if not infos or len(infos) > self.MAX_ARCHIVE_FILES:
                    raise ArtifactTooLarge(f"ZIP 须包含 1–{self.MAX_ARCHIVE_FILES} 个条目")
                seen, expanded, total_read = set(), 0, 0
                for item in infos:
                    name = item.orig_filename
                    parts = PurePosixPath(name).parts
                    if (not parts or name.startswith(("/", "\\")) or "\\" in name or ":" in name
                            or any(part in {".", ".."} for part in name.split("/"))
                            or re.search(r"[\x00-\x1f\x7f]", name) or len(name) > 256):
                        raise ArtifactError("ZIP 包含无效路径")
                    normalized = unicodedata.normalize("NFC", name).casefold().rstrip("/")
                    if normalized in seen:
                        raise ArtifactError("ZIP 包含重复路径")
                    seen.add(normalized)
                    filetype = stat.S_IFMT(item.external_attr >> 16)
                    if filetype not in {0, stat.S_IFREG, stat.S_IFDIR}:
                        raise ArtifactError("ZIP 不允许符号链接或特殊文件")
                    if item.flag_bits & 1:
                        raise ArtifactError("ZIP 不支持加密条目")
                    if item.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                        raise ArtifactError("ZIP 仅支持 Stored 或 Deflate 压缩")
                    expanded += item.file_size
                    if (expanded > self.MAX_ARCHIVE_BYTES or item.file_size > self.max_bytes
                            or item.file_size > max(1, item.compress_size) * self.MAX_ARCHIVE_RATIO):
                        raise ArtifactTooLarge("ZIP 展开大小或压缩比超过限制")
                    if item.is_dir():
                        continue
                    # Bounded decompression verifies actual size/CRC, with no
                    # extraction and no evaluation of member contents.
                    with archive.open(item) as member:
                        while chunk := member.read(self.CHUNK_BYTES):
                            total_read += len(chunk)
                            if total_read > self.MAX_ARCHIVE_BYTES:
                                raise ArtifactTooLarge("ZIP 展开大小超过限制")
                return {"container": "zip", "entries": len(infos), "expandedBytes": expanded}
        except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError, OSError) as error:
            raise ArtifactError("ZIP 格式或校验无效") from error

    def _checks(self, path, metadata, size):
        if size <= 0:
            raise ArtifactError("文件不能为空")
        if metadata["format"] == "json":
            if size > self.MAX_JSON_BYTES:
                raise ArtifactTooLarge("JSON 定义不能超过 4 MiB")
            self._json(path.read_bytes())
            return {"container": "json"}
        if metadata["format"] == "zip":
            return self._archive(path)
        if metadata["format"] == "safetensors":
            with path.open("rb") as stream:
                header_size = int.from_bytes(stream.read(8), "little")
                if not 2 <= header_size <= min(self.MAX_HEADER_BYTES, size - 8):
                    raise ArtifactError("safetensors 头部大小无效")
                header = self._json(stream.read(header_size))
                if not isinstance(header, dict):
                    raise ArtifactError("safetensors 头部须为对象")
            return {"container": "safetensors", "headerBytes": header_size}
        return {"container": "onnx", "structure": "pending_validation"}

    @staticmethod
    def _public(row):
        return {"id": row["id"], "kind": row["kind"], "name": row["name"], "version": row["version"],
                "filename": row["filename"], "sha256": row["sha256"], "bytes": row["bytes"], "format": row["format"],
                "createdAt": row["created_at"], "checks": json.loads(row["checks"]),
                "status": "registered", "validation": "pending_validation"}

    def _finish(self, incoming, metadata, digest, size):
        checks = self._checks(incoming, metadata, size)
        target = self.root / "objects" / digest
        with closing(sqlite3.connect(self.database, timeout=15)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT * FROM artifacts WHERE kind=? AND name=? AND version=?",
                                  (metadata["kind"], metadata["name"], metadata["version"])).fetchone()
            if previous:
                if previous["sha256"] != digest:
                    raise ArtifactConflict("该名称与版本已登记其他内容，请使用新版本号")
                return self._public(previous)
            if db.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0] >= self.max_artifacts:
                raise ArtifactTooLarge("文件版本数量已达到上限")
            stored = db.execute("SELECT COALESCE(SUM(bytes),0) FROM (SELECT sha256, MAX(bytes) bytes FROM artifacts GROUP BY sha256)").fetchone()[0]
            known = db.execute("SELECT 1 FROM artifacts WHERE sha256=? LIMIT 1", (digest,)).fetchone()
            if stored + (0 if known else size) > self.max_total_bytes:
                raise ArtifactTooLarge("文件库容量已达到上限")
            try:
                os.link(incoming, target)
                target.chmod(0o400)
            except FileExistsError:
                if target.is_symlink() or not target.is_file() or target.stat().st_size != size:
                    raise ArtifactError("文件库内容校验失败")
                with target.open("rb") as stream:
                    existing_digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if existing_digest != digest:
                    raise ArtifactError("文件库内容校验失败")
            identifier = uuid.uuid4().hex
            db.execute("INSERT INTO artifacts VALUES (?,?,?,?,?,?,?,?,?,?)",
                       (identifier, metadata["kind"], metadata["name"], metadata["version"], metadata["filename"],
                        digest, size, metadata["format"], time.time(), json.dumps(checks)))
            result = self._public(db.execute("SELECT * FROM artifacts WHERE id=?", (identifier,)).fetchone())
        return result

    def list(self, kind=None):
        if kind is not None and kind not in self.KINDS:
            raise ArtifactError("未知文件类型")
        with closing(sqlite3.connect(self.database)) as db:
            db.row_factory = sqlite3.Row
            rows = (db.execute("SELECT * FROM artifacts WHERE kind=? ORDER BY created_at DESC LIMIT 200", (kind,))
                    if kind else db.execute("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT 200"))
            return [self._public(row) for row in rows]

    def get(self, identifier):
        if not isinstance(identifier, str) or not re.fullmatch(r"[a-f0-9]{32}", identifier):
            raise KeyError("Artifact not found")
        with closing(sqlite3.connect(self.database)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM artifacts WHERE id=?", (identifier,)).fetchone()
            if row is None:
                raise KeyError("Artifact not found")
            return self._public(row)
