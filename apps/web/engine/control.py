"""Private UNIX control with durable, at-most-once operation receipts.

Socket delivery is not acknowledgement: a disconnected client must query the
same operation ID, never infer failure or create an automatic retry. SQLite
commits intent before the handler and its result before attempting a reply.
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import re
import sqlite3
import stat
import sys
import time
import uuid
from pathlib import Path

MAX_MESSAGE = 128 * 1024
OPERATION_ID = re.compile(r"^[A-Za-z0-9_-]{8,96}$")
COMMANDS = ("status", "receipt", "halt", "flatten", "reduce", "resume", "limit")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)


class ControlServer:
    def __init__(self, path: Path, handlers: dict):
        self.path = Path(path)
        self.handlers = handlers
        self.server = None
        self.lock = asyncio.Lock()
        self.active = set()
        self.closing = False
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        journal = self.path.parent / "control-operations.sqlite"
        if journal.is_symlink():
            raise ValueError("操作日志不能是符号链接")
        self.db = sqlite3.connect(journal)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA busy_timeout=15000")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS operations (
            id TEXT PRIMARY KEY, request TEXT NOT NULL, status TEXT NOT NULL,
            result TEXT, created REAL NOT NULL, updated REAL NOT NULL)""")
        with self.db:
            self.db.execute("UPDATE operations SET status='unknown' WHERE status='processing'")
        journal.chmod(0o600)

    def receipt(self, operation_id):
        if not isinstance(operation_id, str) or not OPERATION_ID.fullmatch(operation_id):
            raise ValueError("操作编号格式不正确")
        row = self.db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            return {"ok": True, "operationId": operation_id, "receiptStatus": "unknown", "found": False}
        result = json.loads(row["result"]) if row["result"] else {}
        return {**result, "operationId": operation_id, "receiptStatus": row["status"], "found": True,
                "ok": result.get("ok", True)}

    async def dispatch(self, request):
        if not isinstance(request, dict):
            raise ValueError("请求必须为 JSON 对象")
        command = request.get("command")
        if command == "receipt":
            return self.receipt(request.get("operationId"))
        handler = self.handlers.get(command)
        if handler is None:
            raise ValueError(f"未知命令 {command!r}")
        if command == "status":
            result = handler(request)
            if inspect.isawaitable(result):
                result = await result
            return {"ok": True, **result}
        operation_id = request.get("operationId")
        if not isinstance(operation_id, str) or not OPERATION_ID.fullmatch(operation_id):
            raise ValueError("写操作需要唯一 operationId")
        serialized = encode(request)
        async with self.lock:
            prior = self.db.execute("SELECT request FROM operations WHERE id=?", (operation_id,)).fetchone()
            if prior:
                if prior["request"] != serialized:
                    raise ValueError("操作编号已用于其他请求")
                return self.receipt(operation_id)
            if self.closing:
                response = {"ok": False, "error": "引擎正在停止，不接受新控制操作"}
                now = time.time()
                with self.db:
                    self.db.execute("INSERT INTO operations VALUES (?,?,?,?,?,?)",
                                    (operation_id, serialized, "failed", encode(response), now, now))
                return self.receipt(operation_id)
            now = time.time()
            with self.db:
                self.db.execute("INSERT INTO operations VALUES (?,?,?,NULL,?,?)",
                                (operation_id, serialized, "processing", now, now))
            # A handler can partially apply before raising. Such exceptions are
            # unknown, not a definitive rejection of the requested side effect.
            try:
                result = handler(request)
                if inspect.isawaitable(result):
                    result = await result
                response = {"ok": True, **result}
                status = "completed"
            except asyncio.CancelledError:
                with self.db:
                    self.db.execute("UPDATE operations SET status='unknown',updated=? WHERE id=?",
                                    (time.time(), operation_id))
                raise
            except Exception as error:
                response = {"ok": False, "error": str(error) or type(error).__name__}
                status = "unknown"
            with self.db:
                self.db.execute("UPDATE operations SET status=?,result=?,updated=? WHERE id=?",
                                (status, encode(response), time.time(), operation_id))
            return self.receipt(operation_id)

    async def start(self):
        if self.path.exists():
            if not stat.S_ISSOCK(self.path.stat().st_mode):
                raise ValueError("控制路径已被非 socket 文件占用")
            try:
                await call(self.path, "status", timeout=0.5)
            except (ConnectionRefusedError, FileNotFoundError):
                self.path.unlink(missing_ok=True)
            else:
                raise ValueError("控制服务已存在")
        self.server = await asyncio.start_unix_server(self.serve, path=str(self.path), limit=MAX_MESSAGE)
        os.chmod(self.path, 0o600)

    async def quiesce(self):
        """Reject new writes and finish accepted writes before stopping a node."""
        self.closing = True
        async with self.lock:
            pass

    async def stop(self):
        await self.quiesce()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            # Acknowledged or disconnected requests finish journaling before
            # the worker is disposed. Never cancel them merely on socket close.
            if self.active:
                await asyncio.gather(*self.active, return_exceptions=True)
            self.path.unlink(missing_ok=True)
            self.server = None
        self.db.close()

    async def serve(self, reader, writer):
        task = asyncio.current_task()
        self.active.add(task)
        try:
            try:
                raw = await asyncio.wait_for(reader.readline(), 5)
                if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE:
                    raise ValueError("请求过大或不完整")
                response = await self.dispatch(json.loads(raw))
            except Exception as error:
                response = {"ok": False, "error": str(error) or type(error).__name__}
            try:
                writer.write((encode(response) + "\n").encode())
                await asyncio.wait_for(writer.drain(), 5)
            except (OSError, TimeoutError):
                pass  # Result already committed; client can query it later.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.active.discard(task)


async def call(path: Path, command: str, timeout=5.0, **arguments):
    # Generate once at the client boundary; an unknown response is never retried.
    if command not in {"status", "receipt"}:
        arguments.setdefault("operationId", uuid.uuid4().hex)
    reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(path), limit=MAX_MESSAGE), timeout)
    try:
        writer.write((encode({"command": command, **arguments}) + "\n").encode())
        await asyncio.wait_for(writer.drain(), timeout)
        raw = await asyncio.wait_for(reader.readline(), timeout)
        if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE:
            raise ValueError("控制响应不完整")
        return json.loads(raw)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--operation-id", default=None)
    parser.add_argument("--reason", default="cli")
    parser.add_argument("--instrument")
    parser.add_argument("--notional")
    args = parser.parse_args()
    extra = {"instrument": args.instrument, "notional": args.notional} if args.command == "limit" else {}
    operation_id = args.operation_id or uuid.uuid4().hex
    try:
        reply = asyncio.run(call(args.socket, args.command, reason=args.reason,
            operator=os.environ.get("USER", "cli"), operationId=operation_id, **extra))
    except (OSError, TimeoutError, ValueError) as error:
        reply = {"ok": False, "operationId": operation_id, "receiptStatus": "unknown", "error": str(error)}
    print(json.dumps(reply, ensure_ascii=False, indent=2))
    sys.exit(0 if reply.get("ok") and reply.get("receiptStatus") != "unknown" else 1)
