"""Async model + mandatory Rust guard subprocesses; no Python guard fallback."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import time

from model_release import load_manifest


class JsonProcess:
    def __init__(self, command):
        self.command, self.process = command, None
        self.lock = asyncio.Lock()

    async def start(self):
        self.process = await asyncio.create_subprocess_exec(*self.command, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=None, limit=4 * 1024 * 1024)

    async def read(self):
        line = await self.process.stdout.readline()
        if not line:
            raise RuntimeError("Model subprocess exited")
        value = json.loads(line)
        if value.get("error") or value.get("ok") is False:
            raise RuntimeError(str(value.get("error", value)))
        return value

    async def exchange(self, value):
        async with self.lock:
            self.process.stdin.write((json.dumps(value, allow_nan=False) + "\n").encode())
            await self.process.stdin.drain()
            return await self.read()

    async def close(self):
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 3)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()


class InferenceProcess(JsonProcess):
    """Multiplex request IDs so the model process can batch queued windows."""
    def __init__(self, command):
        super().__init__(command)
        self.pending = {}
        self.reader_task = None

    def start_dispatch(self):
        self.reader_task = asyncio.create_task(self.dispatch())

    async def dispatch(self):
        try:
            while line := await self.process.stdout.readline():
                response = json.loads(line)
                future = self.pending.pop(response.get("requestId"), None)
                if future is None or future.done():
                    continue
                if response.get("error"):
                    future.set_exception(RuntimeError(f"{response['error']}: {response.get('detail', '')}"))
                else:
                    future.set_result(response)
            raise RuntimeError("Inference worker exited")
        except BaseException as failure:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(RuntimeError(str(failure) or "Inference worker stopped"))
            self.pending.clear()

    async def exchange(self, value):
        request_id = value["requestId"]
        if request_id in self.pending:
            raise ValueError("Duplicate in-flight inference requestId")
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            async with self.lock:
                self.process.stdin.write((json.dumps(value, allow_nan=False) + "\n").encode())
                await self.process.stdin.drain()
            return await future
        finally:
            self.pending.pop(request_id, None)

    async def close(self):
        await super().close()
        if self.reader_task:
            self.reader_task.cancel()
            await asyncio.gather(self.reader_task, return_exceptions=True)


class ModelGuardClient:
    def __init__(self, manifest, manifest_sha256, guard_binary, runner_root=None, python=sys.executable, contract_key=None):
        if not guard_binary or not Path(guard_binary).is_file():
            raise ValueError("An explicit installed --guard-binary is required; no Python fallback")
        self.manifest_path = str(Path(manifest).resolve())
        self.manifest = load_manifest(manifest, manifest_sha256)
        self.contract_key = contract_key
        if self.manifest["runnerId"] == "rdt4quant_v1" and contract_key not in self.manifest["domainContracts"]:
            raise ValueError("RDT client needs the host-selected published contract")
        self.guard = JsonProcess([str(Path(guard_binary).resolve())])
        command = [python, str(Path(__file__).with_name("model_inference_worker.py")),
            "--manifest", self.manifest_path, "--manifest-sha256", manifest_sha256]
        if runner_root:
            command += ["--runner-root", str(runner_root)]
        if contract_key:
            command += ["--contract-key", contract_key]
        self.worker = InferenceProcess(command)
        self.limit = asyncio.Semaphore(self.manifest["runtime"]["maxQueueSize"])
        self.closed = False
        self.latest = None
        self.ready = None

    async def start(self):
        try:
            await self.guard.start()
            await asyncio.wait_for(self.guard.exchange({"op": "init", "release": self.manifest}), 10)
            await self.worker.start()
            ready = await asyncio.wait_for(self.worker.read(), 300)
            if ready.get("type") != "ready" or ready.get("modelHash") != self.manifest["model"]["sha256"]:
                raise RuntimeError("Worker readiness identity mismatch")
            self.worker.start_dispatch()
            self.ready = ready
            return ready
        except BaseException:
            await self.close()
            raise

    @property
    def is_ready(self):
        return self.ready is not None and self.failure_reason() is None

    def failure_reason(self):
        if self.closed:
            return "Model client is stopped"
        for name, child in (("guard", self.guard), ("inference", self.worker)):
            if child.process is None:
                return f"Model {name} process has not started"
            if child.process.returncode is not None:
                return f"Model {name} process exited with code {child.process.returncode}"
        return None

    async def predict(self, request):
        if not self.is_ready:
            raise RuntimeError(self.failure_reason() or "Model warmup is incomplete")
        if self.contract_key and f"{request.get('domain')}:{request.get('instrument')}" != self.contract_key:
            raise ValueError("Input contract differs from host-selected model group")
        if self.limit.locked():
            raise RuntimeError("Model client queue is full")
        async with self.limit:
            timeout = self.manifest["runtime"]["timeoutMs"] / 1000
            try:
                async with asyncio.timeout(timeout):
                    prepared = await self.guard.exchange({**request, "op": "prepare", "nowNs": time.time_ns()})
                    worker_request = prepared["workerRequest"]
                    prediction = await self.worker.exchange(worker_request)
                    checked = await self.guard.exchange({"op": "validate_prediction", "nowNs": time.time_ns(),
                        "prediction": prediction, "request": request})
                    self.latest = {"prediction": prediction, "checked": checked}
                    return self.latest
            except TimeoutError:
                # A timed-out native/GPU call cannot be cancelled in a thread;
                # terminate the dedicated process so it cannot emit late orders.
                await self.close()
                raise RuntimeError("Inference deadline exceeded; model processes stopped") from None

    async def close(self):
        self.closed = True
        await asyncio.gather(self.guard.close(), self.worker.close())
