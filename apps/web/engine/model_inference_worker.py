#!/usr/bin/env python3
"""Persistent bounded JSONL inference worker, separate from Nautilus' event loop."""
from __future__ import annotations

import argparse
import asyncio
from collections import deque
import json
import sys
import time

from model_release import ModelAdapter

MAX_LINE = 4 * 1024 * 1024


class InferenceService:
    def __init__(self, adapter, emit):
        self.adapter, self.emit = adapter, emit
        self.runtime = adapter.manifest["runtime"]
        self.queue = asyncio.Queue(maxsize=self.runtime["maxQueueSize"])
        self.latencies = deque(maxlen=1024)
        self.counts = {"accepted": 0, "completed": 0, "failed": 0, "expired": 0, "queueRejected": 0}
        self.last_error = None

    def metrics(self):
        ordered = sorted(self.latencies)
        return {**self.counts, "queueDepth": self.queue.qsize(), "lastError": self.last_error,
            "latencySamples": len(ordered),
            "p50Ms": ordered[int((len(ordered) - 1) * .5)] if ordered else None,
            "p95Ms": ordered[int((len(ordered) - 1) * .95)] if ordered else None}

    def error(self, request, code, detail):
        self.last_error = detail
        self.emit({"type": "prediction", "requestId": request.get("requestId"), "error": code,
            "detail": detail, "releaseId": self.adapter.manifest["releaseId"],
            "modelHash": self.adapter.manifest["model"]["sha256"], "metrics": self.metrics()})

    def check_time(self, request, now):
        for field in ("asOfNs", "deadlineNs"):
            if type(request.get(field)) is not int:
                raise ValueError(f"{field} must be integer UTC nanoseconds")
        contract = self.adapter.contract(request)
        age_ms = self.runtime["maxInputAgeMs"]
        if isinstance(contract, dict) and contract.get("barSeconds"):
            age_ms = min(age_ms, 2 * contract["barSeconds"] * 1000)
        if request["asOfNs"] > now or now - request["asOfNs"] > age_ms * 1_000_000:
            raise TimeoutError("Input is future-dated or stale")
        if request["deadlineNs"] <= now:
            raise TimeoutError("Prediction deadline elapsed")
        if request["deadlineNs"] - now > self.runtime["timeoutMs"] * 1_000_000:
            raise ValueError("Deadline exceeds published timeout")

    def submit(self, request):
        if request.get("op") == "metrics":
            self.emit({"type": "metrics", "requestId": request.get("requestId"), **self.metrics()})
            return
        try:
            if not isinstance(request.get("requestId"), str) or not 1 <= len(request["requestId"]) <= 128:
                raise ValueError("requestId is required")
            self.check_time(request, time.time_ns())
            self.adapter.validate(request)
            self.queue.put_nowait((request, time.perf_counter_ns()))
            self.counts["accepted"] += 1
        except asyncio.QueueFull:
            self.counts["queueRejected"] += 1
            self.error(request, "queue_full", "Inference queue capacity exhausted")
        except TimeoutError as failure:
            self.counts["expired"] += 1
            self.error(request, "expired", str(failure))
        except (KeyError, TypeError, ValueError) as failure:
            self.counts["failed"] += 1
            self.error(request, "invalid_request", str(failure))

    async def run(self):
        carry = None
        while True:
            first = carry or await self.queue.get()
            carry = None
            batch = [first]
            first_contract = self.adapter.contract(first[0])
            while len(batch) < self.runtime["maxBatchSize"]:
                try:
                    item = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if self.adapter.contract(item[0]) != first_contract:
                    carry = item
                    break
                batch.append(item)
            active = []
            for request, queued in batch:
                try:
                    self.check_time(request, time.time_ns())
                    active.append((request, queued))
                except (TimeoutError, ValueError) as failure:
                    self.counts["expired"] += 1
                    self.error(request, "expired", str(failure))
                    self.queue.task_done()
            if not active:
                continue
            started = time.perf_counter_ns()
            try:
                # Exactly one model invocation at a time. CPU/GPU work runs off
                # this event loop; the parent kills this process on a hard hang.
                values = await asyncio.to_thread(self.adapter.predict_batch, [r for r, _ in active])
                completed = time.time_ns()
                for (request, queued), value in zip(active, values, strict=True):
                    elapsed = (time.perf_counter_ns() - queued) / 1e6
                    try:
                        self.check_time(request, completed)
                    except (ValueError, TimeoutError):
                        self.counts["expired"] += 1
                        self.error(request, "expired", "Model finished after deadline; output discarded")
                        continue
                    self.latencies.append(elapsed)
                    self.counts["completed"] += 1
                    self.emit({"type": "prediction", "requestId": request["requestId"],
                        "releaseId": request["releaseId"], "modelHash": request["modelHash"],
                        "instrument": request["instrument"], "domain": request.get("domain"),
                        "asOfNs": request["asOfNs"], "completedAtNs": completed,
                        "latencyMs": elapsed, "queueMs": (started - queued) / 1e6,
                        "inferenceMs": (time.perf_counter_ns() - started) / 1e6,
                        **value, "metrics": self.metrics()})
            except Exception as failure:
                for request, _ in active:
                    self.counts["failed"] += 1
                    self.error(request, "inference_failed", str(failure))
            finally:
                for _ in active:
                    self.queue.task_done()


def emit(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    sys.stdout.flush()


async def serve(args):
    started = time.perf_counter()
    adapter = await asyncio.to_thread(ModelAdapter, args.manifest, args.manifest_sha256, args.runner_root, args.contract_key)
    await asyncio.to_thread(adapter.warmup)
    service = InferenceService(adapter, emit)
    emit({"type": "ready", "releaseId": adapter.manifest["releaseId"],
        "modelHash": adapter.manifest["model"]["sha256"], "warmupMs": (time.perf_counter() - started) * 1000,
        "device": adapter.device, "modelVersion": adapter.manifest["modelVersion"],
        "runtimeLocation": "local_subprocess", "torchVersion": str(adapter.torch.__version__),
        "cudaVersion": adapter.torch.version.cuda,
        "parameters": sum(p.numel() for p in adapter.model.parameters()), "metrics": service.metrics()})
    reader = asyncio.StreamReader(limit=MAX_LINE)
    protocol = asyncio.StreamReaderProtocol(reader)
    await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    running = asyncio.create_task(service.run())
    try:
        while line := await reader.readline():
            try:
                request = json.loads(line, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
                if not isinstance(request, dict):
                    raise ValueError("JSON request must be an object")
                service.submit(request)
            except (ValueError, TypeError) as failure:
                service.error({}, "invalid_json", str(failure))
        await service.queue.join()
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--runner-root", help="Administrator-installed reviewed pipeline directory; never an upload")
    parser.add_argument("--contract-key", help="Administrator-selected RDT domain:instrument; required for RDT")
    args = parser.parse_args()
    try:
        asyncio.run(serve(args))
    except Exception as failure:
        emit({"type": "fatal", "error": str(failure)})
        raise SystemExit(1)


if __name__ == "__main__":
    main()
