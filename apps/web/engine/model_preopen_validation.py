"""Explicit isolated Tokyo model smoke, invoked only by the administrator.

No existing services or groups are modified. All mutable state is underneath
/www/nautilus/model-preopen-20260906 and this process only starts shadow groups.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import time
import uuid

BASE = Path("/usr/local/lib/montlok-model-preopen-20260906")
STATE = Path("/www/nautilus/model-preopen-20260906")
PYTHON = Path("/opt/montlok-model-runtime/bin/python")
ENGINE = BASE / "operator_terminal/engine"
sys.path.insert(0, str(BASE / "operator_terminal/server"))
from artifacts import ArtifactStore
from model_releases import ModelReleaseStore
from group_runtime import GroupSupervisor, LaunchRegistry
from model_release import sha


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False, default=str) + "\n")
    temporary.replace(path)


def emit(event, **values):
    print(json.dumps({"event": event, "utc": datetime.now(timezone.utc).isoformat(), **values}, allow_nan=False, default=str), flush=True)


def setup():
    if os.geteuid() != 0 or Path(__file__).resolve() != ENGINE / "model_preopen_validation.py":
        raise ValueError("Setup requires the fixed root-installed validation script")
    if not STATE.is_dir() or not (BASE / "bin/nautilus-model-guard").is_file():
        raise ValueError("Independent state directory and installed Linux guard must already exist")
    settings = {"trader_id": "MODEL-PREOPEN", "environment": "sandbox", "state_dir": str(STATE / "unused-template-state"),
        "instruments": ["BTC-USDT.OKX"], "instrument_types": ["SPOT"], "strategies": [], "capture": True,
        "sandbox_balance": "10000 USDT", "log_level": "INFO",
        "risk": {"max_notional_per_order": {"BTC-USDT.OKX": 1000}},
        "alerts": {"webhook": None, "heartbeat_mins": 0, "stale_after_secs": 60, "rss_limit_mb": 2048}}
    save(BASE / "settings.json", settings)
    registry = {"version": 1, "pythonPath": str(PYTHON), "workerPath": str(ENGINE / "group_worker.py"), "groups": [],
        "modelRuntime": {"storePath": str(STATE / "model-store"), "workerPath": str(ENGINE / "model_group_worker.py"),
            "pythonPath": str(PYTHON), "guardBinary": str(BASE / "bin/nautilus-model-guard"),
            "runnerRoot": str(BASE / "pipelines"), "settingsPath": str(BASE / "settings.json"),
            "settingsSha256": sha(BASE / "settings.json"), "runtimePath": str(ENGINE), "mode": "shadow",
            "maxBudgetUsdt": "10000.00", "maxDurationSeconds": 180}}
    save(BASE / "registry.json", registry)
    emit("isolated_setup", registry=str(BASE / "registry.json"), state=str(STATE), guardHash=sha(BASE / "bin/nautilus-model-guard"))


async def resident_benchmark(manifest, manifest_sha, window):
    from model_client import ModelGuardClient
    import psutil
    client = ModelGuardClient(manifest, manifest_sha, BASE / "bin/nautilus-model-guard", BASE / "pipelines")
    ready = await client.start()
    timings, worker_times = [], []
    try:
        for index in range(100):
            now = time.time_ns()
            # Current-market values, explicitly synthetic benchmark event
            # envelopes. Only the separate fresh probe is market-valid proof.
            request = {**window, "requestId": "cpu-benchmark-" + str(index), "releaseId": client.manifest["releaseId"],
                "modelHash": client.manifest["model"]["sha256"], "asOfNs": now - 1_000_000,
                "deadlineNs": now + 1_900_000_000, "mode": "shadow", "normalized": False}
            before = time.perf_counter()
            result = await client.predict(request)
            timings.append((time.perf_counter() - before) * 1000)
            worker_times.append(result["prediction"]["latencyMs"])
        worker_rss = psutil.Process(client.worker.process.pid).memory_info().rss
        import numpy as np
        c = client.manifest["featureContract"]
        normalized = np.clip((np.asarray(window["inputs"]) - c["mean"]) / c["scale"], *c["clip"]).astype("float32").tolist()
        deadline_results = []
        for index in range(12):
            now = time.time_ns()
            short = {**request, "requestId": f"deadline-probe-{index}", "inputs": normalized, "normalized": True,
                "asOfNs": now - 1_000_000, "deadlineNs": now + (2_000_000 if index % 2 else 3_000_000)}
            try:
                reply = await client.worker.exchange(short)
                deadline_results.append({"accepted": True, "latencyMs": reply["latencyMs"]})
            except RuntimeError as failure:
                deadline_results.append({"accepted": False, "error": str(failure)})
        # A dead resident worker must immediately invalidate model readiness.
        await client.worker.close()
        invalidated = not client.is_ready and client.failure_reason() is not None
        return {"kind": "synthetic_envelope_cpu_benchmark_not_market_events", "iterations": 100,
            "endToEndP50Ms": statistics.median(timings), "endToEndP95Ms": sorted(timings)[94],
            "endToEndP99Ms": sorted(timings)[98], "workerP50Ms": statistics.median(worker_times),
            "workerP95Ms": sorted(worker_times)[94], "residentWorkerRssBytes": worker_rss,
            "deadWorkerReadinessInvalidated": invalidated,
            "deadlineProbe": deadline_results,
            "latePostInferenceDiscardObserved": any("Model finished after deadline" in row.get("error", "") for row in deadline_results),
            "ready": ready, "ordersSent": False}
    finally:
        await client.close()


async def run_validation():
    if os.geteuid() == 0:
        raise ValueError("Run the isolated supervisor as the nautilus service user, not root")
    artifacts = ArtifactStore(STATE / "artifacts")
    releases = ModelReleaseStore(STATE / "model-store", artifacts)
    bundle = BASE / "gru-latest-20260906-v1.zip"
    existing = [a for a in artifacts.list() if a["name"] == "GRU preopen validation"]
    artifact = existing[0] if existing else artifacts.register_file(bundle, kind="model", name="GRU preopen validation",
        version="20260906-v1", filename=bundle.name)
    preview = releases.validate(artifact["id"])
    prepared = releases.prepare("publish", {"artifactId": artifact["id"]})
    receipt = releases.execute("publish", prepared["request"], "preopen-publish-" + uuid.uuid4().hex, "isolated-technical-validation")
    save(STATE / "publication.json", {"receipt": receipt, "preview": preview})
    emit("published_in_isolated_store", releaseId=preview["releaseId"], manifestSha256=preview["manifestSha256"], started=False)
    registry = LaunchRegistry(BASE / "registry.json")
    supervisor = GroupSupervisor(registry, STATE / "runs")
    group_id = "model-" + preview["manifestSha256"][:16]
    spec = registry.validate_inputs(group_id)
    manifest = releases.manifest_path(preview["releaseId"])
    from model_live_probe import probe
    args = argparse.Namespace(manifest=str(manifest), manifest_sha256=preview["manifestSha256"],
        runner_root=str(BASE / "pipelines"), guard_binary=str(BASE / "bin/nautilus-model-guard"),
        fixture_output=str(STATE / "fresh-window.json"))
    fresh = await probe(args)
    save(STATE / "fresh-probe.json", fresh)
    emit("fresh_guarded_prediction", asOfNs=fresh["asOfNs"], prediction=fresh["reference"]["prediction"],
        difference=fresh["maxAbsPredictionDifference"], ordersSent=False)
    benchmark = await resident_benchmark(manifest, preview["manifestSha256"], json.loads((STATE / "fresh-window.json").read_text()))
    save(STATE / "cpu-benchmark.json", benchmark)
    emit("cpu_benchmark", **{k: v for k, v in benchmark.items() if k != "ready"})
    before = await supervisor.prepare({"groupId": group_id, "action": "start", "budgetUsdt": "10000.00", "durationSeconds": 180})
    launched = await supervisor.execute(before["request"], "preopen-start-" + uuid.uuid4().hex)
    save(STATE / "launch.json", launched)
    if not launched.get("runId"):
        raise RuntimeError(str(launched))
    run_id = launched["runId"]
    directory = STATE / "runs" / run_id
    emit("native_shadow_started", **launched)
    import psutil
    started, first_ready, first_prediction = time.monotonic(), None, None
    samples = []
    sent_stop = False
    controls = []
    try:
        while time.monotonic() - started < 215:
            status = await supervisor.run_status(supervisor.row(run_id, group_id))
            elapsed = time.monotonic() - started
            engine = status.get("engine", {})
            model = engine.get("model", {})
            if engine.get("warmupComplete") and first_ready is None:
                first_ready = elapsed
            if model.get("latestTarget") and engine.get("marketReady") is True and first_prediction is None:
                first_prediction = elapsed
            row = supervisor.row(run_id, group_id)
            memory = None
            try:
                process = psutil.Process(row["pid"])
                memory = sum(p.memory_info().rss for p in [process, *process.children(recursive=True)] if p.is_running())
            except psutil.Error:
                pass
            samples.append({"elapsedSeconds": elapsed, "state": status["status"], "engine": engine,
                "processTreeRssBytes": memory})
            if len(samples) % 5 == 1:
                emit("native_shadow_progress", runId=run_id, elapsedSeconds=round(elapsed, 1), state=status["status"],
                    warmupComplete=engine.get("warmupComplete"), marketReady=engine.get("marketReady"), windowReady=model.get("windowReady"),
                    connected=engine.get("connected"), processTreeRssBytes=memory)
            if status["status"] not in {"starting", "running", "halted", "reducing", "stopping", "unresponsive", "error", "recovering", "engine_stopped"}:
                break
            for action, delay, expected in (("reduce", 20, "reducing"), ("halt", 35, "halted"), ("resume", 50, "running")):
                if (first_prediction is not None and elapsed - first_prediction >= delay
                        and action not in {c["action"] for c in controls}):
                    checked = await supervisor.prepare({"groupId": group_id, "runId": run_id, "action": action})
                    operation_id = f"preopen-{action}-" + uuid.uuid4().hex
                    response = await supervisor.execute(checked["request"], operation_id)
                    observed = await supervisor.run_status(supervisor.row(run_id, group_id))
                    receipt = await supervisor.receipt(operation_id)
                    controls.append({"action": action, "operationId": operation_id, "result": response,
                        "observedStatus": observed["status"], "receipt": receipt})
                    emit("native_shadow_control", action=action, runId=run_id, observedStatus=observed["status"],
                        receiptStatus=receipt.get("receiptStatus"))
                    if observed["status"] != expected:
                        raise RuntimeError(f"Shadow control {action} produced {observed['status']} instead of {expected}")
            if not sent_stop and first_prediction is not None and elapsed - first_prediction >= 120:
                check = await supervisor.prepare({"groupId": group_id, "runId": run_id, "action": "stop"})
                stopped = await supervisor.execute(check["request"], "preopen-stop-" + uuid.uuid4().hex)
                save(STATE / "stop.json", stopped)
                emit("native_shadow_stop_requested", runId=run_id, receipt=stopped)
                sent_stop = True
            await asyncio.sleep(3)
    finally:
        status = await supervisor.run_status(supervisor.row(run_id, group_id))
        if supervisor.owned_process(supervisor.row(run_id, group_id)) is not None:
            check = await supervisor.prepare({"groupId": group_id, "runId": run_id, "action": "stop"})
            await supervisor.execute(check["request"], "preopen-cleanup-" + uuid.uuid4().hex)
        if supervisor.tasks:
            await asyncio.wait_for(asyncio.gather(*tuple(supervisor.tasks), return_exceptions=True), 35)
        status = await supervisor.run_status(supervisor.row(run_id, group_id))
        final = json.loads((directory / "final.json").read_text()) if (directory / "final.json").exists() else None
        view = json.loads((directory / "view.json").read_text()) if (directory / "view.json").exists() else None
        result = {"runId": run_id, "state": status["status"], "firstReadySeconds": first_ready,
            "firstPredictionSeconds": first_prediction, "wallSeconds": time.monotonic() - started,
            "marketReadySamples": sum(s["engine"].get("marketReady") is True for s in samples),
            "maxNativeDelivered": max((s["engine"].get("nativeDataIngress", {}).get("stats", {}).get("delivered", 0) for s in samples), default=0),
            "maxProcessTreeRssBytes": max((s["processTreeRssBytes"] or 0 for s in samples), default=0),
            "samples": samples, "controls": controls, "final": final, "view": view, "freshProbe": fresh, "cpuBenchmark": benchmark}
        save(STATE / ("result-" + run_id + ".json"), result)
        emit("native_shadow_finished", **{k: v for k, v in result.items() if k not in {"samples", "controls", "final", "view", "freshProbe", "cpuBenchmark"}},
            finalError=final.get("error") if final else "missing final", ordersTotal=view.get("ordersTotal") if view else None,
            fillsTotal=view.get("fillsTotal") if view else None, evidence=str(STATE / ("result-" + run_id + ".json")))
    if (not final or final.get("error") or not view or view["ordersTotal"] != 0 or view["fillsTotal"] != 0
            or first_prediction is None or result["marketReadySamples"] < 2 or result["maxNativeDelivered"] <= 0):
        raise RuntimeError("Native model shadow validation did not complete successfully; retained evidence must be inspected")


async def stop_validation_run():
    launched = json.loads((STATE / "launch.json").read_text())
    supervisor = GroupSupervisor(LaunchRegistry(BASE / "registry.json"), STATE / "runs")
    check = await supervisor.prepare({"groupId": launched["groupId"], "runId": launched["runId"], "action": "stop"})
    result = await supervisor.execute(check["request"], "preopen-explicit-stop-" + uuid.uuid4().hex)
    emit("isolated_explicit_stop", **result)


async def warmup_stop_validation():
    registry = LaunchRegistry(BASE / "registry.json")
    supervisor = GroupSupervisor(registry, STATE / "runs")
    publication = json.loads((STATE / "publication.json").read_text())
    group_id = "model-" + publication["preview"]["manifestSha256"][:16]
    prepared = await supervisor.prepare({"groupId": group_id, "action": "start", "budgetUsdt": "10000.00", "durationSeconds": 180})
    launched = await supervisor.execute(prepared["request"], "warmup-stop-start-" + uuid.uuid4().hex)
    run_id = launched["runId"]
    directory = STATE / "runs" / run_id
    before = time.monotonic()
    while not (directory / "manifest.json").exists():
        if time.monotonic() - before > 15:
            raise RuntimeError("Worker did not reach warmup within 15 seconds")
        await asyncio.sleep(.01)
    if (directory / "model-ready.json").exists():
        raise RuntimeError("Warmup stop test missed the pre-ready interval")
    check = await supervisor.prepare({"groupId": group_id, "runId": run_id, "action": "stop"})
    stop_started = time.monotonic()
    receipt = await supervisor.execute(check["request"], "warmup-stop-request-" + uuid.uuid4().hex)
    await asyncio.wait_for(asyncio.gather(*tuple(supervisor.tasks), return_exceptions=True), 15)
    state = await supervisor.run_status(supervisor.row(run_id, group_id))
    final = json.loads((directory / "final.json").read_text())
    result = {"runId": run_id, "stopLatencySeconds": time.monotonic() - stop_started, "state": state["status"],
        "receipt": receipt, "final": final}
    save(STATE / "warmup-stop.json", result)
    emit("warmup_stop_result", runId=run_id, stopLatencySeconds=result["stopLatencySeconds"], state=state["status"],
        stopReason=final.get("stop_reason"), nodeStarted=final.get("node_started"), ordersEnabled=final.get("orders_enabled"), error=final.get("error"))
    if final.get("stop_reason") != "stopped_before_model_ready" or final.get("node_started") is not False or final.get("error"):
        raise RuntimeError("Warmup cancellation did not produce the required never-started final evidence")


async def performance_validation():
    publication = json.loads((STATE / "publication.json").read_text())["preview"]
    manifest = STATE / "model-store/releases" / publication["manifestSha256"] / "manifest.json"
    result = await resident_benchmark(manifest, publication["manifestSha256"], json.loads((STATE / "fresh-window.json").read_text()))
    save(STATE / "cpu-benchmark-late.json", result)
    emit("cpu_benchmark_with_deadline_probe", **{k: v for k, v in result.items() if k != "ready"})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["setup", "run", "stop", "warmup-stop", "performance"])
    args = p.parse_args()
    if args.action == "setup":
        setup()
    elif args.action == "run":
        asyncio.run(run_validation())
    elif args.action == "stop":
        asyncio.run(stop_validation_run())
    elif args.action == "warmup-stop":
        asyncio.run(warmup_stop_validation())
    else:
        asyncio.run(performance_validation())


if __name__ == "__main__":
    main()
