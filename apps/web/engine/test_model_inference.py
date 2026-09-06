"""Focused runtime boundaries plus optional real local GRU artifact reload."""
import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
import sys

from model_inference_worker import InferenceService
from model_release import ModelAdapter, package_gru, sha
from model_strategy import checked_fraction

ROOT = Path(__file__).resolve().parents[3] / "model_training"


class FakeAdapter:
    manifest = {"releaseId": "test", "model": {"sha256": "abc"}, "runtime": {
        "maxQueueSize": 2, "maxBatchSize": 2, "maxInputAgeMs": 1000, "timeoutMs": 500}}
    def validate(self, request):
        if request.get("invalid"):
            raise ValueError("Invalid fixture")
    def contract(self, request):
        return "one"
    def predict_batch(self, requests):
        time.sleep(.01)
        return [{"prediction": .01} for _ in requests]


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_during_warmup_cancels_start_without_late_node_start(self):
        from model_group_worker import start_model_until_stop
        class WaitingClient:
            def __init__(self):
                self.started = asyncio.Event()
                self.cancelled = False
                self.closed = False
            async def start(self):
                self.started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise
            async def close(self):
                self.closed = True
        client, stop = WaitingClient(), asyncio.Event()
        task = asyncio.create_task(start_model_until_stop(client, stop))
        await client.started.wait()
        stop.set()
        self.assertIsNone(await asyncio.wait_for(task, 1))
        self.assertTrue(client.cancelled)
        self.assertTrue(client.closed)

    def request(self, **extra):
        now = time.time_ns()
        return {"requestId": "r", "asOfNs": now - 10_000_000, "deadlineNs": now + 400_000_000,
            "releaseId": "test", "modelHash": "abc", "instrument": "BTC-USDT", **extra}

    async def test_queue_bounded_and_expiry(self):
        outputs = []
        service = InferenceService(FakeAdapter(), outputs.append)
        service.submit(self.request())
        service.submit(self.request())
        service.submit(self.request())
        self.assertEqual(outputs[-1]["error"], "queue_full")
        service.submit(self.request(asOfNs=0))
        self.assertEqual(outputs[-1]["error"], "expired")
        service.submit(self.request(invalid=True))
        self.assertEqual(outputs[-1]["error"], "invalid_request")

    @unittest.skipUnless((ROOT / "models/trained/btc_gru_recent_20260905/latest/model.pt").exists(), "local weights unavailable")
    async def test_real_persistent_process_multiplexing(self):
        from model_client import InferenceProcess
        with tempfile.TemporaryDirectory() as temp:
            info = package_gru(ROOT / "models/trained/btc_gru_recent_20260905/latest", ROOT / "pipelines/recent_btc",
                Path(temp) / "release", "test-process")
            worker = InferenceProcess([sys.executable, str(Path(__file__).with_name("model_inference_worker.py")),
                "--manifest", info["manifest"], "--manifest-sha256", info["sha256"], "--runner-root", str(ROOT / "pipelines")])
            await worker.start()
            try:
                ready = await asyncio.wait_for(worker.read(), 10)
                self.assertEqual(ready["type"], "ready")
                worker.start_dispatch()
                manifest = json.loads(Path(info["manifest"]).read_text())
                now = time.time_ns()
                # Zero standardized inputs and current test-clock timestamps
                # are explicitly synthetic protocol fixtures, not live bars.
                request = dict(releaseId="test-process", modelHash=info["modelHash"], mode="shadow", instrument="BTC-USDT",
                    normalized=True, featureNames=manifest["featureContract"]["names"], inputs=[[0.] * 40 for _ in range(32)],
                    asOfNs=now - 1_000_000, deadlineNs=now + 1_900_000_000)
                outputs = await asyncio.wait_for(asyncio.gather(*[
                    worker.exchange({**request, "requestId": f"r{i}"}) for i in range(8)]), 3)
                self.assertEqual([o["requestId"] for o in outputs], [f"r{i}" for i in range(8)])
                self.assertTrue(all(o["modelHash"] == info["modelHash"] for o in outputs))
                with self.assertRaises(RuntimeError):
                    await worker.exchange({**request, "requestId": "stale", "asOfNs": 0})
                good = await worker.exchange({**request, "requestId": "after-error"})
                self.assertIn("prediction", good)
            finally:
                await worker.close()

    @unittest.skipUnless((Path(__file__).resolve().parents[2] / "target/release/nautilus-model-guard").exists(), "compiled Rust guard unavailable")
    async def test_real_rust_guard_to_persistent_worker(self):
        import numpy as np
        from model_client import ModelGuardClient
        from model_probe import gru_feature_window
        guard = Path(__file__).resolve().parents[2] / "target/release/nautilus-model-guard"
        with tempfile.TemporaryDirectory() as temp:
            info = package_gru(ROOT / "models/trained/btc_gru_recent_20260905/latest", ROOT / "pipelines/recent_btc",
                Path(temp) / "release", "test-real-guard")
            adapter = ModelAdapter(info["manifest"], info["sha256"], ROOT / "pipelines")
            window = gru_feature_window(adapter.manifest, ROOT / "data/recent_20260905", ROOT / "pipelines")
            c = adapter.manifest["featureContract"]
            expected = adapter.predict_batch([{**window, "releaseId": "test-real-guard", "modelHash": info["modelHash"],
                "mode": "shadow", "normalized": True,
                "inputs": np.clip((np.asarray(window["inputs"]) - c["mean"]) / c["scale"], *c["clip"]).astype("float32")}])[0]["prediction"]
            client = ModelGuardClient(info["manifest"], info["sha256"], guard, ROOT / "pipelines")
            try:
                await client.start()
                now = time.time_ns()
                # Historical values with explicit synthetic protocol timestamps;
                # live-market freshness is checked separately by model_live_probe.
                request = {**window, "requestId": "guard-test", "releaseId": "test-real-guard", "modelHash": info["modelHash"],
                    "mode": "shadow", "normalized": False, "asOfNs": now - 1_000_000, "deadlineNs": now + 1_900_000_000}
                result = await client.predict(request)
                self.assertEqual(result["prediction"]["prediction"], expected)
                self.assertEqual(result["checked"]["targetFraction"], 0)
                self.assertFalse(result["checked"]["ordersCreated"])
                with self.assertRaises(RuntimeError):
                    await client.predict({**request, "requestId": "guard-bad-mode", "mode": "live"})
                await client.worker.close()
                self.assertFalse(client.is_ready)
                self.assertIn("process exited", client.failure_reason())
            finally:
                await client.close()
    async def test_batched_prediction_correlation_and_metrics(self):
        outputs = []
        service = InferenceService(FakeAdapter(), outputs.append)
        service.submit(self.request(requestId="one"))
        service.submit(self.request(requestId="two"))
        task = asyncio.create_task(service.run())
        await asyncio.wait_for(service.queue.join(), 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual([x["requestId"] for x in outputs], ["one", "two"])
        self.assertEqual(service.metrics()["completed"], 2)
        self.assertTrue(all(x["modelHash"] == "abc" and x["latencyMs"] > 0 for x in outputs))

    async def test_output_after_deadline_discarded(self):
        outputs = []
        service = InferenceService(FakeAdapter(), outputs.append)
        service.submit(self.request(deadlineNs=time.time_ns() + 1_000_000))
        task = asyncio.create_task(service.run())
        await asyncio.wait_for(service.queue.join(), 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(outputs[-1]["error"], "expired")


class ModelTests(unittest.TestCase):
    def test_market_ready_requires_real_quote_ingress_and_fresh_prediction(self):
        from model_group_worker import model_market_ready
        values = {"model_ready": True, "failure": None, "connected": {"data": True, "exec": True},
            "stats": {"delivered": 1, "closed": False}, "quote_age": .1, "window_ready": True,
            "target": {"asOfNs": 1_000_000_000}, "now_ns": 2_000_000_000, "max_age_ms": 2000}
        self.assertTrue(model_market_ready(**values))
        for change in ({"quote_age": None}, {"quote_age": 6}, {"quote_age": -1},
                       {"stats": {"delivered": 0, "closed": False}}, {"stats": {"delivered": 2, "closed": True}},
                       {"connected": {"data": False, "exec": True}}, {"target": None},
                       {"window_ready": False}, {"model_ready": False}, {"failure": "overflow"},
                       {"now_ns": 4_000_000_001}):
            self.assertFalse(model_market_ready(**{**values, **change}))

    def test_candles_only_confirmed_true_quote_volume_and_no_gaps(self):
        from model_market_actor import parse_completed_candles
        end = (time.time_ns() // 900_000_000_000 - 1) * 900000
        rows = [[str(end - i * 900000), "100", "105", "95", "101", "2", "3", "901.5", "1"] for i in range(128)]
        frame = parse_completed_candles({"code": "0", "data": rows}, time.time_ns())
        self.assertEqual(len(frame), 128)
        self.assertEqual(float(frame.volume_quote.iloc[-1]), 901.5)
        for bad in (rows[:-1], rows + [rows[-1]], [{**{}}], [[*rows[0][:7], "NaN", "1"]] + rows[1:]):
            with self.assertRaises((ValueError, TypeError)):
                parse_completed_candles({"code": "0", "data": bad}, time.time_ns())

    def test_invalid_targets_rejected(self):
        for value in (None, True, -1, 1.01, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                checked_fraction({"targetFraction": value})
        self.assertEqual(checked_fraction({"targetFraction": .5}), .5)

    @unittest.skipUnless((ROOT / "models/trained/btc_gru_recent_20260905/latest/model.pt").exists(), "local weights unavailable")
    def test_real_gru_release_reload_predictions_and_identity(self):
        import numpy as np
        from model_probe import gru_feature_window
        with tempfile.TemporaryDirectory() as temp:
            info = package_gru(ROOT / "models/trained/btc_gru_recent_20260905/latest", ROOT / "pipelines/recent_btc",
                Path(temp) / "release", "test-gru-latest")
            adapter = ModelAdapter(info["manifest"], info["sha256"], ROOT / "pipelines")
            adapter.warmup()
            raw = gru_feature_window(adapter.manifest, ROOT / "data/recent_20260905", ROOT / "pipelines")
            c = adapter.manifest["featureContract"]
            request = {**raw, "releaseId": "test-gru-latest", "modelHash": info["modelHash"], "mode": "shadow",
                "normalized": True, "inputs": np.clip((np.asarray(raw["inputs"]) - c["mean"]) / c["scale"], -8, 8).astype("float32")}
            value = adapter.predict_batch([request])[0]["prediction"]
            self.assertTrue(np.isfinite(value))
            self.assertNotEqual(value, 0)
            reload = ModelAdapter(info["manifest"], info["sha256"], ROOT / "pipelines")
            self.assertEqual(value, reload.predict_batch([request])[0]["prediction"])
            for changed in ({"modelHash": "wrong"}, {"mode": "live"}, {"featureNames": list(reversed(c["names"]))},
                            {"inputs": np.zeros((31, 40))}, {"inputs": np.full((32, 40), float("nan"))}):
                with self.assertRaises(ValueError):
                    adapter.predict_batch([{**request, **changed}])
            # Runtime never imports the packaged provenance source.
            (Path(info["manifest"]).parent / "source/train_gru.py").write_text("raise RuntimeError('upload code')")
            with self.assertRaises(ValueError):
                ModelAdapter(info["manifest"], info["sha256"], ROOT / "pipelines")


if __name__ == "__main__":
    unittest.main()
