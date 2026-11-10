"""Focused Python glue contracts, independently of an installed native wheel.

Rust tests exercise the real PyO3 bridge. This harness loads the actual modified
Python methods with small kernel fixtures; it does not claim a live-node smoke.
"""
import ast
import asyncio
import unittest
import weakref
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock


ROOT = Path(__file__).resolve().parents[3]


def load_methods(relative, class_name, names, namespace):
    tree = ast.parse((ROOT / relative).read_text())
    original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    selected = [node for node in original.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert len(selected) == len(names)
    cls = ast.ClassDef(name=class_name, bases=[], keywords=[], body=selected, decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
    exec(compile(module, str(ROOT / relative), "exec"), namespace)
    return namespace[class_name]


class BridgeFixture:
    def __init__(self, loop, owner_id, on_failure, capacity, max_batch, *, owner_context=None):
        self.owner_id, self.on_failure = owner_id, on_failure
        self.failure, self.closed = None, False
        self.owner_context = owner_context

    def fail(self, reason):
        if self.failure is None and not self.closed:
            self.failure = reason
            self.on_failure(reason)

    def close(self):
        self.closed = True
        self.owner_context = None

    def validate_context(self, context):
        if self.closed or self.owner_context is not context:
            raise ValueError("different kernel cache")


class NativeIngressGlueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        environment = SimpleNamespace(SANDBOX="SANDBOX")
        engine_type = load_methods("nautilus_trader/live/data_engine.py", "LiveDataEngine",
            {"process", "bind_python_data_ingress", "_handle_queue_exception", "_run_data_queue"},
            {"asyncio": asyncio, "Data": object})
        self.engine = engine_type()
        self.engine._python_data_ingress = None
        self.engine._data_queue = asyncio.Queue(maxsize=1)
        self.engine._data_enqueuer = Mock()
        self.engine._cache = object()
        self.engine._log = Mock()
        self.engine.data_qsize = lambda: self.engine._data_queue.qsize()
        self.engine._sentinel = object()
        node_type = load_methods("nautilus_trader/live/node.py", "TradingNode",
            {"enable_python_data_ingress", "native_data_ingress_failure", "_on_native_data_ingress_failure",
             "_await_with_native_data_ingress", "stop_async"},
            {"asyncio": asyncio, "weakref": weakref, "Environment": environment,
             "nautilus_pyo3": SimpleNamespace(PythonDataIngress=BridgeFixture)})
        self.node = node_type()
        self.node._is_built = False
        self.node._python_data_ingress = None
        self.node._native_data_ingress_error = None
        self.node._native_data_failure_event = asyncio.Event()
        self.node.instance_id = "node-owned-instance"
        self.node._config = SimpleNamespace(timeout_shutdown=1)
        self.node.kernel = SimpleNamespace(environment="SANDBOX", loop=asyncio.get_running_loop(),
            is_running=lambda: False, data_engine=self.engine, cache=self.engine._cache,
            risk_engine=Mock(), stop_async=AsyncMock())

    async def test_queue_full_is_terminal_without_unbounded_put_fallback(self):
        bridge = self.node.enable_python_data_ingress()
        self.engine.process("one")
        with self.assertRaisesRegex(RuntimeError, "queue full"):
            self.engine.process("two")
        self.assertEqual(self.engine._data_queue.qsize(), 1)
        self.engine._data_enqueuer.enqueue.assert_not_called()
        self.assertEqual(bridge.failure, "python_data_engine_queue_full")
        self.node.kernel.risk_engine.force_halt_for_terminal_failure.assert_called_once()
        self.assertTrue(self.node._native_data_failure_event.is_set())

    async def test_fault_notifies_even_when_risk_halt_raises(self):
        bridge = self.node.enable_python_data_ingress()
        self.node.kernel.risk_engine.force_halt_for_terminal_failure.side_effect = RuntimeError("broken risk")
        with self.assertRaisesRegex(RuntimeError, "broken risk"):
            bridge.fail("terminal")
        self.assertEqual(self.node.native_data_ingress_failure, "terminal")
        self.assertTrue(self.node._native_data_failure_event.is_set())

    async def test_running_consumer_is_stopped_before_awaitable_is_drained(self):
        bridge = self.node.enable_python_data_ingress()
        stopped = asyncio.Event()
        async def stop_kernel():
            stopped.set()
        self.node.kernel.stop_async.side_effect = stop_kernel
        operation = asyncio.create_task(self.node._await_with_native_data_ingress(stopped.wait()))
        await asyncio.sleep(0)
        bridge.fail("adapter disconnected")
        with self.assertRaisesRegex(RuntimeError, "adapter disconnected"):
            await asyncio.wait_for(operation, 1)
        self.assertTrue(bridge.closed)
        self.node.kernel.stop_async.assert_awaited_once()

    async def test_startup_is_cancelled_before_kernel_shutdown_on_fault(self):
        bridge = self.node.enable_python_data_ingress()
        cancelled = asyncio.Event()
        async def connecting():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        async def stop_kernel():
            self.assertTrue(cancelled.is_set())
        self.node.kernel.stop_async.side_effect = stop_kernel
        operation = asyncio.create_task(self.node._await_with_native_data_ingress(connecting(), cancel_on_failure=True))
        await asyncio.sleep(0)
        bridge.fail("startup input lost")
        with self.assertRaisesRegex(RuntimeError, "startup input lost"):
            await asyncio.wait_for(operation, 1)

    async def test_normal_stop_closes_without_inventing_a_failure(self):
        bridge = self.node.enable_python_data_ingress()
        await self.node.stop_async()
        self.assertTrue(bridge.closed)
        self.assertIsNone(self.node.native_data_ingress_failure)
        self.node.kernel.risk_engine.force_halt_for_terminal_failure.assert_not_called()

    async def test_only_one_sandbox_prebuild_owner_is_allowed(self):
        self.node.kernel.environment = "LIVE"
        with self.assertRaisesRegex(ValueError, "sandbox"):
            self.node.enable_python_data_ingress()
        self.node.kernel.environment = "SANDBOX"
        bridge = self.node.enable_python_data_ingress()
        with self.assertRaises(RuntimeError):
            self.node.enable_python_data_ingress()
        with self.assertRaises(ValueError):
            fresh_engine = type(self.engine)()
            fresh_engine._python_data_ingress = None
            fresh_engine.bind_python_data_ingress(bridge, "foreign-node")

    async def test_legacy_queue_path_remains_unchanged_without_bridge(self):
        self.engine.process("legacy")
        self.engine._data_enqueuer.enqueue.assert_called_once_with("legacy")
        self.assertEqual(self.engine._data_queue.qsize(), 0)
        self.assertEqual(await self.node._await_with_native_data_ingress(asyncio.sleep(0, result=7)), 7)

    async def test_foreign_client_on_same_loop_cannot_bind_another_nodes_cache(self):
        bridge = self.node.enable_python_data_ingress()
        client_type = load_methods("nautilus_trader/adapters/okx/data.py", "OKXDataClient",
            {"set_python_data_ingress"}, {"nautilus_pyo3": SimpleNamespace(PythonDataIngress=BridgeFixture)})
        client = client_type()
        client._cache = object()  # Same host loop, different real kernel cache.
        client._python_data_ingress = None
        with self.assertRaisesRegex(ValueError, "kernel cache"):
            client.set_python_data_ingress(bridge)
        self.assertIsNone(client._python_data_ingress)
        client._cache = self.node.kernel.cache
        client.set_python_data_ingress(bridge)
        self.assertIs(client._python_data_ingress, bridge)

    async def test_final_consumer_fault_stops_before_processing_queued_followups(self):
        bridge = self.node.enable_python_data_ingress()
        loop = asyncio.get_running_loop()
        def deferred_fail(reason):
            bridge.failure = reason
            loop.call_soon(bridge.on_failure, reason)
        bridge.fail = deferred_fail
        self.engine._data_queue = asyncio.Queue(maxsize=8)
        self.engine._data_queue.put_nowait("bad")
        self.engine._data_queue.put_nowait("must-not-dispatch")
        self.engine._handle_data = Mock(side_effect=ValueError("bad decoded data"))
        self.engine.shutdown_system = Mock()
        await self.engine._run_data_queue()
        self.engine._handle_data.assert_called_once_with("bad")
        self.assertEqual(self.engine._data_queue.qsize(), 1)
        self.assertFalse(self.node._native_data_failure_event.is_set())
        await asyncio.sleep(0)
        self.assertTrue(self.node._native_data_failure_event.is_set())
        self.assertIn("consumer_failed", self.node.native_data_ingress_failure)
        self.engine.shutdown_system.assert_not_called()


if __name__ == "__main__":
    unittest.main()
