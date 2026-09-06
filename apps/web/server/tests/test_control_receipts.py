import asyncio
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

source = Path(__file__).resolve().parents[2] / "engine" / "control.py"
spec = importlib.util.spec_from_file_location("durable_control_under_test", source)
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)


class DurableControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="control-receipts-", dir="/private/tmp")
        self.path = Path(self.temporary.name) / "control.sock"
        self.calls = 0
        self.state = "ACTIVE"

        async def halt(request):
            self.calls += 1
            self.state = "HALTED"
            await asyncio.sleep(0.08)
            return {"tradingState": self.state}

        self.handlers = {"halt": halt, "status": lambda _: {"tradingState": self.state}}
        self.server = control.ControlServer(self.path, self.handlers)
        await self.server.start()

    async def asyncTearDown(self):
        await self.server.stop()
        self.temporary.cleanup()

    async def test_timeout_after_effect_can_be_queried_without_resubmission(self):
        with self.assertRaises(TimeoutError):
            await control.call(self.path, "halt", timeout=0.02, operationId="operation_timeout_001")
        self.assertEqual(self.state, "HALTED")
        pending = await control.call(self.path, "receipt", operationId="operation_timeout_001")
        self.assertEqual(pending["receiptStatus"], "processing")
        await asyncio.sleep(0.1)
        result = await control.call(self.path, "receipt", operationId="operation_timeout_001")
        self.assertEqual(result["receiptStatus"], "completed")
        self.assertEqual(result["tradingState"], "HALTED")
        self.assertEqual(self.calls, 1)

    async def test_closed_control_journal_supports_readonly_recovery(self):
        journal = self.path.parent / "control-operations.sqlite"
        await self.server.stop()
        self.path.parent.chmod(0o500)
        try:
            with sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True) as db:
                self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        finally:
            self.path.parent.chmod(0o700)
        self.server = control.ControlServer(self.path, self.handlers)
        await self.server.start()

    async def test_concurrent_duplicate_executes_once_and_conflict_rejected(self):
        replies = await asyncio.gather(*(control.call(self.path, "halt", operationId="operation_duplicate_001") for _ in range(8)))
        self.assertTrue(all(reply["receiptStatus"] == "completed" for reply in replies))
        self.assertEqual(self.calls, 1)
        conflict = await control.call(self.path, "halt", operationId="operation_duplicate_001", reason="different")
        self.assertFalse(conflict["ok"])
        self.assertEqual(self.calls, 1)

    async def test_restart_recovers_completed_and_does_not_replay_uncertain_intent(self):
        await control.call(self.path, "halt", operationId="operation_recovered_001")
        with self.server.db:
            self.server.db.execute("INSERT INTO operations VALUES (?,?,?,NULL,1,1)",
                ("operation_interrupted_001", control.encode({"command": "halt", "operationId": "operation_interrupted_001"}), "processing"))
        await self.server.stop()
        self.server = control.ControlServer(self.path, self.handlers)
        await self.server.start()
        finished = await control.call(self.path, "receipt", operationId="operation_recovered_001")
        self.assertEqual(finished["receiptStatus"], "completed")
        pending = await control.call(self.path, "halt", operationId="operation_interrupted_001")
        self.assertEqual(pending["receiptStatus"], "unknown")
        self.assertEqual(self.calls, 1)

    async def test_partial_handler_exception_is_unknown_not_failed(self):
        def partial(_request):
            self.calls += 1
            self.state = "HALTED"
            raise RuntimeError("notifier disconnected after state changed")
        self.server.handlers["halt"] = partial
        result = await control.call(self.path, "halt", operationId="operation_partial_001")
        self.assertEqual(result["receiptStatus"], "unknown")
        self.assertFalse(result["ok"])
        repeated = await control.call(self.path, "halt", operationId="operation_partial_001")
        self.assertEqual(repeated, result)
        self.assertEqual(self.calls, 1)

    async def test_quiesce_finishes_existing_action_and_rejects_closed_action(self):
        pending = asyncio.create_task(control.call(self.path, "halt", operationId="operation_shutdown_001"))
        await asyncio.sleep(0.02)
        await self.server.quiesce()
        self.assertEqual((await pending)["receiptStatus"], "completed")
        rejected = await control.call(self.path, "halt", operationId="operation_closed_001")
        self.assertEqual(rejected["receiptStatus"], "failed")
        receipt = await control.call(self.path, "receipt", operationId="operation_closed_001")
        self.assertEqual(receipt, rejected)
        self.assertEqual(self.calls, 1)


if __name__ == "__main__":
    unittest.main()
