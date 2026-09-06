import asyncio
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

from group_runtime import GroupSupervisor, LaunchRegistry


PREVIEW = {
    "schemaVersion": 1,
    "profileId": "tokyoreal",
    "account": {"uid": "sub", "mainUid": "main", "perm": "read_only,trade", "ip": "127.0.0.1"},
    "totalEqUsd": "417.5",
    "pairs": [{"instrument": "BTC-USDT", "baseAvailable": "0.001", "quoteAvailable": "86"}],
    "openManagedPairOrders": 0,
    "inventoryHash": "a" * 64,
}


class LiveHFTRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="live-hft-runtime-", dir="/private/tmp")
        self.root = Path(self.temporary.name)
        self.worker = self.root / "live-worker.py"
        self.worker.write_text("import json,sys\nprint(json.dumps(" + repr(PREVIEW) + "))\n")
        self.config = self.root / "config.json"
        self.config.write_text("{}")
        self.state = self.root / "profiles"
        self.state.mkdir()
        self.registry_path = self.root / "registry.json"
        self.document = {"version": 1, "pythonPath": sys.executable, "workerPath": sys.executable,
            "groups": [{"id": "live-hft-inventory", "kind": "live", "name": "实盘高频",
                "enabled": True, "environment": "live", "workerPath": str(self.worker),
                "workerSha256": hashlib.sha256(self.worker.read_bytes()).hexdigest(), "pythonPath": sys.executable,
                "configPath": str(self.config), "configSha256": hashlib.sha256(self.config.read_bytes()).hexdigest(),
                "profileStatePath": str(self.state), "profileId": "tokyoreal", "operatorServerPath": str(self.root),
                "maxDurationSeconds": 3600}]}
        self.registry_path.write_text(json.dumps(self.document))
        self.registry = LaunchRegistry(self.registry_path, strict=False)
        self.supervisor = GroupSupervisor(self.registry, self.root / "runs")

    async def asyncTearDown(self):
        self.supervisor.db.close()
        self.temporary.cleanup()

    async def test_preview_freezes_inventory_and_uses_account_capital(self):
        prepared = await self.supervisor.prepare({"groupId": "live-hft-inventory", "action": "start",
            "budgetUsdt": 999999, "durationSeconds": 600})
        self.assertEqual(prepared["mode"], "live")
        self.assertTrue(prepared["ordersEnabled"])
        self.assertEqual(prepared["liveInventory"]["account"]["uid"], "sub")
        self.assertEqual(prepared["request"]["inventoryHash"], "a" * 64)
        self.assertNotIn("budgetUsdt", prepared["request"])
        group = (await self.supervisor.status("live-hft-inventory"))["groups"][0]
        self.assertEqual(group["kind"], "live")
        self.assertEqual(group["capitalMode"], "account_inventory")
        self.assertEqual(group["supportedActions"], ["start"])

    async def test_changed_inventory_and_non_live_controls_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "余额或委托"):
            await self.supervisor.prepare({"groupId": "live-hft-inventory", "action": "start",
                "durationSeconds": 600, "inventoryHash": "b" * 64})
        for action in ("halt", "reduce", "resume"):
            with self.assertRaisesRegex(ValueError, "启动和停止"):
                await self.supervisor.prepare({"groupId": "live-hft-inventory", "action": action,
                    "runId": "live-hft-inventory-run"})

    def test_program_or_config_change_invalidates_launch(self):
        self.worker.write_text("changed")
        with self.assertRaisesRegex(ValueError, "已变化"):
            self.registry.validate_inputs("live-hft-inventory")


if __name__ == "__main__":
    unittest.main()
