import asyncio
import hashlib
import json
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock
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

    async def test_continuous_run_requires_published_worker_protocol(self):
        spec = self.registry.groups['live-hft-inventory']
        with self.assertRaisesRegex(ValueError, '运行时长'):
            await self.supervisor.prepare({'groupId': spec['id'], 'action': 'start', 'durationSeconds': 0})
        spec.update(durationPolicyVersion=1, maxDurationSeconds=None, executionPolicy='inventory_quotes')
        for duration in (0, 1, 7 * 86400):
            prepared = await self.supervisor.prepare({'groupId': spec['id'], 'action': 'start', 'durationSeconds': duration})
            self.assertEqual(prepared['request']['durationSeconds'], duration)
            self.assertTrue(prepared['durationPolicy']['continuous'])
        status = (await self.supervisor.status(spec['id']))['groups'][0]
        self.assertEqual(status['executionPolicy']['label'], '连续报价')
        self.assertIsNone(status['maxDurationSeconds'])
        for invalid in (-1, True, 0.5, '0', None, 9007199254740992):
            with self.assertRaisesRegex(ValueError, '运行时长'):
                await self.supervisor.prepare({'groupId': spec['id'], 'action': 'start', 'durationSeconds': invalid})

    def test_registry_accepts_explicit_continuous_or_long_duration(self):
        spec = self.document['groups'][0]
        spec.update(durationPolicyVersion=1, maxDurationSeconds=None)
        self.registry_path.write_text(json.dumps(self.document))
        self.assertIsNone(LaunchRegistry(self.registry_path, strict=False).spec(spec['id'])['maxDurationSeconds'])
        spec['maxDurationSeconds'] = 7 * 86400
        self.registry_path.write_text(json.dumps(self.document))
        self.assertEqual(LaunchRegistry(self.registry_path, strict=False).spec(spec['id'])['maxDurationSeconds'], 7 * 86400)

    async def test_explicit_time_limit_is_preserved(self):
        spec = self.registry.groups['live-hft-inventory']
        spec['durationPolicyVersion'] = 1
        for duration in (0, 3601):
            with self.assertRaisesRegex(ValueError, '运行时长'):
                await self.supervisor.prepare({'groupId': spec['id'], 'action': 'start', 'durationSeconds': duration})
        group = (await self.supervisor.status(spec['id']))['groups'][0]
        self.assertFalse(group['durationPolicy']['continuous'])

    def test_program_or_config_change_invalidates_launch(self):
        self.worker.write_text("changed")
        with self.assertRaisesRegex(ValueError, "已变化"):
            self.registry.validate_inputs("live-hft-inventory")

    async def test_native_controls_bind_run_and_normalize_flatten_scope(self):
        spec=self.registry.groups['live-hft-inventory'];spec['controlVersion']=1
        row={'id':'live-hft-inventory-run','group_id':spec['id']}
        self.supervisor.row=Mock(return_value=row)
        self.supervisor.owned_process=Mock(return_value=object())
        self.supervisor.run_status=AsyncMock(return_value={'status':'running','engine':{
            'controlVersion':1,'inventory':{'pairs':[{'instrument':'BTC-USDT'}]}}})
        prepared=await self.supervisor.prepare({'groupId':spec['id'],'runId':row['id'],'action':'flatten','instruments':['BTC-USDT','BTC-USDT']})
        self.assertEqual(prepared['request']['instruments'],['BTC-USDT'])
        self.assertIn('10 bps',prepared['effect'])
        with self.assertRaisesRegex(ValueError,'品种'):
            await self.supervisor.prepare({'groupId':spec['id'],'runId':row['id'],'action':'flatten','instruments':['ETH-USDT']})
        self.supervisor.run_status.return_value={'status':'halted','engine':{'controlVersion':1,'resumeAllowed':False}}
        with self.assertRaisesRegex(ValueError,'库存已变化'):
            await self.supervisor.prepare({'groupId':spec['id'],'runId':row['id'],'action':'resume'})

    def test_legacy_signal_metadata_does_not_require_a_signal_file(self):
        self.document['groups'][0]['signalsSha256']='legacy-inventory-source'
        self.registry_path.write_text(json.dumps(self.document))
        registry=LaunchRegistry(self.registry_path,strict=False)
        self.assertEqual(registry.spec('live-hft-inventory')['signalsSha256'],'legacy-inventory-source')

    def test_file_backed_alpha_requires_matching_signal_content(self):
        signal=self.root/'signals.json';signal.write_text('{}')
        self.document['groups'][0].update(signalsPath=str(signal),signalsSha256='bad')
        self.registry_path.write_text(json.dumps(self.document))
        with self.assertRaisesRegex(ValueError,'信号版本'):
            LaunchRegistry(self.registry_path,strict=False)


if __name__ == "__main__":
    unittest.main()
