import asyncio
import unittest
from unittest.mock import AsyncMock
from pathlib import Path
from types import SimpleNamespace

import test_boundary as boundary_fixture


class GroupAPITests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = boundary_fixture.BoundaryTests.asyncTearDown
    ticket = boundary_fixture.BoundaryTests.ticket

    async def asyncSetUp(self):
        await boundary_fixture.BoundaryTests.asyncSetUp(self)
        self.normalized = {"groupId": "baseline", "action": "start", "registryVersion": "v1",
                           "budgetUsdt": "2000.00", "durationSeconds": 60}
        self.preview = {"request": self.normalized, "mode": "nautilus_sandbox", "groupName": "四板块"}
        self.operator.group_runtime.prepare = AsyncMock(return_value=self.preview)
        self.operator.group_runtime.execute = AsyncMock(return_value={"runId": "baseline-123", "status": "starting"})
        self.operator.group_runtime.status = AsyncMock(return_value={"available": True, "groups": []})
        self.operation = {"kind": "group", "name": "start", "arguments": {
            "groupId": "baseline", "budgetUsdt": 2000, "durationSeconds": 60}}

    async def test_group_preflight_does_not_start(self):
        response = await self.client.post("/api/strategy-groups/baseline/preflight",
            json={"budgetUsdt": 2000, "durationSeconds": 60}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["mode"], "nautilus_sandbox")
        self.operator.group_runtime.execute.assert_not_awaited()

    async def test_live_workspace_does_not_offer_or_start_a_sandbox_group(self):
        self.operator.profiles.save({**self.operator.profiles.get(), 'mode':'live_readonly'})
        listing=await (await self.client.get('/api/strategy-groups')).json()
        self.assertEqual(listing['scope'],'live')
        self.assertTrue(all(row['mode']=='live' for row in listing['groups']))
        archive=await (await self.client.get('/api/strategy-groups?scope=all')).json()
        self.assertTrue(any(row['id']=='baseline' for row in archive['groups']))
        response=await self.client.post('/api/prepare',json=self.operation,headers=self.headers)
        self.assertEqual(response.status,400)
        self.assertIn('历史研究',(await response.json())['error'])
        self.operator.group_runtime.execute.assert_not_awaited()

    async def test_listing_and_default_detail_use_current_managed_run(self):
        state=Path(self.directory.name)/'runs'
        self.operator.group_runtime.path=state/'supervisor.sock'
        run=dict(runId='baseline-current',groupId='baseline',status='running',runDir=str(state/'baseline-current'))
        group=dict(groupId='baseline',name='四板块 60/40',runId=run['runId'],runs=[run],mode='nautilus_sandbox',ready=True,capabilities={})
        self.operator.group_runtime.status.return_value=dict(available=True,groups=[group])
        self.operator.managed_views[run['runId']]=SimpleNamespace(
            detail=lambda _:dict(id='baseline',runId=run['runId'],status='ready',metrics={'nav':200}),
            equity=lambda _:dict(groupId='baseline',runId=run['runId'],points=[]))
        listing=await (await self.client.get('/api/strategy-groups')).json()
        selected=next(row for row in listing['groups'] if row['id']=='baseline')
        self.assertEqual(selected['runId'],'baseline-current')
        self.assertEqual(selected['status'],'running')
        self.assertTrue(selected['managed'])
        detail=await (await self.client.get('/api/strategy-groups/baseline')).json()
        curve=await (await self.client.get('/api/strategy-groups/baseline/equity')).json()
        self.assertEqual(detail['runId'],curve['runId'])
        self.assertEqual(detail['status'],'running')
        self.operator.group_runtime.execute.assert_not_awaited()

    async def test_group_start_confirmed_once_and_uses_frozen_request(self):
        identifier = await self.ticket()
        responses = await asyncio.gather(*[
            self.client.post("/api/execute", json={"id": identifier}, headers=self.headers) for _ in range(5)])
        self.assertTrue(all(item.status == 200 for item in responses))
        self.operator.group_runtime.execute.assert_awaited_once_with(self.normalized, identifier)
        self.assertEqual(self.calls, 0)

    async def test_async_start_returns_receipt_before_slow_launch_and_never_repeats(self):
        release=asyncio.Event()
        async def slow(*_):
            await release.wait()
            return {'runId':'baseline-async','groupId':'baseline','status':'starting','receiptStatus':'completed'}
        self.operator.group_runtime.execute.side_effect=slow
        identifier=await self.ticket()
        try:
            response=await asyncio.wait_for(self.client.post('/api/execute',json={'id':identifier,'async':True},headers=self.headers),.5)
            self.assertEqual(response.status,202)
            self.assertEqual((await response.json())['status'],'processing')
            duplicate=await self.client.post('/api/execute',json={'id':identifier,'async':True},headers=self.headers)
            self.assertTrue((await duplicate.json())['replayed'])
            self.assertEqual(self.operator.database.execute('SELECT status FROM operations WHERE id=?',(identifier,)).fetchone()[0],'processing')
        finally:
            release.set()
            await asyncio.gather(*self.operator.pending_starts)
        self.operator.group_runtime.execute.assert_awaited_once()
        self.assertEqual(self.operator.database.execute('SELECT status FROM operations WHERE id=?',(identifier,)).fetchone()[0],'completed')

    async def test_live_group_frozen_inventory_hash_survives_confirmation(self):
        inventory_hash = "a" * 64
        self.normalized = {"groupId": "live-alpha", "action": "start", "registryVersion": "v2",
                           "inventoryHash": inventory_hash, "durationSeconds": 60}
        self.preview = {"request": self.normalized, "mode": "live", "groupName": "四板块实盘"}
        self.operator.group_runtime.prepare.return_value = self.preview
        self.operation = {"kind": "group", "name": "start", "arguments": {
            "groupId": "live-alpha", "durationSeconds": 60}}
        identifier = await self.ticket()
        response = await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.operator.group_runtime.execute.assert_awaited_once_with(self.normalized, identifier)

    async def test_group_preflight_requires_admin_csrf(self):
        response = await self.client.post("/api/strategy-groups/baseline/preflight", json={})
        self.assertEqual(response.status, 403)
        for entry in self.operator.sessions.values():
            entry["role"] = "viewer"
        self.assertEqual((await self.client.post("/api/prepare", json=self.operation, headers=self.headers)).status, 403)
        self.operator.group_runtime.prepare.assert_not_awaited()

    async def test_group_cannot_supply_code_paths_or_disagree_with_action(self):
        for fields in ({"strategyPath": "/tmp/code.py"}, {"action": "resume"}, {"groupId": "../other"}):
            value = {**self.operation, "arguments": {**self.operation["arguments"], **fields}}
            response = await self.client.post("/api/prepare", json=value, headers=self.headers)
            self.assertEqual(response.status, 400)
        self.operator.group_runtime.prepare.assert_not_awaited()

    async def test_group_execution_reports_failure_truthfully(self):
        self.operator.group_runtime.execute.return_value = {"status": "failed", "error": "启动失败"}
        identifier = await self.ticket()
        result = await (await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)).json()
        self.assertEqual(result["status"], "error")

    async def test_unknown_receipt_is_reconciled_via_get_without_resubmitting(self):
        self.operator.group_runtime.execute.return_value = {"status": "unknown", "receiptStatus": "unknown"}
        identifier = await self.ticket()
        response = await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.assertEqual((await response.json())["status"], "unknown")
        self.operator.group_runtime.receipt = AsyncMock(return_value={"operationId": identifier,
            "status": "halted", "receiptStatus": "completed"})
        receipt = await self.client.get(f"/api/operations/{identifier}")
        self.assertEqual((await receipt.json())["status"], "completed")
        self.operator.group_runtime.execute.assert_awaited_once()
        self.operator.group_runtime.receipt.assert_awaited_once_with(identifier)

    async def test_receipt_requires_own_session(self):
        identifier = await self.ticket()
        await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.operator.database.execute("UPDATE operations SET session='other-session',owner='other-operator' WHERE id=?", (identifier,))
        self.operator.database.commit()
        self.assertEqual((await self.client.get(f"/api/operations/{identifier}")).status, 403)

    async def test_stable_owner_can_read_after_relogin_but_cannot_replay_execution(self):
        identifier = await self.ticket()
        await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.operator.sessions.clear()
        session = await (await self.client.get("/api/session")).json()
        self.headers = {"X-Operator-CSRF": session["csrf"]}
        self.assertEqual((await self.client.get(f"/api/operations/{identifier}")).status, 200)
        self.assertEqual((await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)).status, 403)
        self.operator.group_runtime.execute.assert_awaited_once()

    async def test_legacy_ownerless_receipt_is_not_assigned_on_relogin(self):
        identifier = await self.ticket()
        await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.operator.database.execute("UPDATE operations SET owner=NULL WHERE id=?", (identifier,))
        self.operator.database.commit()
        self.operator.sessions.clear()
        await self.client.get("/api/session")
        self.assertEqual((await self.client.get(f"/api/operations/{identifier}")).status, 403)

    async def test_group_runtime_unavailable_is_explicit(self):
        self.operator.group_runtime.status.side_effect = ValueError("运行服务未连接")
        result = await (await self.client.get("/api/strategy-groups/baseline/runtime")).json()
        self.assertFalse(result["available"])
        self.assertEqual(result["groups"], [])
