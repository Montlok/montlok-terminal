import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp import CookieJar
from aiohttp.test_utils import TestClient, TestServer
from aiohttp.test_utils import make_mocked_request

from app import Operator
from auth import Authentication, password_hash
from profiles import atomic_private


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        here = Path(__file__).resolve().parent
        args = SimpleNamespace(state_dir=Path(self.directory.name), credentials=None,
                               catalog=here.parent / "catalog", paper_bridge=here / "paper_stub.py",
                               paper_run=Path("test-run"), static_dir=here, node="unused", mcp="unused",
                               allowed_origins=["http://127.0.0.1:18081"])
        self.operator = Operator(args)
        self.operator.profiles.save({"id": "demo", "name": "Test", "mode": "demo", "site": "global",
                                     "apiKey": "test-api-key", "secret": "super-secret", "passphrase": "phrase"})
        self.operator.profiles.select("demo")
        self.calls = 0
        async def fake_perform(operation):
            self.calls += 1
            await asyncio.sleep(0.005)
            return {"ok": True, "testOnly": True}
        self.operator.perform = fake_perform
        app = self.operator.application()
        app.cleanup_ctx.clear()  # No market/account background requests in these tests.
        self.client = TestClient(TestServer(app), cookie_jar=CookieJar(unsafe=True))
        await self.client.start_server()
        session = await (await self.client.get("/api/session")).json()
        self.headers = {"X-Operator-CSRF": session["csrf"]}
        self.operation = {"kind": "mcp", "name": "spot_place_order", "arguments": {
            "instId": "BTC-USDT", "tdMode": "cash", "side": "buy", "ordType": "market", "sz": "0.001"}}

    async def asyncTearDown(self):
        await self.client.close()
        self.operator.database.close()
        self.directory.cleanup()

    async def ticket(self):
        response = await self.client.post("/api/prepare", json=self.operation, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        return (await response.json())["id"]

    async def test_mutation_needs_csrf(self):
        response = await self.client.post("/api/prepare", json=self.operation)
        self.assertEqual(response.status, 403)
        self.assertEqual((await response.json())["code"], "SESSION_CSRF_MISMATCH")
        self.assertEqual(self.calls, 0)

    async def test_cross_origin_rejected(self):
        response = await self.client.get("/api/session", headers={"Origin": "https://untrusted.example"})
        self.assertEqual(response.status, 403)

    def test_public_swap_charts_do_not_enable_derivative_trading(self):
        request = make_mocked_request('GET', '/api/market/candles?instrument=BTC-USDT-SWAP&bar=1H&mode=live')
        self.assertEqual(self.operator.market_parameters(request), ('BTC-USDT-SWAP', '1H', 'live'))
        with self.assertRaises(ValueError):
            self.operator.market_parameters(make_mocked_request('GET', '/api/market/candles?instrument=BTC-USDT-SWAP/other'))

    async def test_prepare_does_not_execute(self):
        await self.ticket()
        self.assertEqual(self.calls, 0)

    async def test_concurrent_confirmation_executes_once(self):
        identifier = await self.ticket()
        responses = await asyncio.gather(*[self.client.post("/api/execute", json={"id": identifier}, headers=self.headers) for _ in range(20)])
        self.assertTrue(all(response.status == 200 for response in responses))
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.operator.database.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 1)

    async def test_expired_confirmation_rejected(self):
        identifier = await self.ticket()
        self.operator.confirmations[identifier]["expires"] = time.time() - 1
        response = await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual(self.calls, 0)

    async def test_profile_change_invalidates_confirmation(self):
        identifier = await self.ticket()
        self.operator.profiles.select("demo")
        response = await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual(self.calls, 0)

    async def test_live_profile_is_readonly(self):
        self.operator.profiles.save({**self.operator.profiles.get(), "mode": "live_readonly"})
        response = await self.client.post("/api/prepare", json=self.operation, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual(self.calls, 0)

    async def test_live_trading_connection_requires_confirmed_execute(self):
        self.operator.profiles.save({**self.operator.profiles.get(), "mode": "live"},
                                    {"permissions": "read_only,trade"})
        identifier = await self.ticket()
        self.assertEqual(self.calls, 0)
        response = await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.assertEqual(self.calls, 1)  # fake_perform only, no exchange transport

    async def test_live_unverified_permissions_cannot_prepare(self):
        self.operator.profiles.save({**self.operator.profiles.get(), "mode": "live"})
        response = await self.client.post("/api/prepare", json=self.operation, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual(self.calls, 0)

    async def test_live_trade_profile_supports_sell_derivatives_and_batch_cancel(self):
        self.operator.profiles.save({**self.operator.profiles.get(), "mode": "live"},
                                    {"permissions": "read_only,trade"})
        for name, arguments in (
            ("POST /api/v5/trade/order", {"instId": "BTC-USDT", "side": "sell", "tdMode": "cash", "ordType": "limit", "sz": "0.1", "px": "100000"}),
            ("POST /api/v5/trade/batch-orders", [{"instId": "BTC-USDT-SWAP", "tdMode": "cross", "side": "buy", "ordType": "limit", "sz": "1", "px": "100000"}]),
            ("POST /api/v5/trade/cancel-batch-orders", [{"instId": "BTC-USDT", "ordId": "test-only"}]),
        ):
            response = await self.client.post("/api/prepare", json={"kind": "rest", "name": name, "arguments": arguments}, headers=self.headers)
            self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(self.calls, 0)

    def test_mcp_transport_keeps_explicit_readonly_distinct_from_live_trading(self):
        from exchange import mcp_flags
        self.assertEqual(mcp_flags('live'), ['--live'])
        self.assertEqual(mcp_flags('live_readonly'), ['--live', '--read-only'])
        self.assertEqual(mcp_flags('demo'), ['--demo'])
        with self.assertRaises(ValueError):
            mcp_flags('typo')

    async def test_credentials_are_not_exposed(self):
        result = await (await self.client.get("/api/profiles")).text()
        self.assertNotIn("super-secret", result)
        self.assertNotIn('"passphrase"', result)
        self.assertNotIn(b"super-secret", self.operator.profiles.path.read_bytes())

    async def test_derivative_batch_is_available_without_spot_only_application_policy(self):
        operation = {"kind": "rest", "name": "POST /api/v5/trade/batch-orders", "arguments": [
            {"instId": "BTC-USDT-SWAP", "tdMode": "cross", "side": "buy", "ordType": "market", "sz": "1"}]}
        response = await self.client.post("/api/prepare", json=operation, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(self.calls, 0)

    async def test_spot_margin_is_available_for_selected_account(self):
        self.operation["arguments"]["tdMode"] = "cross"
        response = await self.client.post("/api/prepare", json=self.operation, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(self.calls, 0)

    async def test_leverage_and_borrowing_are_not_hidden_by_application_policy(self):
        for name in ('POST /api/v5/account/set-leverage', 'POST /api/v5/account/spot-manual-borrow-repay'):
            self.assertIsNone(self.operator.blocked('rest', name, {}))
        self.assertEqual(self.calls, 0)

    def enable_login(self):
        path = Path(self.directory.name) / "login.json"
        salt = b"test-salt-16byte!"
        atomic_private(path, json.dumps({"username": "tester", "salt": salt.hex(),
                                        "hash": password_hash("test-password", salt)}).encode())
        self.operator.auth = Authentication(path)

    async def test_public_entry_requires_configured_auth(self):
        response = await self.client.get("/api/session", headers={"X-Operator-Public": "1", "X-Forwarded-Proto": "https"})
        self.assertEqual(response.status, 503)

    async def test_anonymous_cannot_mint_session_when_login_enabled(self):
        self.enable_login()
        self.client.session.cookie_jar.clear()
        for path in ("/api/session", "/api/account", "/api/profiles", "/api/catalog", "/api/events",
                     "/api/strategy-groups", "/api/strategy-groups/baseline", "/api/strategy-groups/baseline/equity"):
            response = await self.client.get(path)
            self.assertEqual(response.status, 401, path)

    async def test_groups_are_read_only_and_unknown_ids_are_not_paths(self):
        response = await self.client.get("/api/strategy-groups/enhanced")
        self.assertEqual(response.status, 200)
        group = await response.json()
        self.assertIsNone(group["metrics"]["nav"])
        self.assertEqual(group["positions"], [])
        self.assertEqual((await self.client.get("/api/strategy-groups/unknown")).status, 404)
        self.assertEqual((await self.client.post("/api/strategy-groups/baseline", json={})).status, 405)
        self.assertEqual(self.calls, 0)

    async def test_login_secure_cookie_and_logout_revoke(self):
        self.enable_login()
        proxy = {"X-Operator-Public": "1", "X-Forwarded-Proto": "https"}
        response = await self.client.post("/api/login", json={"username": "tester", "password": "test-password"}, headers=proxy)
        self.assertEqual(response.status, 200)
        cookie = response.cookies["__Host-operator_session"]
        self.assertTrue(cookie["secure"])
        self.assertTrue(cookie["httponly"])
        self.assertEqual(cookie["samesite"], "Strict")
        headers = {**proxy, "Cookie": f"__Host-operator_session={cookie.value}", "X-Operator-CSRF": (await response.json())["csrf"]}
        self.assertEqual((await self.client.get("/api/account", headers=headers)).status, 200)
        logout = await self.client.post("/api/logout", json={}, headers=headers)
        self.assertEqual(logout.status, 200)
        self.assertTrue(logout.cookies["__Host-operator_session"]["secure"])
        self.assertEqual((await self.client.get("/api/account", headers=headers)).status, 401)

    async def test_login_bad_password_and_rate_limit(self):
        self.enable_login()
        for _ in range(5):
            response = await self.client.post("/api/login", json={"username": "tester", "password": "wrong"})
            self.assertEqual(response.status, 401)
        response = await self.client.post("/api/login", json={"username": "tester", "password": "test-password"})
        self.assertEqual(response.status, 429)

    async def test_local_session_cannot_be_replayed_as_public(self):
        self.enable_login()
        identifier = next(iter(self.operator.sessions))
        response = await self.client.get("/api/account", headers={"X-Operator-Public": "1", "X-Forwarded-Proto": "https", "Cookie": f"__Host-operator_session={identifier}"})
        self.assertEqual(response.status, 401)

    async def test_completed_receipt_bound_to_session(self):
        identifier = await self.ticket()
        await self.client.post("/api/execute", json={"id": identifier}, headers=self.headers)
        self.client.session.cookie_jar.clear()
        value = await (await self.client.get("/api/session")).json()
        response = await self.client.post("/api/execute", json={"id": identifier}, headers={"X-Operator-CSRF": value["csrf"]})
        self.assertEqual(response.status, 403)
        self.assertEqual(self.calls, 1)


if __name__ == "__main__":
    unittest.main()
