"""Local cryptographic fixtures only; never enroll a test credential on the server."""
import base64
import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import cbor2
from aiohttp import CookieJar
from aiohttp.test_utils import TestClient, TestServer
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

from app import Operator
from passkeys import Passkeys

ORIGIN = "https://tokyo.montlok.com"


def encode(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class Authenticator:
    def __init__(self, synced=True):
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.id = hashlib.sha256(self.key.public_key().public_numbers().x.to_bytes(32, "big")).digest()
        self.backup = 0x18 if synced else 0

    def client(self, options, kind, origin):
        return json.dumps({"type": kind, "challenge": options["challenge"], "origin": origin}).encode()

    def register(self, options, origin=ORIGIN, rp="tokyo.montlok.com", uv=True):
        self.handle = options["user"]["id"]
        numbers = self.key.public_key().public_numbers()
        cose = cbor2.dumps({1: 2, 3: -7, -1: 1, -2: numbers.x.to_bytes(32, "big"), -3: numbers.y.to_bytes(32, "big")})
        data = (hashlib.sha256(rp.encode()).digest() + bytes([0x41 | (4 if uv else 0) | self.backup])
                + bytes(4 + 16) + len(self.id).to_bytes(2, "big") + self.id + cose)
        return {"id": encode(self.id), "rawId": encode(self.id), "type": "public-key",
                "response": {"clientDataJSON": encode(self.client(options, "webauthn.create", origin)),
                             "attestationObject": encode(cbor2.dumps({"fmt": "none", "attStmt": {}, "authData": data})),
                             "transports": ["internal", "hybrid"]}, "clientExtensionResults": {}}

    def login(self, options, origin=ORIGIN, rp="tokyo.montlok.com", uv=True, count=0, signature=True):
        client = self.client(options, "webauthn.get", origin)
        data = hashlib.sha256(rp.encode()).digest() + bytes([1 | (4 if uv else 0) | self.backup]) + count.to_bytes(4, "big")
        signed = self.key.sign(data + hashlib.sha256(client).digest(), ec.ECDSA(hashes.SHA256()))
        return {"id": encode(self.id), "rawId": encode(self.id), "type": "public-key",
                "response": {"clientDataJSON": encode(client), "authenticatorData": encode(data),
                             "signature": encode(signed if signature else bytes(64)), "userHandle": self.handle},
                "clientExtensionResults": {}}


class PasskeyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.identity = self.root / "identity.json"
        self.identity.write_text(json.dumps({"users": [{"username": "admin", "role": "admin"}, {"username": "audit", "role": "viewer"}]}))
        self.path = self.root / "passkeys.sqlite"
        self.store = Passkeys(self.path, self.identity, ORIGIN, initialize=True)
        self.authenticator = Authenticator()

    def tearDown(self):
        self.store.database.close()
        self.directory.cleanup()

    def enroll(self, user="admin"):
        token = self.store.issue_enrollment(user)
        flow, options = self.store.registration_options(token)
        self.store.register(flow, self.authenticator.register(options))
        return token

    def test_synced_passkey_login_and_persistent_password_retirement(self):
        token = self.enroll()
        self.assertFalse(self.store.password_disabled)
        for _ in range(2):  # Synced iCloud credentials may legitimately keep the counter at zero.
            flow, options = self.store.authentication_options()
            user, _ = self.store.authenticate(flow, self.authenticator.login(options))
            self.assertEqual(user, {"username": "admin", "role": "admin"})
        self.assertTrue(self.store.password_disabled)
        with self.assertRaises(ValueError):
            self.store.registration_options(token)
        self.store.database.close()
        self.store = Passkeys(self.path, self.identity, ORIGIN)
        self.assertTrue(self.store.password_disabled)
        with self.store.database:
            self.store.database.execute("DELETE FROM credentials")
        self.assertTrue(self.store.password_disabled)

    def test_missing_database_or_setting_does_not_enable_passwords(self):
        with self.assertRaises(sqlite3.OperationalError):
            Passkeys(self.root / "missing.sqlite", self.identity, ORIGIN)
        with self.store.database:
            self.store.database.execute("DELETE FROM settings WHERE key='password_login_disabled'")
        with self.assertRaises(ValueError):
            _ = self.store.password_disabled

    def test_registration_options_require_uv_and_resident_key(self):
        _, options = self.store.registration_options(self.store.issue_enrollment("admin"))
        self.assertEqual(options["authenticatorSelection"]["residentKey"], "required")
        self.assertEqual(options["authenticatorSelection"]["userVerification"], "required")
        self.assertEqual(options["rp"]["id"], "tokyo.montlok.com")
        self.assertEqual(options["attestation"], "none")

    def test_registration_rejects_origin_rp_and_missing_uv(self):
        for changes in ({"origin": "https://evil.example"}, {"rp": "montlok.com"}, {"uv": False}):
            with self.subTest(changes=changes):
                flow, options = self.store.registration_options(self.store.issue_enrollment("admin"))
                with self.assertRaises(ValueError):
                    self.store.register(flow, self.authenticator.register(options, **changes))
        self.assertEqual(self.store.database.execute("SELECT count(*) FROM credentials").fetchone()[0], 0)

    def test_login_rejects_origin_rp_uv_signature_user_handle_and_raw_id(self):
        self.enroll()
        for changes in ({"origin": "https://evil.example"}, {"rp": "montlok.com"}, {"uv": False}, {"signature": False}):
            with self.subTest(changes=changes):
                flow, options = self.store.authentication_options()
                with self.assertRaises(ValueError):
                    self.store.authenticate(flow, self.authenticator.login(options, **changes))
        for field in ("userHandle", "rawId"):
            flow, options = self.store.authentication_options()
            credential = self.authenticator.login(options)
            (credential["response"] if field == "userHandle" else credential)[field] = encode(b"another-user")
            with self.assertRaises(ValueError):
                self.store.authenticate(flow, credential)
        self.assertFalse(self.store.password_disabled)

    def test_challenge_is_single_use_bound_to_browser_and_expires(self):
        self.enroll()
        flow, options = self.store.authentication_options()
        credential = self.authenticator.login(options)
        with self.assertRaises(ValueError):
            self.store.authenticate("different-browser-cookie", credential)
        self.store.authenticate(flow, credential)
        with self.assertRaises(ValueError):
            self.store.authenticate(flow, credential)
        flow, options = self.store.authentication_options()
        self.store.pending[flow]["expires"] = time.monotonic() - 1
        with self.assertRaises(ValueError):
            self.store.authenticate(flow, self.authenticator.login(options))

    def test_old_challenge_cannot_be_used_with_new_flow(self):
        self.enroll()
        _, old_options = self.store.authentication_options()
        flow, _ = self.store.authentication_options()
        with self.assertRaises(ValueError):
            self.store.authenticate(flow, self.authenticator.login(old_options))

    def test_link_expires_and_new_link_revokes_old(self):
        old = self.store.issue_enrollment("admin")
        token = self.store.issue_enrollment("admin")
        with self.assertRaises(ValueError):
            self.store.registration_options(old)
        flow, options = self.store.registration_options(token)
        with self.store.database:
            self.store.database.execute("UPDATE enrollments SET expires=0")
        with self.assertRaises(ValueError):
            self.store.register(flow, self.authenticator.register(options))

    def test_viewer_remains_viewer_and_cannot_activate_admin_migration(self):
        self.enroll("audit")
        flow, options = self.store.authentication_options()
        user, activated = self.store.authenticate(flow, self.authenticator.login(options))
        self.assertEqual(user["role"], "viewer")
        self.assertFalse(activated)
        self.assertFalse(self.store.password_disabled)

    def test_removed_identity_cannot_login(self):
        self.enroll()
        self.identity.write_text('{"users": [{"username": "audit", "role": "viewer"}]}')
        flow, options = self.store.authentication_options()
        with self.assertRaises(ValueError):
            self.store.authenticate(flow, self.authenticator.login(options))

    def test_nonsynced_counter_replay_rejected(self):
        self.authenticator = Authenticator(synced=False)
        self.enroll()
        flow, options = self.store.authentication_options()
        self.store.authenticate(flow, self.authenticator.login(options, count=1))
        flow, options = self.store.authentication_options()
        with self.assertRaises(ValueError):
            self.store.authenticate(flow, self.authenticator.login(options, count=1))

    def test_request_budget_is_bounded(self):
        for _ in range(20):
            self.store.throttle("peer")
        with self.assertRaises(OverflowError):
            self.store.throttle("peer")


class PasskeyHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.identity = root / "identity.json"
        self.identity.write_text(json.dumps({"users": [{"username": "admin", "role": "admin"}, {"username": "audit", "role": "viewer"}]}))
        store = Passkeys(root / "passkeys.sqlite", self.identity, ORIGIN, initialize=True)
        store.database.close()
        here = Path(__file__).resolve().parent
        args = SimpleNamespace(state_dir=root, credentials=None, auth_file=self.identity, passkey_origin=ORIGIN,
                               catalog=here.parent / "catalog", paper_bridge=here / "paper_stub.py",
                               paper_run=Path("test-run"), static_dir=here, node="unused", mcp="unused", allowed_origins=[ORIGIN])
        self.operator = Operator(args)
        self.operator.profiles.save({"id": "demo", "name": "Test", "mode": "demo", "site": "global",
                                     "apiKey": "test-api-key", "secret": "test-secret", "passphrase": "test"})
        self.operator.profiles.select("demo")
        app = self.operator.application()
        app.cleanup_ctx.clear()
        self.client = TestClient(TestServer(app), cookie_jar=CookieJar(unsafe=True))
        await self.client.start_server()
        self.headers = {"Origin": ORIGIN, "X-Operator-Public": "1", "X-Forwarded-Proto": "https"}
        self.authenticator = Authenticator()

    async def asyncTearDown(self):
        await self.client.close()
        self.operator.database.close()
        self.operator.passkeys.database.close()
        self.directory.cleanup()

    async def post(self, action, value, cookie=""):
        return await self.client.post("/api/passkeys/" + action, json=value,
                                      headers={**self.headers, "Cookie": cookie})

    async def enroll_and_login(self, user="admin"):
        response = await self.post("register-options", {"token": self.operator.passkeys.issue_enrollment(user)})
        self.assertEqual(response.status, 200, await response.text())
        cookie = response.cookies["__Host-operator_passkey"]
        self.assertTrue(cookie["secure"] and cookie["httponly"] and cookie["samesite"] == "Strict")
        flow_cookie = "__Host-operator_passkey=" + cookie.value
        response = await self.post("register", {"credential": self.authenticator.register(await response.json())}, flow_cookie)
        self.assertEqual(response.status, 200, await response.text())
        self.assertNotIn("__Host-operator_session", response.cookies)
        response = await self.post("login-options", {})
        options = await response.json()
        self.assertFalse(options.get("allowCredentials"))
        flow_cookie = "__Host-operator_passkey=" + response.cookies["__Host-operator_passkey"].value
        return await self.post("login", {"credential": self.authenticator.login(options)}, flow_cookie)

    async def test_admin_activation_retires_password_and_old_sessions(self):
        self.operator.sessions["old"] = {"expires": time.time() + 3600}
        self.operator.confirmations["old"] = {"test": True}
        response = await self.enroll_and_login()
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual((await response.json())["role"], "admin")
        self.assertNotIn("old", self.operator.sessions)
        self.assertEqual(self.operator.confirmations, {})
        response = await self.client.post("/api/login", json={"username": "admin", "password": "anything"}, headers=self.headers)
        self.assertEqual(response.status, 410)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    async def test_viewer_passkey_cannot_prepare_mutation(self):
        response = await self.enroll_and_login("audit")
        self.assertEqual(response.status, 200, await response.text())
        session = await response.json()
        headers = {**self.headers, "X-Operator-CSRF": session["csrf"],
                   "Cookie": "__Host-operator_session=" + response.cookies["__Host-operator_session"].value}
        response = await self.client.post("/api/prepare", json={}, headers=headers)
        self.assertEqual(response.status, 403)

    async def test_origin_https_json_and_enrollment_authorization_required(self):
        for headers in ({}, {**self.headers, "Origin": "https://evil.example"},
                        {**self.headers, "Origin": ""}, {**self.headers, "X-Forwarded-Proto": "http"}):
            response = await self.client.post("/api/passkeys/login-options", json={}, headers=headers)
            self.assertIn(response.status, (403, 503))
        self.assertEqual((await self.client.post("/api/passkeys/login-options", data="{}", headers=self.headers)).status, 415)
        self.assertEqual((await self.post("register-options", {})).status, 400)
        self.assertEqual((await self.post("register-options", {"token": "x" * 43})).status, 400)
        self.assertEqual((await self.post("login", {"credential": {}})).status, 400)


if __name__ == "__main__":
    unittest.main()
