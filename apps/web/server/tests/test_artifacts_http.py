import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp import CookieJar, FormData, web
from aiohttp.test_utils import TestClient, TestServer, make_mocked_request

from app import Operator


class ArtifactHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        here = Path(__file__).resolve().parent
        self.operator = Operator(SimpleNamespace(
            state_dir=Path(self.temporary.name), credentials=None, catalog=here.parent / "catalog",
            paper_bridge=here / "paper_stub.py", paper_run=Path("test-run"), static_dir=here,
            node="unused", mcp="unused", allowed_origins=["http://127.0.0.1:18081"],
        ))
        self.operator.profiles.save({"id": "demo", "name": "Test", "mode": "demo", "site": "global",
            "apiKey": "fake", "secret": "test-secret", "passphrase": "phrase"})
        self.operator.profiles.select("demo")
        app = self.operator.application()
        app.cleanup_ctx.clear()
        self.client = TestClient(TestServer(app), cookie_jar=CookieJar(unsafe=True))
        await self.client.start_server()
        session = await (await self.client.get("/api/session")).json()
        self.headers = {"X-Operator-CSRF": session["csrf"]}

    async def asyncTearDown(self):
        await self.client.close()
        self.operator.database.close()
        self.temporary.cleanup()

    def form(self, content=b'{"window":20}', filename="factor.json", kind="factor", extra=None):
        form = FormData()
        for name, value in (("kind", kind), ("name", "动量"), ("version", "1.0.0")):
            form.add_field(name, value)
        form.add_field("file", io.BytesIO(content), filename=filename, content_type="application/octet-stream")
        if extra:
            form.add_field(*extra)
        return form

    async def test_registration_and_viewer_metadata_list(self):
        response = await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)
        self.assertEqual(response.status, 201, await response.text())
        artifact = (await response.json())["artifact"]
        self.assertEqual(artifact["validation"], "pending_validation")
        self.assertNotIn("path", artifact)
        for session in self.operator.sessions.values():
            session["role"] = "viewer"
        response = await self.client.get("/api/artifacts?kind=factor")
        result = await response.json()
        self.assertEqual(result["artifacts"], [artifact])
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual((await self.client.head("/api/artifacts")).status, 200)
        self.assertEqual((await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)).status, 403)

    async def test_auth_and_csrf_checked_before_consuming_multipart(self):
        request = make_mocked_request("POST", "/api/artifacts", headers={"Content-Type": "multipart/form-data; boundary=x"})
        with patch.object(web.Request, "multipart", new_callable=AsyncMock) as read:
            with self.assertRaises(web.HTTPUnauthorized):
                await self.operator.artifacts_route(request)
            read.assert_not_awaited()
        response = await self.client.post("/api/artifacts", data=self.form())
        self.assertEqual(response.status, 403)
        self.assertEqual((await response.json())["code"], "SESSION_CSRF_MISMATCH")
        self.assertEqual(self.operator.artifacts.list(), [])

    async def test_anonymous_list_denied_and_cross_origin_upload_denied(self):
        response = await self.client.post("/api/artifacts", data=self.form(),
            headers={**self.headers, "Origin": "https://untrusted.example"})
        self.assertEqual(response.status, 403)
        self.client.session.cookie_jar.clear()
        self.assertEqual((await self.client.get("/api/artifacts")).status, 401)

    async def test_only_upload_route_accepts_more_than_one_mib(self):
        payload = b"opaque model bytes" + b"x" * 1_048_576
        response = await self.client.post("/api/artifacts", data=self.form(payload, "model.onnx", "model"), headers=self.headers)
        self.assertEqual(response.status, 201, await response.text())
        self.assertEqual((await response.json())["artifact"]["bytes"], len(payload))
        response = await self.client.post("/api/query", json={"padding": "x" * 1_048_576}, headers=self.headers)
        self.assertEqual(response.status, 413)

    async def test_oversized_fixed_and_chunked_uploads_are_rejected_without_registration(self):
        self.operator.artifacts.max_bytes = 1024
        response = await self.client.post("/api/artifacts", data=self.form(b"x" * 20000, "model.onnx", "model"), headers=self.headers)
        self.assertEqual(response.status, 413)
        response = await self.client.post("/api/artifacts", data=self.form(b"x" * 2048, "model.onnx", "model"),
            headers=self.headers, chunked=True)
        self.assertEqual(response.status, 413)
        self.assertEqual(self.operator.artifacts.list(), [])
        self.assertEqual(list((self.operator.artifacts.root / "incoming").iterdir()), [])

    async def test_duplicate_extra_and_file_first_fields_do_not_commit(self):
        for field in (("name", "duplicate"), ("unknown", "secret")):
            response = await self.client.post("/api/artifacts", data=self.form(extra=field), headers=self.headers)
            self.assertEqual(response.status, 400, await response.text())
        form = FormData()
        form.add_field("file", b"{}", filename="factor.json")
        form.add_field("kind", "factor")
        response = await self.client.post("/api/artifacts", data=form, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual(self.operator.artifacts.list(), [])
        self.assertEqual(list((self.operator.artifacts.root / "objects").iterdir()), [])

    async def test_invalid_content_extension_and_metadata_have_bounded_errors(self):
        for form in (self.form(b"not json"), self.form(filename="../factor.json"),
                     self.form(filename="model.pkl", kind="model")):
            response = await self.client.post("/api/artifacts", data=form, headers=self.headers)
            self.assertEqual(response.status, 400, await response.text())
            self.assertNotIn("not json", await response.text())
            self.assertNotIn(self.temporary.name, await response.text())
        self.assertEqual((await self.client.get("/api/artifacts?kind=unknown")).status, 400)
        self.assertEqual((await self.client.post("/api/artifacts", json={}, headers=self.headers)).status, 415)
        response = await self.client.post("/api/artifacts", data=b"invalid", headers={
            **self.headers, "Content-Type": "multipart/form-data; boundary=x"})
        self.assertEqual(response.status, 400)

    async def test_storage_failures_do_not_expose_paths_sql_or_contents(self):
        secret = "/www/private/secret/token database contents"
        with patch.object(self.operator.artifacts, "list", side_effect=OSError(secret)):
            response = await self.client.get("/api/artifacts")
            self.assertEqual(response.status, 503)
            self.assertEqual(await response.json(), {"error": "文件库暂不可用"})
        with patch.object(self.operator.artifacts, "register_async", side_effect=OSError(secret)):
            response = await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)
            self.assertEqual(response.status, 503)
            self.assertNotIn(secret, await response.text())

    async def test_concurrent_upload_is_rejected_and_version_conflicts_preserved(self):
        async with self.operator.artifact_upload_lock:
            response = await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)
            self.assertEqual(response.status, 429)
        self.assertEqual((await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)).status, 201)
        response = await self.client.post("/api/artifacts", data=self.form(b'{"window":60}'), headers=self.headers)
        self.assertEqual(response.status, 409)

    async def test_revoked_session_during_upload_does_not_commit(self):
        register = self.operator.artifacts.register_async
        async def revoke(chunks, **fields):
            async def wrapped():
                async for chunk in chunks:
                    yield chunk
                    self.operator.sessions.clear()
            return await register(wrapped(), **fields)
        with patch.object(self.operator.artifacts, "register_async", side_effect=revoke):
            response = await self.client.post("/api/artifacts", data=self.form(), headers=self.headers)
        self.assertEqual(response.status, 401)
        self.assertEqual(self.operator.artifacts.list(), [])
        self.assertEqual(list((self.operator.artifacts.root / "incoming").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
