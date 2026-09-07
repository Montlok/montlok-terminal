"""Local operator API. Exchange mutations require a session-bound, single-use confirmation."""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import importlib.util
import json
import re
import secrets
import signal
import sqlite3
import time
from pathlib import Path
from urllib.parse import unquote

import aiohttp
import jsonschema
import psutil
from aiohttp import web

from artifacts import ArtifactError, ArtifactStore, ArtifactTooLarge
from auth import Authentication
from exchange import MCP, rest, verify
from group_runtime import GroupRuntimeClient
from group_views import GroupViews
from model_releases import ModelReleaseStore
from passkeys import Passkeys
from profiles import Profiles
from sessions import SessionStore
from watch_market import PublicWatchMarket, parse_symbols


def redact(value):
    if isinstance(value, dict):
        return {key: "[已隐藏]" if re.search(r"secret|passphrase|password|api.?key|token|authorization|cookie", key, re.I) else redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


class Operator:
    def __init__(self, args):
        self.args = args
        self.profiles = Profiles(args.state_dir, args.credentials)
        self.artifacts = ArtifactStore(args.state_dir / "artifacts")
        self.model_releases = ModelReleaseStore(args.state_dir / "models", self.artifacts)
        self.artifact_upload_lock = asyncio.Lock()
        self.mcp = MCP(args.node, args.mcp)
        self.sessions = {}
        self.auth = Authentication(args.auth_file) if getattr(args, "auth_file", None) else None
        self.session_store = SessionStore(args.state_dir,self.profiles.cipher,getattr(args,'auth_file',None),getattr(args,'passkey_origin',None))
        self.sessions = self.session_store.load()
        self.passkeys = Passkeys(args.state_dir / "passkeys.sqlite", args.auth_file, args.passkey_origin) if getattr(args, "passkey_origin", None) else None
        self.confirmations = {}
        self.write_lock = asyncio.Lock()
        self.tools = {tool["name"]: tool for tool in json.loads((args.catalog / "okx_mcp_tools.json").read_text())["tools"]}
        self.routes = {f"{item['method']} {item['path']}": item for item in json.loads((args.catalog / "okx_rest_endpoints.json").read_text())}
        self.native_methods = json.loads((args.catalog / "nautilus_operator_methods.json").read_text())
        self.database = sqlite3.connect(args.state_dir / "operations.sqlite")
        self.database.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, at REAL, profile TEXT, action TEXT, parameters TEXT, status TEXT, result TEXT)")
        self.database.commit()
        if "session" not in {row[1] for row in self.database.execute("PRAGMA table_info(operations)")}:
            self.database.execute("ALTER TABLE operations ADD COLUMN session TEXT")
            self.database.commit()
        if "owner" not in {row[1] for row in self.database.execute("PRAGMA table_info(operations)")}:
            self.database.execute("ALTER TABLE operations ADD COLUMN owner TEXT")
            self.database.commit()
        self.database.execute("UPDATE operations SET status='unknown',result=? WHERE status='processing'",
                              (json.dumps({"error": "服务重启前的操作结果待核对，请查看运行实例与交易记录"}),))
        self.database.commit()
        self.account = {"available": False, "privateConnected": False, "balances": [], "orders": [], "fills": []}
        self.last_account_epoch = -1
        self.market_snapshots = {}
        self.snapshot_lock = asyncio.Lock()
        self.account_changed = asyncio.Event()
        self.live_only = getattr(args, "live_only", False)
        self.paper = None
        self.group_views = None
        if not self.live_only:
            spec = importlib.util.spec_from_file_location("paper_view", args.paper_bridge)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.paper = module.RunView(args.paper_run)
            self.group_views = GroupViews(args.paper_run, self.paper)
        self.group_runtime = GroupRuntimeClient(getattr(args, "group_socket", None))
        self.managed_views = {}
        self.watch_market = PublicWatchMarket(self.fetch_watch_market)
        self.pending_starts = set()

    async def fetch_watch_market(self):
        # Public quotes are independent of the active private account. No
        # credentials or account data enter this shared market cache.
        async with self.http.get('https://www.okx.com/api/v5/market/tickers',params={'instType':'SPOT'},
                allow_redirects=False,timeout=aiohttp.ClientTimeout(total=10)) as response:
            value=await response.json(content_type=None)
            if response.status!=200 or value.get('code')!='0':
                raise ValueError('行情暂时不可用')
            return value['data']

    async def watch_market_route(self, request):
        self.session(request)
        return web.json_response(await self.watch_market.read(parse_symbols(request.query.get('instruments',''))))

    def session(self, request, write=False):
        public = request.headers.get("X-Operator-Public") == "1"
        session_id = request.cookies.get("__Host-operator_session" if public else "operator_session")
        entry = self.sessions.get(session_id)
        if not entry or entry["expires"] < time.time() or time.time() - entry["lastSeen"] > 1800 or entry["public"] != public:
            raise web.HTTPUnauthorized(text="请登录交易操作台")
        entry["lastSeen"] = time.time()
        self.session_store.save(self.sessions)
        if write and not secrets.compare_digest(request.headers.get("X-Operator-CSRF", ""), entry["csrf"]):
            raise web.HTTPForbidden(text=json.dumps({"code": "SESSION_CSRF_MISMATCH", "error": "操作会话已更新"}),
                                    content_type="application/json")
        return session_id

    def blocked(self, kind, name, arguments=None):
        arguments = arguments or {}
        if isinstance(arguments, list):
            return next((reason for item in arguments if (reason := self.blocked(kind, name, item))), None)
        profile = self.profiles.get()
        is_write = kind == "mcp" and not self.tools[name].get("annotations", {}).get("readOnlyHint", False)
        is_write |= kind == "rest" and not name.startswith("GET ")
        if not is_write:
            return None
        if profile["mode"] == "live_readonly":
            return "此连接的手动操作权限为只读，可在 API 连接中设置"
        if profile["mode"] not in ("demo", "live"):
            return "交易环境未配置"
        if profile["mode"] == "live":
            permissions = (profile.get("verification") or {}).get("permissions", "")
            if "trade" not in re.split(r"[,;\s]+", permissions):
                return "此 API 尚未核验交易权限，请测试并保存连接"
        if name == "skills_download":
            return "技能安装需要主机级流程，此账户操作面板仅提供查询"
        if isinstance(arguments.get("orders"), list):
            for order in arguments["orders"]:
                reason = self.blocked(kind, name, order)
                if reason:
                    return reason
        return None

    def require_operator(self, request):
        identifier = self.session(request, True)
        if self.sessions[identifier].get("role", "viewer") != "admin":
            raise web.HTTPForbidden(text="此账号为只读权限")
        return identifier

    def validate_operation(self, operation):
        if not isinstance(operation, dict):
            raise ValueError("操作必须是 JSON 对象")
        kind, name, arguments = operation.get("kind"), operation.get("name"), operation.get("arguments", {})
        if not isinstance(arguments, dict) and not (kind == "rest" and isinstance(arguments, list) and not name.startswith("GET ")):
            raise ValueError("参数必须是 JSON 对象")
        if isinstance(arguments, list) and not all(isinstance(item, dict) for item in arguments):
            raise ValueError("批量请求的每一项必须是 JSON 对象")
        if kind == "mcp":
            if name not in self.tools:
                raise ValueError("未知 MCP 工具")
            if any(key in arguments for key in ("profile", "site", "baseUrl", "base_url")):
                raise ValueError("账户和站点只能在连接管理中切换")
            properties = self.tools[name]["inputSchema"].get("properties", {})
            if set(arguments) - set(properties):
                raise ValueError("请求包含工具定义之外的字段")
            if "demo" in properties:
                arguments = {**arguments, "demo": self.profiles.get()["mode"] == "demo"}
            jsonschema.validate(arguments, self.tools[name]["inputSchema"])
            read_only = self.tools[name].get("annotations", {}).get("readOnlyHint", False)
        elif kind == "rest":
            if name not in self.routes:
                raise ValueError("该路由不在官方 API 目录中")
            if isinstance(arguments, dict) and any(key in arguments for key in ("headers", "baseUrl", "base_url", "apiKey", "secret")):
                raise ValueError("请求不可覆盖服务器认证与目的地址")
            read_only = name.startswith("GET ")
        elif kind == "native" and name in ("status", "positions", "orders", "fills", "strategies", "config", "exceptions", "stop"):
            if self.live_only:
                raise ValueError("生产工作台不提供模拟运行操作")
            if arguments.get("runId") != self.args.paper_run.name:
                raise ValueError("运行实例已变化，请刷新后重试")
            read_only = name != "stop"
        elif kind == "group" and name in ("start", "halt", "reduce", "resume", "stop", "cancel", "flatten"):
            if set(arguments) - {"groupId", "runId", "budgetUsdt", "durationSeconds", "registryVersion", "inventoryHash", "action", "instruments", "executionSettings"}:
                raise ValueError("策略组操作包含未定义参数")
            if not isinstance(arguments.get("groupId"), str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", arguments["groupId"]):
                raise ValueError("策略组标识不正确")
            if arguments.get("action", name) != name:
                raise ValueError("策略组操作与确认内容不一致")
            arguments = {**arguments, "action": name}
            read_only = False
        elif kind == "model_release" and name in ("validate", "publish", "rollback"):
            allowed = {"artifactId"} if name == "validate" else {"artifactId", "releaseId", "manifestSha256", "expectedActiveReleaseId"}
            if set(arguments) - allowed:
                raise ValueError("模型发布请求包含未定义字段")
            if name == "validate" and not isinstance(arguments.get("artifactId"), str):
                raise ValueError("请选择已登记的模型文件")
            read_only = False
        elif kind == "profile" and name in ("save", "select", "delete"):
            if name == "save":
                arguments = self.profiles.validate(arguments)
            else:
                self.profiles.get(arguments.get("id"))
            selected = arguments if name == "save" else self.profiles.get(arguments.get("id")) if name == "select" else None
            if self.live_only and selected and selected.get("mode") == "demo":
                raise ValueError("生产工作台只接受实盘连接")
            read_only = False
        else:
            raise ValueError("未知操作")
        if kind in ("rest", "mcp"):
            reason = self.blocked(kind, name, arguments)
            if reason:
                raise ValueError(reason)
        return {"kind": kind, "name": name, "arguments": arguments}, read_only

    async def perform(self, operation):
        kind, name, arguments = operation["kind"], operation["name"], operation["arguments"]
        if kind == "mcp":
            return await self.mcp.call(self.profiles.get(), self.profiles.epoch, name, arguments)
        if kind == "rest":
            method, path = name.split(" ", 1)
            return await rest(self.http, self.profiles.get(), method, path, arguments)
        if kind == "profile":
            if name == "save":
                result = self.profiles.save(arguments, await verify(self.http, arguments))
            elif name == "select":
                await verify(self.http, self.profiles.get(arguments["id"]))
                result = self.profiles.select(arguments["id"])
            else:
                result = self.profiles.delete(arguments["id"])
            self.account = {"available": False, "privateConnected": False, "balances": [], "orders": [], "fills": []}
            return result
        if kind == "native":
            if self.live_only or self.paper is None:
                raise ValueError("生产工作台不提供模拟运行操作")
            snapshot = await asyncio.to_thread(self.paper.snapshot)
            if name == "stop":
                manifest = json.loads((self.args.paper_run / "manifest.json").read_text())
                process = psutil.Process(manifest["pid"])
                if abs(process.create_time() - manifest["process_create_time"]) > 0.01:
                    raise ValueError("进程身份已变化，未发送停止信号")
                process.send_signal(signal.SIGINT)
                return {"runId": self.args.paper_run.name, "status": "stop_requested", "detail": "已请求本轮模拟正常收尾；不等同于已停止或已清空持仓"}
            return snapshot if name == "status" else snapshot.get("runtimeConfig" if name == "config" else name)
        raise ValueError("未知操作")

    async def session_route(self, request):
        try:
            existing = self.sessions[self.session(request)]
            return web.json_response({"csrf": existing["csrf"], "operator": existing["operator"], "role": existing["role"], "mode": self.profiles.get()["mode"]})
        except web.HTTPUnauthorized:
            if self.auth:
                raise
        return self.new_session(request, "SSH 本地操作员")

    def new_session(self, request, operator, role="admin"):
        identifier = secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(32)
        now = time.time()
        self.sessions = {key: value for key, value in self.sessions.items() if value["expires"] > now}
        public = request.headers.get("X-Operator-Public") == "1"
        self.sessions[identifier] = {"csrf": csrf, "expires": now + 8 * 3600, "lastSeen": now,
                                     "operator": operator, "public": public, "role": role}
        self.session_store.save(self.sessions,True)
        response = web.json_response({"csrf": csrf, "operator": operator, "role": role, "mode": self.profiles.get()["mode"]})
        response.set_cookie("__Host-operator_session" if public else "operator_session", identifier,
                            secure=public, httponly=True, samesite="Strict", max_age=8 * 3600, path="/")
        return response

    async def login_route(self, request):
        if self.passkeys and self.passkeys.password_disabled:
            raise web.HTTPGone(text="请使用通行密钥登录")
        if not self.auth:
            raise web.HTTPServiceUnavailable(text="尚未配置网页登录")
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType()
        value = await request.json()
        peer = request.headers.get("X-Real-IP", request.remote) if request.headers.get("X-Operator-Public") == "1" else request.remote
        try:
            accepted = await self.auth.verify(peer, value.get("username", ""), value.get("password", ""))
        except OverflowError as error:
            raise web.HTTPTooManyRequests(text=str(error), headers={"Retry-After": "300"}) from None
        if not accepted:
            raise web.HTTPUnauthorized(text="用户名或密码错误")
        # A passkey may have activated while the password hash ran in a worker.
        if self.passkeys and self.passkeys.password_disabled:
            raise web.HTTPGone(text="请使用通行密钥登录")
        for name in ("operator_session", "__Host-operator_session"):
            self.sessions.pop(request.cookies.get(name), None)
        return self.new_session(request, accepted["username"], accepted["role"])

    async def passkey_route(self, request):
        if not self.passkeys or not self.auth:
            raise web.HTTPServiceUnavailable(text="通行密钥暂不可用")
        if (request.headers.get("Origin") != self.passkeys.origin
                or request.headers.get("X-Operator-Public") != "1"
                or request.headers.get("X-Forwarded-Proto") != "https"):
            raise web.HTTPForbidden(text="请从网站登录页重试")
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType()
        try:
            self.passkeys.throttle(request.headers.get("X-Real-IP", request.remote))
        except OverflowError as error:
            raise web.HTTPTooManyRequests(text=str(error), headers={"Retry-After": "300"}) from None
        value = await request.json()
        if not isinstance(value, dict):
            raise ValueError("请求格式错误")
        action = request.match_info["action"]
        cookie = "__Host-operator_passkey"
        if action in ("register-options", "login-options"):
            try:
                if action == "register-options":
                    flow, options = self.passkeys.registration_options(value.get("token"))
                else:
                    flow, options = self.passkeys.authentication_options()
            except OverflowError as error:
                raise web.HTTPTooManyRequests(text=str(error)) from None
            self.passkeys.pending.pop(request.cookies.get(cookie), None)
            response = web.json_response(options)
            response.set_cookie(cookie, flow, secure=True, httponly=True, samesite="Strict", max_age=300, path="/")
        else:
            credential = value.get("credential")
            if not isinstance(credential, dict):
                raise ValueError("请求格式错误")
            if action == "register":
                self.passkeys.register(request.cookies.get(cookie), credential)
                response = web.json_response({"registered": True})
            else:
                user, activated = self.passkeys.authenticate(request.cookies.get(cookie), credential)
                if activated:
                    self.sessions.clear()
                    self.confirmations.clear()
                else:
                    self.sessions.pop(request.cookies.get("__Host-operator_session"), None)
                response = self.new_session(request, user["username"], user["role"])
            response.del_cookie(cookie, secure=True, httponly=True, samesite="Strict", path="/")
        return response

    async def logout_route(self, request):
        identifier = self.session(request, True)
        self.sessions.pop(identifier, None)
        self.session_store.save(self.sessions,True)
        self.confirmations = {key: value for key, value in self.confirmations.items() if value["session"] != identifier}
        response = web.json_response({"loggedOut": True})
        public = request.headers.get("X-Operator-Public") == "1"
        response.del_cookie("__Host-operator_session" if public else "operator_session", path="/",
                            secure=public, httponly=True, samesite="Strict")
        return response

    async def catalog_route(self, request):
        readonly = self.sessions[self.session(request)]["role"] != "admin"
        tools = [{**tool, "kind": "mcp", "blocked": "此账号为只读权限" if readonly and not tool.get("annotations", {}).get("readOnlyHint") else self.blocked("mcp", tool["name"])} for tool in self.tools.values()]
        routes = [{**item, "name": name, "kind": "rest", "blocked": "此账号为只读权限" if readonly and not name.startswith("GET ") else self.blocked("rest", name)} for name, item in self.routes.items()]
        return web.json_response({"tools": tools, "routes": routes, "nativeMethods": self.native_methods})

    async def profiles_route(self, request):
        self.session(request)
        return web.json_response({"profiles": self.profiles.public(), "epoch": self.profiles.epoch})

    async def profile_test(self, request):
        self.require_operator(request)
        value = await request.json()
        profile = self.profiles.get(value.get('id')) if set(value) <= {'id'} else self.profiles.validate(value)
        if self.live_only and profile.get("mode") == "demo":
            raise ValueError("生产工作台只接受实盘连接")
        return web.json_response(await verify(self.http, profile))

    async def account_route(self, request):
        self.session(request)
        return web.json_response({**self.account, "profiles": self.profiles.public(), "epoch": self.profiles.epoch})

    async def paper_route(self, request):
        self.session(request)
        if self.live_only or self.profiles.get()["mode"] in ("live", "live_readonly"):
            raise web.HTTPNotFound(text="生产工作台不提供模拟运行接口")
        return web.json_response(await asyncio.to_thread(self.paper.snapshot))

    async def artifacts_route(self, request):
        if request.method in {"GET", "HEAD"}:
            self.session(request)
        else:
            self.require_operator(request)
        try:
            if request.method in {"GET", "HEAD"}:
                rows = await asyncio.to_thread(self.artifacts.list, request.query.get("kind"))
                return web.json_response({"artifacts": rows, "limits": {"maxBytes": self.artifacts.max_bytes}})
            if request.content_type != "multipart/form-data":
                raise web.HTTPUnsupportedMediaType(text="请选择上传文件")
            if request.headers.get("Content-Encoding", "identity") != "identity":
                raise ArtifactError("上传请求不支持内容编码")
            # Only this authenticated route accepts larger bodies. Multipart
            # streaming has its own byte limits; JSON routes retain 1 MiB.
            request._client_max_size = self.artifacts.max_bytes + 16 * 1024
            if request.content_length is not None and request.content_length > request._client_max_size:
                raise ArtifactTooLarge("上传请求超过大小限制")
            if self.artifact_upload_lock.locked():
                raise web.HTTPTooManyRequests(text="已有文件正在上传，请稍后重试")
            async with self.artifact_upload_lock, asyncio.timeout(120):
                reader = await request.multipart()
                fields = {}
                for _ in range(4):
                    part = await reader.next()
                    if not isinstance(part, aiohttp.BodyPartReader):
                        raise ArtifactError("上传须包含 kind、name、version 和一个文件")
                    if part.headers.get("Content-Encoding") or part.headers.get("Content-Transfer-Encoding"):
                        raise ArtifactError("上传字段不支持内容编码")
                    if part.name == "file":
                        if set(fields) != {"kind", "name", "version"} or not part.filename:
                            raise ArtifactError("请先提供类型、名称和版本，再上传文件")
                        filename = unquote(part.filename, errors="strict")
                        self.artifacts.metadata(**fields, filename=filename)
                        async def chunks():
                            while chunk := await part.read_chunk(self.artifacts.CHUNK_BYTES):
                                if request.content.total_bytes > request._client_max_size:
                                    raise ArtifactTooLarge("上传请求超过大小限制")
                                yield chunk
                            if await reader.next() is not None:
                                raise ArtifactError("一次只能上传一个文件，文件须为最后一个字段")
                            if request.content.total_bytes > request._client_max_size:
                                raise ArtifactTooLarge("上传请求超过大小限制")
                            self.require_operator(request)
                        result = await self.artifacts.register_async(chunks(), **fields, filename=filename)
                        return web.json_response({"artifact": result}, status=201)
                    if part.name not in {"kind", "name", "version"} or part.name in fields or part.filename:
                        raise ArtifactError("上传包含重复或未知字段")
                    value = bytearray()
                    while chunk := await part.read_chunk(4096):
                        value.extend(chunk)
                        if len(value) > 1024:
                            raise ArtifactTooLarge("上传字段过长")
                    fields[part.name] = value.decode("utf-8")
                raise ArtifactError("上传缺少文件")
        except web.HTTPException:
            raise
        except ArtifactError as error:
            return web.json_response({"error": str(error)}, status=error.status)
        except (ValueError, AssertionError, aiohttp.ClientPayloadError):
            return web.json_response({"error": "上传格式无效"}, status=400)
        except TimeoutError:
            return web.json_response({"error": "上传超时，请重试"}, status=408)
        except Exception:
            # Filesystem paths, SQLite messages and uploaded contents never
            # cross the API boundary through generic exception formatting.
            return web.json_response({"error": "文件库暂不可用"}, status=503)

    async def strategy_groups_route(self, request):
        self.session(request)
        group_id = request.match_info.get("group_id")
        detail_view = request.query.get('view')
        if detail_view not in (None,'summary','market'):
            raise ValueError('策略视图无效')
        scope = 'live' if self.live_only else request.query.get('scope') or ('live' if self.profiles.get()['mode'] in ('live','live_readonly') else 'all')
        if self.live_only and request.query.get('scope') not in (None,'live'):
            raise ValueError('生产工作台只提供实盘运行')
        if scope not in ('all','live','research'):
            raise ValueError('策略范围无效')
        if scope == 'live' and group_id in GroupViews.GROUP_IDS:
            raise web.HTTPNotFound(text='该记录属于研究历史')
        registry = []
        try:
            registry = (await self.group_runtime.status()).get("groups", [])
        except ValueError:
            pass
        if self.live_only:
            registry = [item for item in registry if item.get('mode') == 'live']
        def managed_view(item, run):
            identifier = run["runId"]
            path = Path(run["runDir"])
            if path.name != identifier or self.group_runtime.path is None or path.parent.resolve() != self.group_runtime.path.parent.resolve():
                raise ValueError("运行目录与控制服务不一致")
            if identifier not in self.managed_views:
                from managed_run_view import ManagedRunView
                if len(self.managed_views) >= 16:
                    self.managed_views.pop(next(iter(self.managed_views)))
                self.managed_views[identifier] = GroupViews(path, ManagedRunView(path, expected_group_id=item["groupId"]),
                    group_id=item["groupId"], group_name=item["name"])
            return self.managed_views[identifier]
        def preferred_run(item):
            return next((run for run in item.get("runs", []) if run["runId"] == item.get("runId")), None) or next(iter(item.get("runs", [])), None)
        def registered_group(item):
            return {"id": item["groupId"], "name": item["name"], "description": "已发布运行配置",
                    "mode": item["mode"], "modeLabel": "OKX 实盘" if item["mode"] == "live" else "影子运行" if item["mode"] == "shadow" else "本地模拟",
                    "accountId": item.get("profileId") if item["mode"] == "live" else None, "runId": None,
                    "status": "ready" if item["ready"] else "pending_validation", "observedAt": None,
                    "metrics": {key: None for key in ("capital", "nav", "pnl", "fees", "fills", "returnPct", "maxDrawdownPct")},
                    "capabilities": item["capabilities"], "alpha": [], "version": [], "health": {},
                    **{key: [] for key in ("positions", "orders", "fills", "strategies", "systems", "exceptions", "runtimeConfig")}}
        view = self.group_views
        item = next((item for item in registry if item["groupId"] == group_id), None)
        if not self.live_only and group_id == 'baseline' and request.query.get('runId') == self.args.paper_run.name:
            legacy = await asyncio.to_thread(view.equity if request.path.endswith('/equity') else view.detail, group_id)
            return web.json_response({**legacy,'managed':False,'historical':True})
        default_run = preferred_run(item) if item else None
        run_id = request.query.get("runId") or (default_run["runId"] if default_run else None)
        if run_id:
            if not group_id or not re.fullmatch(r"[a-z][a-z0-9_-]{0,80}", run_id):
                raise ValueError("运行实例标识不正确")
            runtime = {"groups": registry}
            run = next((run for group in runtime.get("groups", []) if group["groupId"] == group_id
                        for run in group.get("runs", []) if run["runId"] == run_id), None)
            if run is None:
                raise web.HTTPNotFound(text="运行实例不存在")
            view = managed_view(item, run)
        try:
            if group_id is None:
                value = ({"groups": []} if view is None else await asyncio.to_thread(view.snapshot))
                listing = {row["id"]: row for row in value["groups"]}
                for registered in registry:
                    if scope != 'all' and not registered.get("dashboardVisible", True):
                        continue
                    chosen = preferred_run(registered)
                    if chosen:
                        detail = await asyncio.to_thread(managed_view(registered, chosen).detail, registered["groupId"])
                        listing[registered["groupId"]] = {**detail, "managed": True, "status": chosen["status"],
                            "activeRunId": registered.get("runId")}
                    elif registered["groupId"] not in listing:
                        listing[registered["groupId"]] = registered_group(registered)
                value["groups"] = [row for row in listing.values()
                    if scope == 'all' or (row.get('mode') == 'live') == (scope == 'live')]
                value['scope'] = scope
                # List consumers render group summaries; full order/fill and
                # position arrays are fetched only for the selected run.
                value['groups']=[{key:item for key,item in row.items() if key not in
                    {'positions','orders','fills','strategies','systems','exceptions','runtimeConfig','universe','alpha','version'}}
                    for row in value['groups']]
            elif not run_id and group_id not in GroupViews.GROUP_IDS:
                item = next((item for item in registry if item["groupId"] == group_id), None)
                if item is None:
                    raise KeyError(group_id)
                value = registered_group(item)
                if request.path.endswith("/equity"):
                    value = {"groupId": group_id, "runId": None, "points": [], "drawdown": [], "sampleCount": 0,
                             "partial": False, "invalidLines": 0, "sourceAvailable": False,
                             "sourceStatus": "unconfigured", "sourceIssue": None}
            elif request.path.endswith("/equity"):
                value = await asyncio.to_thread(view.equity, group_id)
            else:
                value = await asyncio.to_thread(view.detail, group_id)
                if run_id:
                    value.update(managed=True, activeRunId=item.get("runId"), status=run["status"])
        except KeyError:
            raise web.HTTPNotFound(text="策略组不存在") from None
        if group_id and detail_view in {'summary','market'}:
            for key in ('orders','fills','strategies','systems','exceptions','runtimeConfig','version','alpha'):
                value.pop(key,None)
        return web.json_response(value)

    async def model_releases_route(self, request):
        self.session(request)
        release_id = request.match_info.get("release_id")
        if release_id:
            releases = [await asyncio.to_thread(self.model_releases.get, release_id)]
        else:
            releases = await asyncio.to_thread(self.model_releases.list)
        try:
            runtime = await self.group_runtime.status()
            groups = runtime.get("groups", []) if runtime.get("available") is True else []
        except ValueError:
            groups = []
        for release in releases:
            group = next((g for g in groups if g.get("kind") in {"model", "live"}
                          and g.get("releaseId") == release["releaseId"]
                          and g.get("manifestSha256") == release["manifestSha256"]), None)
            if group is not None:
                ready = bool(group.get("ready"))
                release["groupId"] = group["groupId"]
                release["deployReady"] = ready
                release["nodeCompatibility"] = {"ready": ready, "reason": group.get("capabilities", {}).get("reason"),
                    "device": group.get("device", release["manifest"]["runtime"]["device"]),
                    "capabilities": group.get("nodeCapabilities", {}), "nodeId": "local"}
            else:
                release["deployReady"] = False
                release["nodeCompatibility"] = {"ready": False, "reason": "目标节点尚未注册此模型版本", "nodeId": "local"}
        return web.json_response(releases[0] if release_id else {"releases": releases})

    async def query_route(self, request):
        self.session(request, True)
        operation, read_only = self.validate_operation(await request.json())
        if not read_only:
            raise ValueError("写操作必须先生成并确认操作单")
        epoch = self.profiles.epoch
        result = await self.perform(operation)
        if epoch != self.profiles.epoch:
            raise ValueError("查询期间连接发生变化，请重新查询")
        return web.json_response({"result": result, "epoch": epoch})

    async def group_runtime_route(self, request):
        group_id = request.match_info["group_id"]
        if request.method == "POST":
            self.require_operator(request)
            values = await request.json()
            if not isinstance(values, dict):
                raise ValueError("配置必须是 JSON 对象")
            operation, _ = self.validate_operation({"kind": "group", "name": "start",
                "arguments": {**values, "groupId": group_id}})
            preview = await self.group_runtime.prepare(operation["arguments"])
            return web.json_response(preview)
        self.session(request)
        try:
            result = await self.group_runtime.status(group_id)
        except ValueError as error:
            result = {"available": False, "groups": [], "reason": str(error)}
        for group in result.get("groups", []):
            for run in group.get("runs", []):
                run.pop("runDir", None)
            pending=self.database.execute("SELECT id,at,parameters,owner FROM operations WHERE action='group:start' AND status='processing' ORDER BY at DESC LIMIT 100").fetchall()
            for identifier,at,parameters,owner in pending:
                if json.loads(parameters).get('groupId')==group_id:
                    group['pendingStart']={'at':at,'phase':'preparing',**({'operationId':identifier} if owner==self.sessions[self.session(request)].get('operator') else {})}
                    group['capabilities']['start']=False
                    group['capabilities']['reason']='启动请求正在处理'
                    break
        return web.json_response(redact(result))

    async def prepare_route(self, request):
        session_id = self.require_operator(request)
        operation, read_only = self.validate_operation(await request.json())
        if read_only:
            raise ValueError("读取操作无需确认单")
        if operation["kind"] == "mcp" and operation["name"] == "spot_place_order":
            operation["arguments"].setdefault("clOrdId", "NO" + secrets.token_hex(12))
        if operation["kind"] == "rest" and operation["name"] == "POST /api/v5/trade/order":
            operation["arguments"].setdefault("clOrdId", "NO" + secrets.token_hex(12))
        now = time.time()
        self.confirmations = {key: value for key, value in self.confirmations.items() if value["expires"] > now}
        if len(self.confirmations) >= 100:
            raise ValueError("待确认操作过多，请先完成或等待过期")
        verification = None
        group_preview = None
        model_preview = None
        if operation["kind"] == "model_release":
            if operation["name"] == "validate":
                model_preview = await asyncio.to_thread(self.model_releases.validate, operation["arguments"]["artifactId"])
            else:
                model_preview = await asyncio.to_thread(self.model_releases.prepare, operation["name"], operation["arguments"])
                operation["arguments"] = model_preview["request"]
        if operation["kind"] == "group":
            group_preview = await self.group_runtime.prepare(operation["arguments"])
            if (operation['name'] == 'start' and self.profiles.get()['mode'] in ('live','live_readonly')
                    and group_preview.get('mode') != 'live'):
                raise ValueError('此配置用于历史研究，请选择实盘策略')
            operation["arguments"] = group_preview["request"]
        if operation["kind"] == "profile" and operation["name"] in ("save", "select"):
            profile = operation["arguments"] if operation["name"] == "save" else self.profiles.get(operation["arguments"]["id"])
            verification = await verify(self.http, profile)
        now = time.time()
        identifier = secrets.token_urlsafe(24)
        self.confirmations[identifier] = {"session": session_id, "operation": operation, "expires": now + 90, "epoch": self.profiles.epoch}
        return web.json_response({"id": identifier, "expiresAt": now + 90, "profile": self.profiles.public(), "operation": redact(operation), "newAccountVerification": verification, "groupPreview": redact(group_preview), "modelPreview": redact(model_preview)})

    async def execute_route(self, request):
        session_id = self.require_operator(request)
        payload=await request.json()
        identifier = payload.get("id", "")
        previous=self.database.execute('SELECT status,result,session FROM operations WHERE id=?',(identifier,)).fetchone()
        if previous:
            if previous[2]!=session_id:raise web.HTTPForbidden(text='操作回执不属于当前会话')
            return web.json_response({'id':identifier,'status':previous[0],'result':json.loads(previous[1]),'replayed':True})
        async with self.write_lock:
            previous = self.database.execute("SELECT status,result,session FROM operations WHERE id=?", (identifier,)).fetchone()
            if previous:
                if previous[2] != session_id:
                    raise web.HTTPForbidden(text="操作回执不属于当前会话")
                return web.json_response({"id": identifier, "status": previous[0], "result": json.loads(previous[1]), "replayed": True})
            ticket = self.confirmations.get(identifier)
            if not ticket or ticket["session"] != session_id or ticket["expires"] < time.time():
                raise ValueError("确认单已过期或不属于当前会话")
            if ticket["epoch"] != self.profiles.epoch:
                raise ValueError("API 连接已改变，请重新确认操作")
            operation, _ = self.validate_operation(ticket["operation"])
            self.confirmations.pop(identifier)
            self.database.execute("INSERT INTO operations (id,at,profile,action,parameters,status,result,session,owner) VALUES (?,?,?,?,?,?,?,?,?)",
                (identifier, time.time(), self.profiles.get()["id"], f"{operation['kind']}:{operation['name']}",
                 json.dumps(redact(operation["arguments"])), "processing", "null", session_id,
                 self.sessions[session_id].get("operator")))
            self.database.commit()
            owner=self.sessions[session_id]['operator']
            if payload.get('async') is True and operation['kind']=='group' and operation['name']=='start':
                task=asyncio.create_task(self.finish_start(operation,identifier,owner,ticket['epoch']))
                self.pending_starts.add(task)
                task.add_done_callback(self.pending_starts.discard)
                return web.json_response({'id':identifier,'status':'processing','result':{
                    'groupId':operation['arguments']['groupId'],'receiptStatus':'processing','phase':'preparing'}},status=202)
            return web.json_response(await self.perform_confirmed(operation,identifier,owner))

    async def finish_start(self, operation, identifier, owner, epoch):
        async with self.write_lock:
            if epoch!=self.profiles.epoch:
                self.database.execute("UPDATE operations SET status='error',result=? WHERE id=?",(json.dumps({'error':'API 连接已变化，请重新确认'}),identifier))
                self.database.commit()
                return
            await self.perform_confirmed(operation,identifier,owner)

    async def perform_confirmed(self, operation, identifier, owner):
            try:
                if operation["kind"] == "group":
                    result = await self.group_runtime.execute(operation["arguments"], identifier)
                    receipt = result.get("receiptStatus", result.get("status"))
                    status = "unknown" if receipt in {"unknown", "processing"} else "error" if receipt == "failed" else "completed"
                elif operation["kind"] == "model_release":
                    if operation["name"] == "validate":
                        result = await asyncio.to_thread(self.model_releases.validate, operation["arguments"]["artifactId"])
                    else:
                        result = await asyncio.to_thread(self.model_releases.execute, operation["name"], operation["arguments"],
                            identifier, owner)
                    status = "completed"
                else:
                    result = await self.perform(operation)
                    status = "error" if isinstance(result, dict) and result.get("isError") else "completed"
            except Exception as error:
                result, status = {"error": str(error)}, "unknown" if isinstance(error, (TimeoutError, aiohttp.ClientError)) or "未知" in str(error) else "error"
            self.database.execute("UPDATE operations SET status=?,result=? WHERE id=?", (status, json.dumps(redact(result)), identifier))
            self.database.commit()
            return {"id": identifier, "status": status, "result": redact(result)}

    async def group_receipt_route(self, request):
        """Read a saved confirmation result; never submit/retry its command."""
        session_id = self.session(request)
        identifier = request.match_info["operation_id"]
        row = self.database.execute("SELECT status,result,session,action,owner FROM operations WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise web.HTTPNotFound(text="操作回执不存在")
        # New sessions may read only their own stable operator identity. Legacy
        # receipts without an owner retain their original session restriction.
        if row[4] is not None and row[4] != self.sessions[session_id].get("operator") or row[4] is None and row[2] != session_id:
            raise web.HTTPForbidden(text="操作回执不属于当前会话")
        status, result = row[0], json.loads(row[1])
        if row[3].startswith("model_release:") and status in {"unknown", "processing"}:
            try:
                saved = await asyncio.to_thread(self.model_releases.receipt, identifier)
            except ValueError:
                saved = None
            if saved is not None:
                result, status = saved, "completed"
                self.database.execute("UPDATE operations SET status=?,result=? WHERE id=?",
                                      (status, json.dumps(redact(result)), identifier))
                self.database.commit()
        if row[3].startswith("group:") and status in {"unknown", "processing"}:
            try:
                result = await self.group_runtime.receipt(identifier)
                receipt = result.get("receiptStatus", result.get("status"))
                status = "completed" if receipt == "completed" else "error" if receipt == "failed" else "unknown"
                self.database.execute("UPDATE operations SET status=?,result=? WHERE id=?",
                                      (status, json.dumps(redact(result)), identifier))
                self.database.commit()
            except (ValueError, TimeoutError, OSError):
                # A temporarily unavailable worker does not change a receipt
                # into a failure and must not cause an automatic retry.
                status = "unknown"
        return web.json_response({"id": identifier, "status": status, "result": redact(result)})

    async def history_route(self, request):
        self.session(request)
        fields = ["id", "at", "profile", "action", "parameters", "status", "result"]
        rows = self.database.execute("SELECT id,at,profile,action,parameters,status,result FROM operations ORDER BY at DESC LIMIT 200").fetchall()
        return web.json_response({"operations": [dict(zip(fields, row)) for row in rows]})

    async def account_loop(self):
        while True:
            epoch = self.profiles.epoch
            try:
                profile = self.profiles.get()
                balances, orders, fills = await asyncio.gather(
                    rest(self.http, profile, "GET", "/api/v5/account/balance"),
                    rest(self.http, profile, "GET", "/api/v5/trade/orders-pending", {"instType": "SPOT"}),
                    rest(self.http, profile, "GET", "/api/v5/trade/fills", {"instType": "SPOT", "limit": 100}),
                )
                if epoch == self.profiles.epoch:
                    self.account.update(available=True, profileId=profile["id"], mode=profile["mode"], updatedAt=time.time(),
                                        balances=balances["data"][0].get("details", []), orders=orders["data"], fills=fills["data"], error=None)
                    self.account_changed.set()
            except Exception as error:
                if epoch == self.profiles.epoch:
                    self.account.update(error=str(error))
            await asyncio.sleep(5)

    async def private_loop(self):
        while True:
            epoch = self.profiles.epoch
            try:
                profile = self.profiles.get()
                if profile["site"] != "global":
                    await asyncio.sleep(10)
                    continue
                host = "wspap.okx.com" if profile["mode"] == "demo" else "ws.okx.com"
                async with self.http.ws_connect(f"wss://{host}:8443/ws/v5/private", heartbeat=15, timeout=15) as socket:
                    timestamp = str(int(time.time()))
                    signature = base64.b64encode(hmac.new(profile["secret"].encode(), (timestamp + "GET/users/self/verify").encode(), hashlib.sha256).digest()).decode()
                    await socket.send_json({"op": "login", "args": [{"apiKey": profile["apiKey"], "passphrase": profile["passphrase"], "timestamp": timestamp, "sign": signature}]})
                    while epoch == self.profiles.epoch:
                        try:
                            message = await asyncio.wait_for(socket.receive(), 10)
                        except TimeoutError:
                            await socket.send_str("ping")
                            continue
                        if message.type != aiohttp.WSMsgType.TEXT:
                            break
                        if message.data == "pong":
                            continue
                        if epoch != self.profiles.epoch:
                            break
                        value = json.loads(message.data)
                        if value.get("event") == "login":
                            if value.get("code") != "0":
                                raise RuntimeError(f"私有 WS：{value.get('msg')}")
                            await socket.send_json({"op": "subscribe", "args": [{"channel": "account"}, {"channel": "orders", "instType": "SPOT"}]})
                            self.account.update(privateConnected=True)
                            self.account_changed.set()
                        channel = value.get("arg", {}).get("channel")
                        if channel == "account" and epoch == self.profiles.epoch:
                            current = {item["ccy"]: item for item in self.account["balances"]}
                            for row in value.get("data", []):
                                current.update({item["ccy"]: item for item in row.get("details", [])})
                            self.account.update(balances=list(current.values()), available=True, mode=profile["mode"], profileId=profile["id"], updatedAt=time.time(), privateEventAt=time.time())
                            self.account_changed.set()
                        elif channel == "orders":
                            self.account["privateEventAt"] = time.time()
                            current = {item["ordId"]: item for item in self.account["orders"]}
                            for order in value.get("data", []):
                                if order.get("state") in ("live", "partially_filled"):
                                    current[order["ordId"]] = order
                                else:
                                    current.pop(order["ordId"], None)
                            self.account["orders"] = list(current.values())
                            self.account_changed.set()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.account["privateError"] = str(error)
            if epoch == self.profiles.epoch:
                self.account["privateConnected"] = False
                self.account_changed.set()
            await asyncio.sleep(3)

    async def events_route(self, request):
        self.session(request)
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-store", "X-Accel-Buffering": "no"})
        await response.prepare(request)
        try:
            while True:
                try:
                    self.session(request)
                except web.HTTPUnauthorized:
                    break
                await response.write(("data: " + json.dumps({"account": self.account, "epoch": self.profiles.epoch, "serverTime": time.time()}) + "\n\n").encode())
                self.account_changed.clear()
                try:
                    await asyncio.wait_for(self.account_changed.wait(), timeout=1)
                except TimeoutError:
                    pass
        except (ConnectionResetError, asyncio.CancelledError):
            return response
        return response

    async def candles_route(self, request):
        self.session(request)
        instrument, bar, mode = self.market_parameters(request)
        profile = {**self.profiles.get(), "mode": mode, "site": "global"}
        return web.json_response(await rest(self.http, profile, "GET", "/api/v5/market/candles", {"instId": instrument, "bar": bar, "limit": 300}))

    async def market_snapshot_route(self, request):
        self.session(request)
        instrument, _, mode = self.market_parameters(request)
        key = (instrument, mode)
        async with self.snapshot_lock:
            cached = self.market_snapshots.get(key)
            if cached and time.monotonic() - cached[0] < 1:
                return web.json_response(cached[1])
            profile = {**self.profiles.get(), "mode": mode, "site": "global"}
            ticker, book, trades = await asyncio.gather(
                rest(self.http, profile, "GET", "/api/v5/market/ticker", {"instId": instrument}),
                rest(self.http, profile, "GET", "/api/v5/market/books", {"instId": instrument, "sz": 20}),
                rest(self.http, profile, "GET", "/api/v5/market/trades", {"instId": instrument, "limit": 60}),
            )
            value = {"ticker": ticker["data"][0], "book": book["data"][0], "trades": trades["data"], "receivedAt": time.time(), "source": mode}
            self.market_snapshots = {k: v for k, v in self.market_snapshots.items() if time.monotonic() - v[0] < 5}
            self.market_snapshots[key] = (time.monotonic(), value)
            return web.json_response(value)

    async def host_route(self, request):
        self.session(request)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/www" if Path("/www").is_dir() else "/")
        panel = False
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", 21981), 1)
            writer.close()
            await writer.wait_closed()
            panel = True
        except (OSError, TimeoutError):
            pass
        return web.json_response({"at": time.time(), "cpu": psutil.cpu_percent(), "cores": psutil.cpu_count(),
                                  "memoryPercent": memory.percent, "memoryUsed": memory.used, "memoryTotal": memory.total,
                                  "diskPercent": disk.percent, "diskUsed": disk.used, "diskTotal": disk.total,
                                  "uptime": time.time() - psutil.boot_time(), "panelReachable": panel,
                                  "panelUrl": "https://64.83.36.66:21981/"})

    def market_parameters(self, request):
        instrument = request.query.get("instrument", "BTC-USDT")
        bar = request.query.get("bar", "1m")
        mode = request.query.get("mode", "live")
        if (not re.fullmatch(r"[A-Z0-9]{1,20}-USDT(?:-SWAP)?", instrument)
                or bar not in ("1m", "5m", "15m", "1H", "4H", "1D")
                or mode not in (("live",) if self.live_only else ("live", "demo"))):
            raise ValueError("不支持的品种、周期或行情环境")
        return instrument, bar, mode

    async def stream_route(self, request):
        self.session(request)
        instrument, bar, mode = self.market_parameters(request)
        client = web.WebSocketResponse(heartbeat=15)
        await client.prepare(request)
        host = "wspap.okx.com" if mode == "demo" else "ws.okx.com"
        async def relay(endpoint, arguments):
            async with self.http.ws_connect(f"wss://{host}:8443/ws/v5/{endpoint}", heartbeat=15, timeout=15) as upstream:
                await upstream.send_json({"op": "subscribe", "args": arguments})
                while not client.closed:
                    try:
                        message = await asyncio.wait_for(upstream.receive(), 20)
                    except TimeoutError:
                        await upstream.send_str("ping")
                        continue
                    if message.type != aiohttp.WSMsgType.TEXT:
                        break
                    if message.data != "pong":
                        await client.send_json({"source": mode, "receivedAt": time.time(), "payload": json.loads(message.data)})
        tasks = [asyncio.create_task(relay("public", [{"channel": name, "instId": instrument} for name in ("books5", "trades", "tickers")])),
                 asyncio.create_task(relay("business", [{"channel": "candle" + bar, "instId": instrument}]))]
        async def consume_client():
            async for _ in client:
                pass
        tasks.append(asyncio.create_task(consume_client()))
        async def session_watch():
            while not client.closed:
                await asyncio.sleep(1)
                try:
                    self.session(request)
                except web.HTTPUnauthorized:
                    return
        tasks.append(asyncio.create_task(session_watch()))
        try:
            finished, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in finished:
                task.result()
        except (aiohttp.ClientError, TimeoutError, RuntimeError):
            if not client.closed:
                await client.send_json({"source": mode, "receivedAt": time.time(), "payload": {"event": "error", "msg": "行情通道中断，正在重连"}})
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.close()
        return client

    async def static_route(self, request):
        relative = request.match_info.get("path", "")
        if relative == "api" or relative.startswith("api/"):
            raise web.HTTPNotFound(text="接口不存在")
        path = (self.args.static_dir / relative).resolve()
        if not path.is_relative_to(self.args.static_dir.resolve()):
            raise web.HTTPForbidden()
        if not path.is_file():
            path = self.args.static_dir / "index.html"
        response = web.FileResponse(path)
        fingerprinted = bool(re.search(r"\.[0-9a-f]{8,}\.(?:async\.)?(?:js|css)$", path.name))
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable" if fingerprinted else "no-store"
        return response

    async def lifecycle(self, app):
        self.http = aiohttp.ClientSession()
        tasks = [asyncio.create_task(self.account_loop()), asyncio.create_task(self.private_loop())]
        yield
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.mcp.close()
        await self.http.close()
        self.database.close()
        if self.passkeys:
            self.passkeys.database.close()

    def application(self):
        @web.middleware
        async def boundary(request, handler):
            host = request.host.split(":")[0]
            origin = request.headers.get("Origin")
            if host not in ("localhost", "127.0.0.1") or (origin and origin not in self.args.allowed_origins):
                raise web.HTTPForbidden(text="仅允许本地操作入口")
            if request.headers.get("X-Operator-Public") == "1" and (not self.auth or request.headers.get("X-Forwarded-Proto") != "https"):
                raise web.HTTPServiceUnavailable(text="HTTPS 登录入口尚未就绪")
            try:
                response = await handler(request)
                if request.path.startswith("/api/"):
                    response.headers["Cache-Control"] = "no-store"
                return response
            except web.HTTPException:
                raise
            except (ValueError, KeyError, jsonschema.ValidationError) as error:
                return web.json_response({"error": error.message if isinstance(error, jsonschema.ValidationError) else str(error)}, status=400)
            except Exception as error:
                return web.json_response({"error": str(error)}, status=502)
        app = web.Application(middlewares=[boundary], client_max_size=1_048_576)
        async def shutdown_sessions(app):
            # Close long-lived SSE/WSS promptly; already executing confirmations may finish.
            if self.pending_starts:
                await asyncio.gather(*self.pending_starts,return_exceptions=True)
            self.session_store.save(self.sessions,True)
            self.sessions.clear()
            self.confirmations.clear()
        async def security_headers(request, response):
            if request.path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-store"
        app.on_shutdown.append(shutdown_sessions)
        app.on_response_prepare.append(security_headers)
        app.cleanup_ctx.append(self.lifecycle)
        app.add_routes([web.post("/api/login", self.login_route), web.post("/api/logout", self.logout_route)])
        app.add_routes([web.post("/api/passkeys/{action:register-options|register|login-options|login}", self.passkey_route)])
        app.add_routes([web.get("/api/market/snapshot", self.market_snapshot_route), web.get("/api/host", self.host_route)])
        app.add_routes([web.get("/api/artifacts", self.artifacts_route), web.post("/api/artifacts", self.artifacts_route)])
        app.add_routes([web.get("/api/strategy-groups", self.strategy_groups_route),
                        web.get("/api/model-releases", self.model_releases_route),
                        web.get("/api/model-releases/{release_id}", self.model_releases_route),
                        web.get("/api/strategy-groups/{group_id}/runtime", self.group_runtime_route),
                        web.post("/api/strategy-groups/{group_id}/preflight", self.group_runtime_route),
                        web.get("/api/strategy-groups/{group_id}", self.strategy_groups_route),
                        web.get("/api/strategy-groups/{group_id}/equity", self.strategy_groups_route)])
        if not self.live_only:
            app.router.add_get("/api/paper", self.paper_route)
        app.add_routes([web.get("/api/session", self.session_route), web.get("/api/catalog", self.catalog_route),
            web.get("/api/profiles", self.profiles_route), web.post("/api/profiles/test", self.profile_test),
            web.get("/api/account", self.account_route),
            web.post("/api/query", self.query_route), web.post("/api/prepare", self.prepare_route),
            web.post("/api/execute", self.execute_route), web.get("/api/history", self.history_route),
            web.get("/api/operations/{operation_id}", self.group_receipt_route),
            web.get("/api/events", self.events_route), web.get('/api/market/watchlist',self.watch_market_route), web.get("/api/market/candles", self.candles_route),
            web.get("/api/market/stream", self.stream_route), web.get("/{path:.*}", self.static_route)])
        return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--credentials", type=Path)
    parser.add_argument("--auth-file", type=Path, help="Required for public HTTPS; absence permits only SSH-loopback sessions")
    parser.add_argument("--passkey-origin", help="Exact HTTPS origin; requires an initialized state/passkeys.sqlite")
    parser.add_argument("--catalog", type=Path, default=Path(__file__).parent / "catalog")
    parser.add_argument("--static-dir", type=Path, required=True)
    parser.add_argument("--paper-run", type=Path)
    parser.add_argument("--group-socket", type=Path, help="UNIX socket of the independent strategy group supervisor")
    parser.add_argument("--paper-bridge", type=Path)
    parser.add_argument("--live-only", action="store_true")
    parser.add_argument("--node", required=True)
    parser.add_argument("--mcp", required=True)
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--allowed-origins", nargs="+", default=["http://127.0.0.1:18080", "http://127.0.0.1:18081", "http://127.0.0.1:18082", "http://localhost:18081", "http://localhost:18082"])
    arguments = parser.parse_args()
    if not arguments.live_only and (arguments.paper_run is None or arguments.paper_bridge is None):
        parser.error("paper-run and paper-bridge are required unless --live-only is used")
    web.run_app(Operator(arguments).application(), host="127.0.0.1", port=arguments.port,
                access_log=None, shutdown_timeout=40)
