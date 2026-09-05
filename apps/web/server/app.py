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

import aiohttp
import jsonschema
import psutil
from aiohttp import web

from auth import Authentication
from exchange import MCP, rest, verify
from profiles import Profiles


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
        self.mcp = MCP(args.node, args.mcp)
        self.sessions = {}
        self.auth = Authentication(args.auth_file) if getattr(args, "auth_file", None) else None
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
        self.account = {"available": False, "privateConnected": False, "balances": [], "orders": [], "fills": []}
        self.last_account_epoch = -1
        self.market_snapshots = {}
        self.snapshot_lock = asyncio.Lock()
        self.account_changed = asyncio.Event()
        spec = importlib.util.spec_from_file_location("paper_view", args.paper_bridge)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.paper = module.RunView(args.paper_run)

    def session(self, request, write=False):
        public = request.headers.get("X-Operator-Public") == "1"
        session_id = request.cookies.get("__Host-operator_session" if public else "operator_session")
        entry = self.sessions.get(session_id)
        if not entry or entry["expires"] < time.time() or time.time() - entry["lastSeen"] > 1800 or entry["public"] != public:
            raise web.HTTPUnauthorized(text="请登录交易操作台")
        entry["lastSeen"] = time.time()
        if write and not secrets.compare_digest(request.headers.get("X-Operator-CSRF", ""), entry["csrf"]):
            raise web.HTTPForbidden(text="操作会话校验失败")
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
        if profile["mode"] != "demo":
            return "当前实盘连接只读"
        if re.search(r"set.leverage|borrow|flexible-loan|adjust.leverage", name, re.I):
            return "当前策略约束：禁止杠杆和借贷"
        if re.match(r"(swap|futures|option)_(place|batch_orders)", name):
            return "当前仅启用现货交易；衍生品下单未启用"
        if name == "skills_download":
            return "技能安装需要主机级流程，此账户操作面板仅提供查询"
        if arguments.get("tdMode", "cash") != "cash" and ("/trade/" in name or name.startswith("spot_")):
            return "当前仅允许现货 cash 模式"
        instrument = str(arguments.get("instId", ""))
        if is_write and "/trade/" in name and (instrument.endswith("SWAP") or len(instrument.split("-")) > 2):
            return "当前仅启用现货交易"
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
            if arguments.get("runId") != self.args.paper_run.name:
                raise ValueError("运行实例已变化，请刷新后重试")
            read_only = name != "stop"
        elif kind == "profile" and name in ("save", "select", "delete"):
            if name == "save":
                arguments = self.profiles.validate(arguments)
            else:
                self.profiles.get(arguments.get("id"))
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
        response = web.json_response({"csrf": csrf, "operator": operator, "role": role, "mode": self.profiles.get()["mode"]})
        response.set_cookie("__Host-operator_session" if public else "operator_session", identifier,
                            secure=public, httponly=True, samesite="Strict", max_age=8 * 3600, path="/")
        return response

    async def login_route(self, request):
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
        for name in ("operator_session", "__Host-operator_session"):
            self.sessions.pop(request.cookies.get(name), None)
        return self.new_session(request, accepted["username"], accepted["role"])

    async def logout_route(self, request):
        identifier = self.session(request, True)
        self.sessions.pop(identifier, None)
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
        profile = self.profiles.validate(value) if any(value.get(key) for key in ("apiKey", "secret", "passphrase")) else self.profiles.get(value.get("id"))
        return web.json_response(await verify(self.http, profile))

    async def account_route(self, request):
        self.session(request)
        return web.json_response({**self.account, "profiles": self.profiles.public(), "epoch": self.profiles.epoch})

    async def paper_route(self, request):
        self.session(request)
        return web.json_response(await asyncio.to_thread(self.paper.snapshot))

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
        if operation["kind"] == "profile" and operation["name"] in ("save", "select"):
            profile = operation["arguments"] if operation["name"] == "save" else self.profiles.get(operation["arguments"]["id"])
            verification = await verify(self.http, profile)
        identifier = secrets.token_urlsafe(24)
        self.confirmations[identifier] = {"session": session_id, "operation": operation, "expires": now + 90, "epoch": self.profiles.epoch}
        return web.json_response({"id": identifier, "expiresAt": now + 90, "profile": self.profiles.public(), "operation": redact(operation), "newAccountVerification": verification})

    async def execute_route(self, request):
        session_id = self.require_operator(request)
        identifier = (await request.json()).get("id", "")
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
            self.database.execute("INSERT INTO operations VALUES (?,?,?,?,?,?,?,?)", (identifier, time.time(), self.profiles.get()["id"], f"{operation['kind']}:{operation['name']}", json.dumps(redact(operation["arguments"])), "processing", "null", session_id))
            self.database.commit()
            try:
                result = await self.perform(operation)
                status = "error" if isinstance(result, dict) and result.get("isError") else "completed"
            except Exception as error:
                result, status = {"error": str(error)}, "unknown" if isinstance(error, (TimeoutError, aiohttp.ClientError)) or "未知" in str(error) else "error"
            self.database.execute("UPDATE operations SET status=?,result=? WHERE id=?", (status, json.dumps(redact(result)), identifier))
            self.database.commit()
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
        if not re.fullmatch(r"[A-Z0-9]{1,20}-USDT", instrument) or bar not in ("1m", "5m", "15m", "1H", "4H", "1D") or mode not in ("live", "demo"):
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
            self.sessions.clear()
            self.confirmations.clear()
        async def security_headers(request, response):
            if request.path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-store"
        app.on_shutdown.append(shutdown_sessions)
        app.on_response_prepare.append(security_headers)
        app.cleanup_ctx.append(self.lifecycle)
        app.add_routes([web.post("/api/login", self.login_route), web.post("/api/logout", self.logout_route)])
        app.add_routes([web.get("/api/market/snapshot", self.market_snapshot_route), web.get("/api/host", self.host_route)])
        app.add_routes([web.get("/api/session", self.session_route), web.get("/api/catalog", self.catalog_route),
            web.get("/api/profiles", self.profiles_route), web.post("/api/profiles/test", self.profile_test),
            web.get("/api/account", self.account_route), web.get("/api/paper", self.paper_route),
            web.post("/api/query", self.query_route), web.post("/api/prepare", self.prepare_route),
            web.post("/api/execute", self.execute_route), web.get("/api/history", self.history_route),
            web.get("/api/events", self.events_route), web.get("/api/market/candles", self.candles_route),
            web.get("/api/market/stream", self.stream_route), web.get("/{path:.*}", self.static_route)])
        return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--credentials", type=Path)
    parser.add_argument("--auth-file", type=Path, help="Required for public HTTPS; absence permits only SSH-loopback sessions")
    parser.add_argument("--catalog", type=Path, default=Path(__file__).parent / "catalog")
    parser.add_argument("--static-dir", type=Path, required=True)
    parser.add_argument("--paper-run", type=Path, required=True)
    parser.add_argument("--paper-bridge", type=Path, required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--mcp", required=True)
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--allowed-origins", nargs="+", default=["http://127.0.0.1:18080", "http://127.0.0.1:18081", "http://127.0.0.1:18082", "http://localhost:18081", "http://localhost:18082"])
    arguments = parser.parse_args()
    web.run_app(Operator(arguments).application(), host="127.0.0.1", port=arguments.port,
                access_log=None, shutdown_timeout=40)
