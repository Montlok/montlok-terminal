"""Official OKX MCP transport and fixed-host REST access."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import aiohttp

from profiles import SITES


def headers(profile: dict, method: str, path: str, body: str = "") -> dict:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    signature = base64.b64encode(hmac.new(profile["secret"].encode(), (timestamp + method + path + body).encode(), hashlib.sha256).digest()).decode()
    value = {"OK-ACCESS-KEY": profile["apiKey"], "OK-ACCESS-SIGN": signature,
             "OK-ACCESS-TIMESTAMP": timestamp, "OK-ACCESS-PASSPHRASE": profile["passphrase"],
             "Content-Type": "application/json", "User-Agent": "Nautilus-Operator/1.0"}
    if profile["mode"] == "demo":
        value["x-simulated-trading"] = "1"
    if method != "GET":
        value["expTime"] = str(int(time.time() * 1000) + 10000)
    return value


async def rest(session: aiohttp.ClientSession, profile: dict, method: str, path: str, parameters: dict | list | None = None) -> dict:
    parameters = {} if parameters is None else parameters
    if method == "GET" and parameters:
        path += "?" + urlencode({key: str(value).lower() if isinstance(value, bool) else value for key, value in parameters.items()}, doseq=True)
    body = json.dumps(parameters, separators=(",", ":")) if method != "GET" else ""
    async with session.request(method, SITES[profile["site"]] + path, data=body or None,
                               headers=headers(profile, method, path, body), allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=20)) as response:
        try:
            value = await response.json(content_type=None)
        except (ValueError, TypeError):
            raise RuntimeError(f"OKX HTTP {response.status}：未返回 JSON") from None
        if response.status >= 400 or value.get("code") != "0":
            raise RuntimeError(f"OKX {value.get('code', response.status)}：{value.get('msg', '请求失败')}")
        return value


async def verify(session: aiohttp.ClientSession, profile: dict) -> dict:
    start = time.monotonic()
    account = (await rest(session, profile, "GET", "/api/v5/account/config"))["data"][0]
    result = {"checkedAt": time.time(), "restMs": round((time.monotonic() - start) * 1000, 1),
            "uid": account.get("uid"), "mainUid": account.get("mainUid"), "label": account.get("label"),
            "permissions": account.get("perm"), "accountLevel": account.get("acctLv"),
            "positionMode": account.get("posMode"), "autoLoan": account.get("autoLoan"),
            "ipBound": bool(account.get("ip")), "environment": profile["mode"]}
    if profile["site"] == "global":
        start = time.monotonic()
        host = "wspap.okx.com" if profile["mode"] == "demo" else "ws.okx.com"
        async with session.ws_connect(f"wss://{host}:8443/ws/v5/private", timeout=15) as socket:
            timestamp = str(int(time.time()))
            signature = base64.b64encode(hmac.new(profile["secret"].encode(), (timestamp + "GET/users/self/verify").encode(), hashlib.sha256).digest()).decode()
            await socket.send_json({"op": "login", "args": [{"apiKey": profile["apiKey"], "passphrase": profile["passphrase"], "timestamp": timestamp, "sign": signature}]})
            message = await asyncio.wait_for(socket.receive_json(), 15)
            if message.get("event") != "login" or message.get("code") != "0":
                raise ValueError("私有 WebSocket 认证失败：" + str(message.get("msg", "未知响应")))
            result.update(wsMs=round((time.monotonic() - start) * 1000, 1), wsAuthenticated=True)
    else:
        result.update(wsAuthenticated=False, wsNote="该区域当前已验证 REST；私有 WS 适配尚未启用")
    return result


class MCP:
    def __init__(self, node: str, entry: str) -> None:
        self.node, self.entry = node, entry
        self.process = None
        self.epoch = -1
        self.lock = asyncio.Lock()
        self.identifier = 0

    async def close(self) -> None:
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        self.process = None

    async def rpc(self, method: str, parameters: dict) -> dict:
        self.identifier += 1
        identifier = self.identifier
        self.process.stdin.write((json.dumps({"jsonrpc": "2.0", "id": identifier, "method": method, "params": parameters}) + "\n").encode())
        await self.process.stdin.drain()
        async with asyncio.timeout(35):
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    raise RuntimeError("官方 OKX MCP 进程已退出")
                value = json.loads(line)
                if value.get("id") == identifier:
                    if "error" in value:
                        raise RuntimeError(str(value["error"].get("message")))
                    return value["result"]

    async def call(self, profile: dict, epoch: int, name: str, arguments: dict) -> dict:
        async with self.lock:
            if not self.process or self.process.returncode is not None or self.epoch != epoch:
                await self.close()
                env = {key: value for key, value in os.environ.items() if not key.startswith("OKX_")}
                env.update(OKX_API_KEY=profile["apiKey"], OKX_SECRET_KEY=profile["secret"], OKX_PASSPHRASE=profile["passphrase"], OKX_API_BASE_URL=SITES[profile["site"]])
                flags = ["--demo"] if profile["mode"] == "demo" else ["--live", "--read-only"]
                self.process = await asyncio.create_subprocess_exec(self.node, self.entry, "--modules", "all", "--site", profile["site"], "--no-log", *flags,
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, env=env, limit=4_194_304)
                await self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "nautilus-operator", "version": "1.0"}})
                self.process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
                await self.process.stdin.drain()
                self.epoch = epoch
            try:
                result = await self.rpc("tools/call", {"name": name, "arguments": arguments})
            except (TimeoutError, BrokenPipeError, ConnectionError):
                await self.close()
                raise RuntimeError("请求结果未知，请先查询订单/回执，不要重复提交") from None
            return result
