"""Read-only TLS, session, SSE and WSS acceptance check against this host's Nginx."""
import asyncio
import json
import socket
from http.cookies import SimpleCookie
from pathlib import Path

import aiohttp


class LocalResolver(aiohttp.abc.AbstractResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        return [{"hostname": host, "host": "127.0.0.1", "port": port, "family": family,
                 "proto": 0, "flags": socket.AI_NUMERICHOST}]

    async def close(self):
        pass


async def main():
    lines = Path("/www/nautilus/operator/identity/login-handoff.txt").read_text().splitlines()
    credentials = {"username": lines[1].split(": ", 1)[1], "password": lines[2].split(": ", 1)[1]}
    base = "https://tokyo.montlok.com"
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=LocalResolver()),
                                    timeout=aiohttp.ClientTimeout(total=35)) as client:
        async with client.get(base + "/api/session") as response:
            assert response.status == 401, response.status
            print("TLS chain/hostname verified; anonymous session rejected (401)")
        async with client.get(base + "/api/account") as response:
            assert response.status == 401
            assert response.headers.get("Strict-Transport-Security")
        async with client.post(base + "/api/login", json=credentials, headers={"Origin": base}) as response:
            assert response.status == 200, await response.text()
            session = await response.json()
            cookie = SimpleCookie(response.headers["Set-Cookie"])["__Host-operator_session"]
            assert cookie["secure"] and cookie["httponly"] and cookie["samesite"] == "Strict"
            print("Password login and Secure/HttpOnly/SameSite session verified")
        async with client.get(base + "/api/account") as response:
            account = await response.json()
            assert response.status == 200 and account["mode"] == "demo"
            print("Authenticated account read verified: Demo, privateConnected=" + str(account["privateConnected"]))
        async with client.get(base + "/api/events") as response:
            event = await response.content.readline()
            assert event.startswith(b"data: ")
            print("HTTPS SSE received")
        async with client.ws_connect(base.replace("https", "wss") + "/api/market/stream?instrument=BTC-USDT&bar=1m&mode=live", origin=base) as websocket:
            for _ in range(20):
                payload = await websocket.receive_json(timeout=15)
                if payload.get("payload", {}).get("data"):
                    print("WSS market payload received")
                    break
            else:
                raise AssertionError("No market data through WSS")
        async with client.post(base + "/api/logout", json={}, headers={"X-Operator-CSRF": session["csrf"], "Origin": base}) as response:
            assert response.status == 200
        async with client.get(base + "/api/account") as response:
            assert response.status == 401
            print("Logout revocation verified; no trading operations submitted")


asyncio.run(main())
