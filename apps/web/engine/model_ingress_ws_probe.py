"""Loopback-only native OKX subscription-ACK failure regression.

No trading client, exchange account, public endpoint, or model order is used.
Run each case in a fresh process against the newly installed native extension.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import time


async def probe(node, bridge, case, native):
    from aiohttp import web, WSMsgType
    subscribed, release_ack, ack_sent = asyncio.Event(), asyncio.Event(), asyncio.Event()
    received, sockets = [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        sockets.append(ws)
        async for message in ws:
            if message.type != WSMsgType.TEXT:
                continue
            if message.data == "ping":
                await ws.send_str("pong")
                continue
            payload = json.loads(message.data)
            if payload.get("op") != "subscribe":
                continue
            received.append(payload)
            subscribed.set()
            await release_ack.wait()
            await ws.send_json({"event": "subscribe", "arg": payload["args"][0],
                "connId": "montlok-loopback", "code": "60012", "msg": "intentional loopback-only subscription rejection"})
            ack_sent.set()
            # Deliberately keep the WebSocket open: the failure must come from
            # the rejected ACK, not an EOF or server disconnect.
        return ws

    application = web.Application()
    application.router.add_get("/ws/v5/public", handle)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    client = native.OKXWebSocketClient(url=f"ws://127.0.0.1:{port}/ws/v5/public")
    callback_values = []
    before = time.perf_counter()
    try:
        await asyncio.wait_for(client.connect(loop_=asyncio.get_running_loop(), instruments=[],
            callback=callback_values.append, live_ingress_lane="data", python_data_ingress=bridge), 8)
        await asyncio.wait_for(client.wait_until_active(timeout_secs=5), 7)
        await asyncio.wait_for(client.subscribe_instruments(native.OKXInstrumentType.SPOT), 5)
        await asyncio.wait_for(subscribed.wait(), 5)
        if case == "late_ack_after_close":
            bridge.close()
        release_ack.set()
        await asyncio.wait_for(ack_sent.wait(), 5)
        if case == "rejected_ack":
            async with asyncio.timeout(5):
                while bridge.failure is None or node.native_data_ingress_failure is None:
                    await asyncio.sleep(.01)
            assert bridge.failure == node.native_data_ingress_failure
            assert "60012" in bridge.failure or "subscription" in bridge.failure.lower(), bridge.failure
        else:
            await asyncio.sleep(.3)
            assert bridge.closed is True
            assert bridge.failure is None
            assert node.native_data_ingress_failure is None
        return {"case": case, "pass": True, "endpoint": f"127.0.0.1:{port}",
            "elapsedMs": (time.perf_counter() - before) * 1000, "subscriptionRequests": received,
            "serverSocketOpenAtObservation": bool(sockets and not sockets[0].closed),
            "serverInitiatedClose": False, "ownerId": str(bridge.owner_id),
            "nodeInstanceId": str(node.instance_id), "bridgeFailure": bridge.failure,
            "nodeFailure": node.native_data_ingress_failure, "bridgeClosed": bridge.closed,
            "stats": bridge.stats(), "callbackValues": len(callback_values),
            "ordersSent": False, "publicEndpointsContacted": False}
    finally:
        bridge.close()
        try:
            await asyncio.wait_for(client.close(), 8)
        finally:
            for ws in sockets:
                await ws.close()
            await runner.cleanup()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("case", choices=["rejected_ack", "late_ack_after_close"])
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"):
        os.environ[key] = ""
    # Import before loop creation: Nautilus installs its uvloop policy.
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.config import TradingNodeConfig, LoggingConfig
    from nautilus_trader.common import Environment
    from nautilus_trader.core import nautilus_pyo3
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    node = TradingNode(TradingNodeConfig(trader_id="INGRESS-TEST", environment=Environment.SANDBOX,
        logging=LoggingConfig(log_level="ERROR"), data_clients={}, exec_clients={}), loop=loop)
    bridge = node.enable_python_data_ingress(capacity=8, max_batch=2)
    try:
        result = loop.run_until_complete(probe(node, bridge, args.case, nautilus_pyo3))
        args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        print(json.dumps(result, allow_nan=False), flush=True)
    finally:
        node.dispose()


if __name__ == "__main__":
    main()
