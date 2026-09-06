#!/usr/bin/env python3
"""Reviewed frozen-sector strategy + hardened Nautilus + published public data.

This entrypoint intentionally supports only root-registered SPOT/CASH sandbox
launches. Frozen signals remain frozen; there is no invented intraday model or
rebalance schedule. Imported strategy code is verified against its release hash.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from group_runtime import MARKET_DATA, LaunchRegistry, encode, money, object_file  # noqa: E402


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False, default=str) + "\n")
    temporary.replace(path)


def validate_launch(registry, request_path):
    request = object_file(request_path)
    if set(request) != {"groupId", "action", "registryVersion", "budgetUsdt", "durationSeconds", "runId", "createdAt"}:
        raise ValueError("运行请求字段不正确")
    if request["action"] != "start" or request["registryVersion"] != registry.digest:
        raise ValueError("运行请求的发布版本不一致")
    spec = registry.validate_inputs(request["groupId"])
    if not request["runId"].startswith(request["groupId"] + "-") or request_path.parent.name != request["runId"]:
        raise ValueError("实例目录与运行编号不一致")
    budget = money(request["budgetUsdt"])
    if Decimal(budget) > Decimal(spec["maxBudgetUsdt"]):
        raise ValueError("虚拟资金超过发布上限")
    if type(request["durationSeconds"]) is not int or not 60 <= request["durationSeconds"] <= spec["maxDurationSeconds"]:
        raise ValueError("运行时长超出范围")
    return request, spec


def load_strategy(path):
    module_spec = importlib.util.spec_from_file_location("montlok_frozen_sector_release", path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def market_factory(spec, release, native_factory):
    transport = spec.get("marketTransport", "native_ws")
    if transport not in MARKET_DATA:
        raise ValueError("未发布的行情传输方式")
    return native_factory if transport == "native_ws" else release.PublicOKXFactory


def readiness_error(connected_once, quote_count):
    if not connected_once:
        return "运行期间行情与执行客户端从未同时连接成功"
    if quote_count <= 0:
        return "运行期间没有收到有效行情，未完成运行验证"
    return None


def assemble(registry, request, spec, directory, loop):
    # Clear even inherited values before importing the native adapter. Credentials
    # are neither read from the operator vault nor accepted in the launch request.
    for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"):
        os.environ[key] = ""
    sys.path.insert(0, spec["runtimePath"])
    import msgspec
    from settings import Settings, problems
    from node_config import build
    from alerts import Notifier
    from commands import EngineCommands
    from control import ControlServer
    from health import HealthMonitor, HealthMonitorConfig
    from nautilus_trader.adapters.okx.data import OKXDataClient
    from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider
    from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
    from nautilus_trader.core import nautilus_pyo3
    from nautilus_trader.live.factories import LiveDataClientFactory
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.enums import OrderSide
    from nautilus_trader.model.enums import order_side_to_str
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Price, Quantity

    raw = object_file(Path(spec["settingsPath"]))
    raw.update(state_dir=str(directory), sandbox_balance=f"{request['budgetUsdt']} USDT", capture=True)
    settings = msgspec.convert(raw, type=Settings, strict=True)
    found = problems(settings)
    if found:
        raise ValueError("; ".join(found))
    configuration = build(settings)
    # Preserve L2 matching and the original 10bps fee assumption independently
    # of the explicitly published market transport.
    configuration = msgspec.structs.replace(
        configuration,
        data_engine=msgspec.structs.replace(configuration.data_engine, emit_quotes_from_book=True),
        exec_clients={"OKX": msgspec.structs.replace(configuration.exec_clients["OKX"],
                                                    book_type="L2_MBP", default_leverage=Decimal(1))},
    )

    class CostedDataClient(OKXDataClient):
        def _handle_data(self, data):
            if isinstance(data, CurrencyPair):
                values = type(data).to_dict(data)
                values.update(maker_fee="0.001", taker_fee="0.001")
                data = type(data).from_dict(values)
            return super()._handle_data(data)

    class PublicWebSocketFactory(LiveDataClientFactory):
        @staticmethod
        def create(loop, name, config, msgbus, cache, clock):
            client = nautilus_pyo3.OKXHttpClient(api_key="", api_secret="", api_passphrase="",
                environment=nautilus_pyo3.OKXEnvironment.LIVE, timeout_secs=15, max_retries=2)
            provider = OKXInstrumentProvider(client=client, instrument_types=config.instrument_types,
                contract_types=config.contract_types, instrument_families=config.instrument_families,
                config=config.instrument_provider)
            return CostedDataClient(loop=loop, client=client, msgbus=msgbus, cache=cache, clock=clock,
                                   instrument_provider=provider, config=config, name=name)

    payload = object_file(Path(spec["signalsPath"]))
    payload["capital_usdt"] = float(request["budgetUsdt"])
    payload["strategy_id"] = "SECTOR-PAPER-001"
    release = load_strategy(spec["strategyPath"])
    class ObservedSector(release.SectorPaper):
        def __init__(self, *args):
            super().__init__(*args)
            self.managed_fills = deque(maxlen=500)
            self.managed_events = 0

        def on_event(self, event):
            super().on_event(event)
            self.managed_events += 1
            if type(event).__name__ == "OrderFilled":
                self.managed_fills.append({"id": str(event.trade_id), "orderId": str(event.client_order_id),
                    "strategy": str(event.strategy_id), "instrument": str(event.instrument_id),
                    "side": order_side_to_str(event.order_side), "quantity": str(event.last_qty),
                    "price": str(event.last_px), "fee": str(event.commission.as_decimal()),
                    "feeCurrency": str(event.commission.currency),
                    "time": datetime.fromtimestamp(event.ts_event / 1e9, timezone.utc).isoformat()})

    strategy = ObservedSector(payload, directory, spec.get("verifyOnly") is True)
    node = TradingNode(config=configuration, loop=loop)
    node.trader.add_strategy(strategy)
    node.add_data_client_factory("OKX", market_factory(spec, release, PublicWebSocketFactory))
    node.add_exec_client_factory("OKX", SandboxLiveExecClientFactory)
    node.build()
    probe = strategy.order_factory.limit(instrument_id=InstrumentId.from_str(settings.instruments[0]),
        order_side=OrderSide.BUY, quantity=Quantity.from_int(1), price=Price.from_int(1))
    clients = node.kernel.exec_engine.get_clients_for_orders([probe])
    if len(clients) != 1 or type(next(iter(clients))).__module__ != "nautilus_trader.adapters.sandbox.execution":
        raise ValueError("执行路由不是 Nautilus Sandbox")
    notifier = Notifier(settings.alerts.webhook, settings.alerts.format, source=f"{settings.trader_id}/sandbox")
    monitor = HealthMonitor(HealthMonitorConfig(instruments=settings.instruments,
        stale_after_secs=settings.alerts.stale_after_secs, rss_limit_mb=settings.alerts.rss_limit_mb,
        disk_path=str(directory), disk_min_free_pct=settings.alerts.disk_min_free_pct,
        heartbeat_mins=settings.alerts.heartbeat_mins), notifier,
        probes={"data": node.kernel.data_engine.check_connected, "exec": node.kernel.exec_engine.check_connected})
    node.trader.add_actor(monitor)
    commands = EngineCommands(node, monitor, notifier, settings)
    return node, strategy, commands, ControlServer(directory / "control.sock", commands.handlers()), payload


def write_view(directory, node, strategy, commands, snapshot, request, *, final=False, error=None):
    """Small cache projections only; CSV/DataFrame reports are reserved for exit."""
    state = "ERROR" if error else "STOPPED" if final else commands.state
    engine = commands.status()
    positions = []
    for row in snapshot.get("positions", []):
        quantity = row.get("quantity")
        mark = row.get("bid_mark")
        pnl = row.get("unrealized_pnl_usdt")
        positions.append({"strategy": str(strategy.id), "instrument": row.get("instrument"), "side": "Long",
            "quantity": quantity, "markPrice": mark, "unrealizedPnl": pnl,
            "averagePrice": mark - pnl / quantity if quantity and mark is not None and pnl is not None else None,
            "notional": quantity * mark if quantity is not None and mark is not None else None,
            "margin": 0, "state": state})
    orders = []
    for order in node.cache.orders(strategy_id=strategy.id)[-500:]:
        row = commands.order_row(order)
        orders.append({**row, "id": row["clientOrderId"], "strategy": str(order.strategy_id)})
    write_json(directory / "view.json", {"schemaVersion": 1, "runId": request["runId"],
        "groupId": request["groupId"], "observed_at": snapshot["observed_at"], "tradingState": state,
        "market_data": snapshot.get("market_data"),
        "completed": final, "positions": positions, "orders": orders,
        "fills": list(reversed(strategy.managed_fills)), "fillsTotal": strategy.fills,
        "ordersTotal": node.cache.orders_total_count(strategy_id=strategy.id),
        "eventCount": strategy.managed_events, "connected": engine.get("connected", {}),
        "health": engine.get("health", {}), "error": error,
        "strategies": [{"id": str(strategy.id), "mode": state, "runId": request["runId"],
            "pnl": snapshot.get("pnl_usdt"), "fills": strategy.fills,
            "artifact": "frozen_sector", "model": None}]})


async def supervise(node, strategy, commands, control, payload, request, spec, directory):
    from nautilus_trader.model.identifiers import Venue
    import psutil

    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    started = time.time()
    market_data = MARKET_DATA[spec.get("marketTransport", "native_ws")]
    manifest = {**payload, "run_id": request["runId"], "group_id": request["groupId"],
        "group_name": spec.get("name"), "strategy_description": spec.get("description"),
        "alpha_metadata": spec.get("alphaMetadata", []),
        "run_started_at": datetime.now(timezone.utc).isoformat(),
        "planned_seconds": request["durationSeconds"], "stop_at_unix": started + request["durationSeconds"],
        "signals_sha256": spec["signalsSha256"], "runner_sha256": spec["strategySha256"],
        "worker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "market_data": market_data, "market_transport": spec.get("marketTransport", "native_ws"),
        "rebalance_policy": "frozen_signals_initial_allocation",
        "execution_client_class": "nautilus_trader.adapters.sandbox.execution.SandboxExecutionClient",
        "orders_enabled": spec.get("verifyOnly") is not True, "pid": os.getpid(),
        "process_create_time": psutil.Process().create_time(), "trader_id": commands.settings.trader_id,
        "registry_sha256": request["registryVersion"], "settings_sha256": spec["settingsSha256"],
        "fee_assumption": {"maker_bps": 10, "taker_bps": 10}}
    write_json(directory / "manifest.json", manifest)
    await control.start()
    commands.notifier.send("INFO", "模拟实例启动", request["runId"])
    running = asyncio.create_task(node.run_async())
    error = None
    connected_once = False
    try:
        while not stop.is_set() and time.time() < manifest["stop_at_unix"]:
            if running.done():
                await running
                raise RuntimeError("引擎提前结束")
            snapshot = strategy.snapshot()
            connected_once = connected_once or (node.kernel.data_engine.check_connected() and node.kernel.exec_engine.check_connected())
            snapshot.update(market_data=market_data, run_id=request["runId"], connected_once=connected_once,
                            group_id=request["groupId"], trading_state=commands.state)
            write_json(directory / "status.json", snapshot)
            write_view(directory, node, strategy, commands, snapshot, request)
            with (directory / "equity.jsonl").open("a") as stream:
                stream.write(json.dumps(snapshot, allow_nan=False, default=str) + "\n")
            try:
                await asyncio.wait_for(stop.wait(), min(1, max(0.01, manifest["stop_at_unix"] - time.time())))
            except TimeoutError:
                pass
        validation_error = readiness_error(connected_once, strategy.quote_count)
        if validation_error:
            raise RuntimeError(validation_error)
    except Exception as failure:
        error = str(failure)
        commands.notifier.send("CRITICAL", "模拟实例退出", error)
        raise
    finally:
        await control.stop()
        try:
            await node.stop_async()
            await asyncio.wait_for(running, 20)
        finally:
            snapshot = strategy.snapshot()
            snapshot.update(market_data=market_data, connected_once=connected_once, completed_at=datetime.now(timezone.utc).isoformat(),
                deadline_reached=time.time() >= manifest["stop_at_unix"], error=error, run_id=request["runId"],
                group_id=request["groupId"], trading_state="ERROR" if error else "STOPPED")
            write_json(directory / "final.json", snapshot)
            write_view(directory, node, strategy, commands, snapshot, request, final=True, error=error)
            node.trader.generate_orders_report().to_csv(directory / "orders.csv")
            node.trader.generate_fills_report().to_csv(directory / "fills.csv")
            node.trader.generate_positions_report().to_csv(directory / "positions.csv")
            node.trader.generate_account_report(venue=Venue("OKX")).to_csv(directory / "account.csv")
            commands.notifier.send("INFO", "模拟实例已停止", request["runId"])


def main(args):
    registry = LaunchRegistry(args.registry)
    request, spec = validate_launch(registry, args.request)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    node = None
    try:
        node, strategy, commands, control, payload = assemble(registry, request, spec, args.request.parent, loop)
        loop.run_until_complete(supervise(node, strategy, commands, control, payload, request, spec, args.request.parent))
    finally:
        if node is not None:
            node.dispose()
        elif not loop.is_closed():
            loop.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main(parser.parse_args())
