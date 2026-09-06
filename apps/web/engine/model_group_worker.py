#!/usr/bin/env python3
"""Root-registered GRU model group, real public data and local-only inference.

The registry chooses installed executable code and immutable model identity.
The operation request chooses only a published group, budget, and duration.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import logging
import os
from pathlib import Path
import re
import signal
import socket
import sys
import time

from control import ControlServer
from model_client import ModelGuardClient
from model_market_actor import make_gru_market_actor, make_rdt_market_actor
from model_release import import_file, load_manifest, sha
from model_strategy import make_model_strategy

sys.path.append(str(Path(__file__).resolve().parents[1] / "server"))
from group_runtime import LaunchRegistry, encode, money, object_file, trusted_file


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encode(value) + "\n")
    temporary.replace(path)


def model_market_ready(*, model_ready, failure, connected, stats, quote_age, window_ready, target, now_ns, max_age_ms):
    input_age = (now_ns - target["asOfNs"]) / 1e6 if target is not None else None
    return bool(model_ready and not failure and connected.get("data") is True and connected.get("exec") is True
        and stats.get("delivered", 0) > 0 and stats.get("closed") is False
        and quote_age is not None and 0 <= quote_age <= 5 and window_ready is True
        and target is not None and 0 <= input_age <= max_age_ms)


def validate_model_launch(args, *, strict=True):
    registry = LaunchRegistry(args.registry, strict=strict)
    digest = registry.digest
    request = object_file(args.request)
    if set(request) - {"groupId", "action", "registryVersion", "budgetUsdt", "durationSeconds", "runId", "createdAt", "mode"}:
        raise ValueError("Unexpected launch request fields")
    if request.get("action") != "start" or request.get("registryVersion") != digest:
        raise ValueError("Launch request differs from published registry")
    spec = registry.validate_inputs(request["groupId"])
    mode = spec.get("modelMode", spec.get("mode", "shadow"))
    if mode not in {"shadow", "sandbox"} or request.get("mode", mode) != mode:
        raise ValueError("Launch mode differs from the explicitly published model group")
    if spec.get("environment") != "sandbox":
        raise ValueError("Model group node must use Nautilus sandbox environment")
    for field, value in (("releaseManifest", args.release_manifest), ("manifestSha256", args.manifest_sha256),
                         ("guardBinary", args.guard_binary), ("runnerRoot", args.runner_root)):
        if str(spec.get(field)) != str(value):
            raise ValueError(f"CLI {field} differs from root launch registry")
    worker_path = registry.document["modelRuntime"]["workerPath"]
    if Path(worker_path).resolve() != Path(__file__).resolve():
        raise ValueError("Registry did not select the preinstalled model worker")
    trusted_file(Path(worker_path), strict)
    trusted_file(Path(args.guard_binary), strict)
    trusted_file(Path(spec["settingsPath"]), strict)
    if sha(spec["settingsPath"]) != spec["settingsSha256"]:
        raise ValueError("Settings hash differs from registry")
    runtime = Path(spec["runtimePath"])
    for filename in ("settings.py", "node_config.py", "health.py", "alerts.py"):
        trusted_file(runtime / filename, strict)
    for filename in ("model_release.py", "model_client.py", "model_inference_worker.py", "model_strategy.py", "model_market_actor.py", "model_probe.py", "commands.py", "control.py"):
        trusted_file(Path(__file__).with_name(filename), strict)
    release = load_manifest(args.release_manifest, args.manifest_sha256)
    if mode not in release["policy"]["allowedModes"]:
        raise ValueError("Selected model mode is not permitted by the release")
    if release["runnerId"] == "gru_v1":
        for filename in ("train_gru.py", "prepare.py"):
            trusted_file(Path(args.runner_root) / "recent_btc" / filename, strict)
        spec.update(executionInstrumentId="BTC-USDT.OKX", modelInstrument="BTC-USDT", domain=None, contractKey=None)
    else:
        contract_key = spec.get("contractKey")
        mappings = {"crypto:BTC-USDT": ("crypto", "BTC-USDT", "BTC-USDT.OKX"),
                    "token_hour:XNVDA": ("token_hour", "XNVDA", "XNVDA-USDT.OKX")}
        if mode != "shadow" or contract_key not in mappings or contract_key not in release["domainContracts"]:
            raise ValueError("RDT requires shadow mode and an explicitly host-selected supported contract")
        domain, instrument, execution = mappings[contract_key]
        if (spec.get("domain"), spec.get("modelInstrument"), spec.get("executionInstrumentId")) != (domain, instrument, execution):
            raise ValueError("RDT group contract/instrument mapping mismatch")
        c = release["domainContracts"][contract_key]
        if (c["domain"], c["instrument"]) != (domain, instrument):
            raise ValueError("Published RDT feature contract identity mismatch")
        selected = release["outputContract"]["selectedHeadByDomain"].get(domain)
        if type(selected) is not int or not 0 <= selected < len(release["outputContract"]["horizons"][domain]):
            raise ValueError("Explicit published RDT output head required")
    run_id = request.get("runId", "")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,95}", run_id) or not run_id.startswith(request["groupId"] + "-") or args.request.parent.name != run_id:
        raise ValueError("Run request path or identifier mismatch")
    budget = money(request.get("budgetUsdt"))
    if Decimal(budget) > Decimal(money(spec["maxBudgetUsdt"])):
        raise ValueError("Virtual budget exceeds published group limit")
    if type(request.get("durationSeconds")) is not int or not 60 <= request["durationSeconds"] <= min(86400, spec.get("maxDurationSeconds", 86400)):
        raise ValueError("Run duration exceeds published limits")
    settings = object_file(Path(spec["settingsPath"]))
    if settings.get("environment") != "sandbox" or settings.get("venue", "OKX") != "OKX":
        raise ValueError("Model settings template must use the OKX sandbox environment")
    cap = settings.get("risk", {}).get("max_notional_per_order", {}).get(spec["executionInstrumentId"])
    if type(cap) is not int or cap <= 0:
        raise ValueError("Root settings must explicitly cap the selected execution instrument")
    return request, {**spec, "modelMode": mode}, release


def assemble_model(args, request, spec, release, loop):
    for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"):
        os.environ[key] = ""
    sys.path.insert(0, spec["runtimePath"])
    import msgspec
    from settings import Settings, problems
    from node_config import build
    from alerts import Notifier
    from health import HealthMonitor, HealthMonitorConfig
    from nautilus_trader.adapters.okx.data import OKXDataClient
    from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider
    from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
    from nautilus_trader.core import nautilus_pyo3
    from nautilus_trader.live.factories import LiveDataClientFactory
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.instruments import CurrencyPair
    EngineCommands = import_file("montlok_model_commands", Path(__file__).with_name("commands.py")).EngineCommands

    directory = args.request.parent
    raw = object_file(Path(spec["settingsPath"]))
    execution_id = spec["executionInstrumentId"]
    raw.update(state_dir=str(directory), sandbox_balance=f"{request['budgetUsdt']} USDT", capture=True,
        instruments=[execution_id], instrument_types=["SPOT"], strategies=[])
    risk = dict(raw.get("risk", {}))
    published_caps = risk.get("max_notional_per_order", {})
    cap = published_caps[execution_id]
    risk["max_notional_per_order"] = {execution_id: cap}
    raw["risk"] = risk
    settings = msgspec.convert(raw, type=Settings, strict=True)
    errors = problems(settings)
    if errors:
        raise ValueError("; ".join(errors))
    config = build(settings)
    config = msgspec.structs.replace(config,
        data_engine=msgspec.structs.replace(config.data_engine, emit_quotes_from_book=True),
        exec_clients={"OKX": msgspec.structs.replace(config.exec_clients["OKX"], book_type="L1_MBP", default_leverage=Decimal(1))})

    class CostedDataClient(OKXDataClient):
        def _handle_data(self, data):
            if isinstance(data, CurrencyPair):
                values = type(data).to_dict(data)
                values.update(maker_fee="0.001", taker_fee="0.001")
                data = type(data).from_dict(values)
            return super()._handle_data(data)

    class PublicFactory(LiveDataClientFactory):
        @staticmethod
        def create(loop, name, config, msgbus, cache, clock):
            http = nautilus_pyo3.OKXHttpClient(api_key="", api_secret="", api_passphrase="",
                environment=nautilus_pyo3.OKXEnvironment.LIVE, timeout_secs=15, max_retries=2)
            provider = OKXInstrumentProvider(client=http, instrument_types=config.instrument_types,
                contract_types=config.contract_types, instrument_families=config.instrument_families, config=config.instrument_provider)
            client = CostedDataClient(loop=loop, client=http, msgbus=msgbus, cache=cache, clock=clock,
                instrument_provider=provider, config=config, name=name)
            if not hasattr(client, "set_python_data_ingress"):
                raise RuntimeError("Installed OKX adapter lacks the explicit bounded Python data ingress API")
            client.set_python_data_ingress(bridge)
            return client

    client = ModelGuardClient(args.release_manifest, args.manifest_sha256, args.guard_binary, args.runner_root,
        contract_key=spec.get("contractKey"))
    node = TradingNode(config=config, loop=loop)
    if not hasattr(node, "enable_python_data_ingress"):
        raise RuntimeError("Installed Nautilus lacks the explicit bounded sandbox Python data ingress API")
    bridge = node.enable_python_data_ingress(capacity=1024, max_batch=64)
    strategy = make_model_strategy(node, client, mode=spec["modelMode"], instrument_id=execution_id,
        budget_usdt=request["budgetUsdt"], loop=loop, model_instrument=spec.get("modelInstrument"), model_domain=spec.get("domain"))
    strategy.native_data_ingress = bridge
    producer = (make_gru_market_actor(release, args.runner_root, loop=loop) if release["runnerId"] == "gru_v1"
        else make_rdt_market_actor(release, args.runner_root, spec["contractKey"], loop=loop))
    node.trader.add_strategy(strategy)
    node.trader.add_actor(producer)
    notifier = Notifier(settings.alerts.webhook, settings.alerts.format, source=f"{settings.trader_id}/{spec['modelMode']}")
    monitor = HealthMonitor(HealthMonitorConfig(instruments=settings.instruments,
        stale_after_secs=settings.alerts.stale_after_secs, rss_limit_mb=settings.alerts.rss_limit_mb,
        disk_path=str(directory), disk_min_free_pct=settings.alerts.disk_min_free_pct,
        heartbeat_mins=settings.alerts.heartbeat_mins), notifier,
        probes={"data": node.kernel.data_engine.check_connected, "exec": node.kernel.exec_engine.check_connected})
    node.trader.add_actor(monitor)
    node.add_data_client_factory("OKX", PublicFactory)
    node.add_exec_client_factory("OKX", SandboxLiveExecClientFactory)
    node.build()
    commands = EngineCommands(node, monitor, notifier, settings)
    handlers = commands.handlers()
    def model_status(request=None):
        status = commands.status(request)
        failure = client.failure_reason() or node.native_data_ingress_failure
        if client.ready is not None and failure:
            status.update(engineTradingState=status["tradingState"], tradingState="ERROR", modelRuntimeError=failure)
        model = strategy.model_snapshot()
        market = producer.model_market_snapshot()
        stats = bridge.stats()
        quote = node.cache.quote_tick(strategy.execution_instrument_id)
        now = time.time_ns()
        quote_age = (now - quote.ts_event) / 1e9 if quote is not None else None
        target = model.get("latestTarget")
        input_age = (now - target["asOfNs"]) / 1e6 if target is not None else None
        contract = release["featureContract"] if release["runnerId"] == "gru_v1" else release["domainContracts"][spec["contractKey"]]
        max_age = min(release["runtime"]["maxInputAgeMs"], 2 * contract["barSeconds"] * 1000)
        market_ready = model_market_ready(model_ready=client.is_ready, failure=failure,
            connected=status["connected"], stats=stats, quote_age=quote_age,
            window_ready=market.get("windowReady"), target=target, now_ns=now, max_age_ms=max_age)
        return {**status, "warmupComplete": client.is_ready,
            "modelHash": release["model"]["sha256"], "manifestSha256": args.manifest_sha256,
            "mode": spec["modelMode"], "model": model, "modelMarket": market,
            "marketReady": bool(market_ready), "quoteAgeSeconds": quote_age, "modelInputAgeMs": input_age,
            "nativeDataIngress": {"ownerId": str(bridge.owner_id), "failure": bridge.failure,
                "closed": bridge.closed, "stats": stats},
            "nativeDataIngressFailure": node.native_data_ingress_failure}
    handlers["status"] = model_status
    control = ControlServer(directory / "control.sock", handlers)
    return node, strategy, producer, client, commands, control


class ModelViews:
    def __init__(self, node, strategy, producer, commands, request):
        self.node, self.strategy, self.producer, self.commands, self.request = node, strategy, producer, commands, request
        self.peak = None
        self.drawdown = 0.0

    def snapshot(self, *, final=False, error=None):
        from nautilus_trader.model.identifiers import InstrumentId, Venue
        from nautilus_trader.model.currencies import USDT
        node, strategy = self.node, self.strategy
        instrument = strategy.execution_instrument_id
        quote = node.cache.quote_tick(instrument)
        account = node.portfolio.account(Venue("OKX"))
        cash = account.balance_total(USDT) if account else None
        nav = float(cash) if cash is not None else None
        positions = []
        for position in node.cache.positions_open(strategy_id=strategy.id):
            mark = float(quote.bid_price) if quote else None
            quantity = float(position.signed_qty)
            pnl = quantity * (mark - float(position.avg_px_open)) if mark is not None else None
            if mark is None:
                nav = None
            elif nav is not None:
                nav += quantity * mark
            positions.append({"strategy": str(strategy.id), "instrument": str(position.instrument_id), "side": "Long",
                "quantity": quantity, "averagePrice": float(position.avg_px_open), "markPrice": mark,
                "unrealizedPnl": pnl, "notional": quantity * mark if mark is not None else None, "margin": 0})
        if nav is not None:
            self.peak = max(self.peak or nav, nav)
            self.drawdown = min(self.drawdown, nav / self.peak - 1) if self.peak else 0.0
        state = "ERROR" if error else "STOPPED" if final else self.commands.state
        status = self.commands.status()
        model = strategy.model_snapshot()
        snapshot = dict(observed_at=datetime.now(timezone.utc).isoformat(), run_id=self.request["runId"],
            group_id=self.request["groupId"], environment="sandbox", mode=strategy.model_mode,
            trading_state=state, warmupComplete=model["warmupComplete"], nav_usdt=nav,
            pnl_usdt=nav - float(self.request["budgetUsdt"]) if nav is not None else None,
            max_drawdown=self.drawdown if nav is not None else None,
            fees_usdt=None if strategy.fees_unconverted else strategy.fees_usdt,
            fills=strategy.fills, model=model, model_market=self.producer.model_market_snapshot(),
            market_data="okx_public_live_websocket_quotes_and_confirmed_candles", error=error,
            connected=status["connected"], instruments_seen=int(quote is not None),
            quote_age_seconds=(time.time_ns() - quote.ts_event) / 1e9 if quote else None)
        bridge = strategy.native_data_ingress
        snapshot["native_data_ingress"] = {"ownerId": str(bridge.owner_id), "failure": bridge.failure,
            "closed": bridge.closed, "stats": bridge.stats()}
        snapshot["native_data_ingress_failure"] = node.native_data_ingress_failure
        target = model["latestTarget"]
        snapshot.update(signal_as_of=datetime.fromtimestamp(target["asOfNs"] / 1e9, timezone.utc).isoformat() if target else None,
            prediction=target["prediction"] if target else None, target_fraction=target["targetFraction"] if target else None)
        orders = [{**self.commands.order_row(o), "id": str(o.client_order_id)} for o in node.cache.orders(strategy_id=strategy.id)[-500:]]
        view = dict(schemaVersion=1, runId=self.request["runId"], groupId=self.request["groupId"],
            observed_at=snapshot["observed_at"], tradingState=state, completed=final, positions=positions, orders=orders,
            environment="sandbox", mode=strategy.model_mode, warmupComplete=model["warmupComplete"],
            fills=list(reversed(strategy.managed_fills)), fillsTotal=strategy.fills,
            ordersTotal=node.cache.orders_total_count(strategy_id=strategy.id), eventCount=strategy.managed_events,
            connected=status["connected"], health=status["health"], error=error, model=model,
            strategies=[{"id": str(strategy.id), "mode": state, "runId": self.request["runId"], "pnl": snapshot["pnl_usdt"],
                "fills": strategy.fills, "artifact": model["releaseId"], "model": model}])
        return snapshot, view


async def start_model_until_stop(client, stop):
    """A stop during native/GPU warmup must never start the trading node later."""
    startup = asyncio.create_task(client.start())
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({startup, stopping}, return_when=asyncio.FIRST_COMPLETED)
        # Give an observed stop priority even when readiness arrives in the
        # same loop iteration. client.start cleans its children on cancellation.
        if stop.is_set():
            if not startup.done():
                startup.cancel()
            await asyncio.gather(startup, return_exceptions=True)
            await client.close()
            return None
        return await startup
    finally:
        if not stopping.done():
            stopping.cancel()
        await asyncio.gather(stopping, return_exceptions=True)


async def supervise_model(args, request, spec, release, components):
    import psutil
    node, strategy, producer, client, commands, control = components
    directory = args.request.parent
    stop = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    started = time.time()
    manifest = dict(run_id=request["runId"], group_id=request["groupId"], group_name=spec.get("name", request["groupId"]),
        execution="nautilus_sandbox", account_type="CASH", leverage=1, instrument_type="SPOT",
        instruments={spec["executionInstrumentId"]: {"modelTarget": True}}, capital_usdt=float(request["budgetUsdt"]),
        mode=spec["modelMode"], model_release=release["releaseId"], model_sha256=release["model"]["sha256"],
        model_manifest_sha256=args.manifest_sha256, signals_sha256=args.manifest_sha256,
        runner_sha256=sha(Path(__file__)), worker_sha256=sha(Path(__file__)),
        registry_sha256=request["registryVersion"], settings_sha256=spec["settingsSha256"],
        guard_sha256=sha(args.guard_binary), runtime_location="local_subprocess", runtime_host=socket.gethostname(),
        run_started_at=datetime.now(timezone.utc).isoformat(), planned_seconds=request["durationSeconds"],
        stop_at_unix=started + request["durationSeconds"], pid=os.getpid(), process_create_time=psutil.Process().create_time(),
        trader_id=commands.settings.trader_id, orders_enabled=spec["modelMode"] == "sandbox",
        market_data="okx_public_live_websocket_quotes_and_confirmed_candles", fee_assumption={"maker_bps": 10, "taker_bps": 10},
        model=release["modelVersion"], independent_test_metrics=release["provenance"].get("independentTestMetrics"),
        contract_key=spec.get("contractKey"), model_domain=spec.get("domain"), model_instrument=spec["modelInstrument"],
        strategy_description="Existing trained model, aligned completed bars, local guarded inference and spot long/cash targets",
        rebalance_policy="one_target_per_new_completed_selected_contract_window",
        alpha_metadata=[{"name": release["modelVersion"], "rule": f"{release['runnerId']} / {spec.get('contractKey') or 'BTC-USDT 15m'} / published output head and threshold",
            "source": "existing trained weights; independent alpha performance unverified"}])
    write_json(directory / "manifest.json", manifest)
    running = None
    error = None
    stopped_before_ready = False
    views = ModelViews(node, strategy, producer, commands, request)
    try:
        ready = await start_model_until_stop(client, stop)
        if ready is None:
            stopped_before_ready = True
            return
        write_json(directory / "model-ready.json", ready)
        await control.start()
        running = asyncio.create_task(node.run_async())
        while not stop.is_set() and time.time() < manifest["stop_at_unix"]:
            if client.failure_reason():
                raise RuntimeError(client.failure_reason())
            if running.done():
                await running
                raise RuntimeError("Nautilus model node stopped unexpectedly")
            snapshot, view = views.snapshot()
            write_json(directory / "status.json", snapshot)
            write_json(directory / "view.json", view)
            with (directory / "equity.jsonl").open("a") as stream:
                stream.write(encode(snapshot) + "\n")
            try:
                await asyncio.wait_for(stop.wait(), min(1, max(.01, manifest["stop_at_unix"] - time.time())))
            except TimeoutError:
                pass
        if not strategy.model_targets and not stop.is_set():
            raise RuntimeError("Run duration elapsed without a fresh model prediction; not validated")
    except Exception as failure:
        error = str(failure) or type(failure).__name__
        raise
    finally:
        # Durable operation receipts settle before stopping risk/execution state.
        await control.stop()
        strategy.accepting = False
        await producer.quiesce()
        for task in tuple(strategy.pending_predictions):
            task.cancel()
        await asyncio.gather(*strategy.pending_predictions, return_exceptions=True)
        await client.close()
        if running is not None:
            await node.stop_async()
            await asyncio.wait_for(running, 20)
        else:
            # Warmup was cancelled before node.run_async: close its already
            # allocated capability before writing the never-started final view.
            strategy.native_data_ingress.close()
        snapshot, view = views.snapshot(final=True, error=error)
        snapshot.update(completed_at=datetime.now(timezone.utc).isoformat(),
            deadline_reached=time.time() >= manifest["stop_at_unix"],
            node_started=running is not None, orders_enabled=spec["modelMode"] == "sandbox" and running is not None,
            stop_reason="failed" if error else "stopped_before_model_ready" if stopped_before_ready else "user_requested" if stop.is_set() else "duration_elapsed")
        write_json(directory / "final.json", snapshot)
        write_json(directory / "view.json", view)
        if running is not None:
            from nautilus_trader.model.identifiers import Venue
            node.trader.generate_orders_report().to_csv(directory / "orders.csv")
            node.trader.generate_fills_report().to_csv(directory / "fills.csv")
            node.trader.generate_positions_report().to_csv(directory / "positions.csv")
            node.trader.generate_account_report(venue=Venue("OKX")).to_csv(directory / "account.csv")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--request", type=Path, required=True)
    p.add_argument("--release-manifest", type=Path, required=True)
    p.add_argument("--manifest-sha256", required=True)
    p.add_argument("--guard-binary", type=Path, required=True)
    p.add_argument("--runner-root", type=Path, required=True)
    args = p.parse_args()
    request, spec, release = validate_model_launch(args)
    # Nautilus installs its event-loop policy when the live node module is
    # imported. Create our loop afterwards so subprocess transport and policy
    # agree (a Selector loop plus uvloop policy lacks a child watcher).
    for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"):
        os.environ[key] = ""
    from nautilus_trader.live.node import TradingNode  # noqa: F401
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    node = None
    try:
        components = assemble_model(args, request, spec, release, loop)
        node = components[0]
        loop.run_until_complete(supervise_model(args, request, spec, release, components))
    finally:
        if node is not None:
            node.dispose()
        elif not loop.is_closed():
            loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
