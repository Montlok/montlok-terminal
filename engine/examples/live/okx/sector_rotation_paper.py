#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""Run frozen sector weights against public OKX data and Nautilus sandbox execution."""

import argparse
import asyncio
import hashlib
import json
import os
import signal
import time
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import psutil

from nautilus_trader.adapters.okx.config import OKXDataClientConfig
from nautilus_trader.adapters.okx.data import OKXDataClient
from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class SnapshotOKXDataClient(OKXDataClient):
    """Use the adapter's REST snapshot parser with the Python sandbox runner."""

    async def _connect(self):
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.get_all().values():
            values = type(instrument).to_dict(instrument)
            values["maker_fee"] = "0.001"
            values["taker_fee"] = "0.001"
            self._instrument_provider.add(type(instrument).from_dict(values))
        self._cache_instruments()
        self._send_all_instruments_to_data_engine()
        self._paper_subscriptions = set()
        self._paper_poll_task = self._loop.create_task(self._poll_snapshots())

    async def _poll_snapshots(self):
        while True:
            for instrument_id in sorted(self._paper_subscriptions, key=str):
                try:
                    raw = await self._http_client.request_orderbook_snapshot(
                        nautilus_pyo3.InstrumentId.from_str(str(instrument_id)), 10,
                    )
                    self._handle_data(OrderBookDeltas.from_pyo3(raw))
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._log.warning(f"Public snapshot failed for {instrument_id}: {error}")
                await asyncio.sleep(0.3)
            if not self._paper_subscriptions:
                await asyncio.sleep(0.3)

    async def _subscribe_order_book_deltas(self, command):
        self._paper_subscriptions.add(command.instrument_id)

    async def _subscribe_quote_ticks(self, command):
        self._paper_subscriptions.add(command.instrument_id)

    async def _disconnect(self):
        self._paper_poll_task.cancel()
        await asyncio.gather(self._paper_poll_task, return_exceptions=True)
        self._http_client.cancel_all_requests()


class PublicOKXFactory(LiveDataClientFactory):
    """Use the existing OKX adapter with anonymous public HTTP and WS sessions."""

    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        # Explicit empty strings prevent the HTTP constructor falling back to account
        # environment variables. Public requests do not sign. Both WS sessions receive
        # None credentials through config and therefore do not attempt login.
        client = nautilus_pyo3.OKXHttpClient(
            api_key="", api_secret="", api_passphrase="",
            environment=nautilus_pyo3.OKXEnvironment.LIVE,
            timeout_secs=15, max_retries=2,
        )
        provider = OKXInstrumentProvider(
            client=client,
            instrument_types=config.instrument_types,
            contract_types=config.contract_types,
            instrument_families=config.instrument_families,
            config=config.instrument_provider,
        )
        return SnapshotOKXDataClient(
            loop=loop, client=client, msgbus=msgbus, cache=cache, clock=clock,
            instrument_provider=provider, config=config, name=name,
        )


def write_json(path: Path, value: dict) -> None:
    pending = path.with_suffix(path.suffix + ".tmp")
    pending.write_text(json.dumps(value, indent=2, allow_nan=False, default=str) + "\n")
    pending.replace(path)


class SectorPaper(Strategy):
    def __init__(self, payload: dict, output: Path, verify_only: bool) -> None:
        super().__init__(StrategyConfig(strategy_id=payload.get("strategy_id", "SECTOR-PAPER-001")))
        self.payload = payload
        self.output = output
        self.targets = {InstrumentId.from_str(k): Decimal(str(v["weight"])) for k, v in payload["instruments"].items()}
        self.quotes = {}
        self.target_qty = {}
        self.orders = {}
        self.attempts = {}
        self.last_attempt = {}
        self.reasons = {}
        self.quote_count = 0
        self.fills = 0
        self.fees = Decimal(0)
        self.peak_nav = float(payload["capital_usdt"])
        self.max_drawdown = 0.0
        self.started = False
        self.verify_only = verify_only

    def on_start(self) -> None:
        missing = [str(i) for i in self.targets if self.cache.instrument(i) is None]
        if missing:
            raise RuntimeError(f"Missing public instrument definitions: {missing}")
        for instrument_id in self.targets:
            self.subscribe_order_book_deltas(instrument_id, BookType.L2_MBP)
            self.subscribe_quote_ticks(instrument_id)
        self.started = True

    def on_quote_tick(self, quote) -> None:
        instrument_id = quote.instrument_id
        self.quotes[instrument_id] = quote
        self.quote_count += 1
        if self.verify_only:
            return
        weight = self.targets.get(instrument_id, Decimal(0))
        if not weight:
            self.reasons[str(instrument_id)] = "sector_off"
            return
        now = self.clock.timestamp_ns()
        if now - quote.ts_init > 5_000_000_000:
            self.reasons[str(instrument_id)] = "stale_quote"
            return
        previous = self.orders.get(instrument_id)
        if previous is not None and not self.cache.order(previous).is_closed:
            return
        if now - self.last_attempt.get(instrument_id, 0) < 5_000_000_000:
            return
        instrument = self.cache.instrument(instrument_id)
        book = self.cache.order_book(instrument_id)
        if book is None or book.best_bid_price() is None or book.best_ask_price() is None:
            self.reasons[str(instrument_id)] = "awaiting_l2_book"
            return
        bid, ask = quote.bid_price.as_decimal(), quote.ask_price.as_decimal()
        if bid <= 0 or ask < bid:
            self.reasons[str(instrument_id)] = "invalid_quote"
            return
        spread_bps = (ask - bid) / ((ask + bid) / 2) * 10_000
        roundtrip_bps = spread_bps + 2 * abs(instrument.taker_fee) * 10_000 + 20
        if roundtrip_bps > 60:
            self.reasons[str(instrument_id)] = "cost_over_60bps"
            return
        limit = instrument.make_price(ask * Decimal("1.001"))
        if instrument_id not in self.target_qty:
            self.target_qty[instrument_id] = (
                Decimal(str(self.payload["capital_usdt"])) * weight
                / limit.as_decimal() / (1 + abs(instrument.taker_fee))
            )
        held = sum(
            (position.quantity.as_decimal() for position in self.cache.positions_open(instrument_id=instrument_id, strategy_id=self.id)),
            Decimal(0),
        )
        remaining = max(self.target_qty[instrument_id] - held, Decimal(0))
        step = instrument.size_increment.as_decimal()
        remaining = (remaining // step) * step
        if remaining == 0 or (instrument.min_quantity and remaining < instrument.min_quantity.as_decimal()):
            self.reasons[str(instrument_id)] = "allocated"
            return
        if instrument.min_notional and remaining * limit.as_decimal() < instrument.min_notional.as_decimal():
            self.reasons[str(instrument_id)] = "remaining_below_minimum"
            return
        if self.attempts.get(instrument_id, 0) >= 5:
            self.reasons[str(instrument_id)] = "attempt_limit"
            return
        order = self.order_factory.limit(
            instrument_id=instrument_id, order_side=OrderSide.BUY,
            quantity=instrument.make_qty(remaining), price=limit,
            time_in_force=TimeInForce.IOC,
        )
        self.orders[instrument_id] = order.client_order_id
        self.attempts[instrument_id] = self.attempts.get(instrument_id, 0) + 1
        self.last_attempt[instrument_id] = now
        self.reasons[str(instrument_id)] = "submitted_to_sandbox"
        self.submit_order(order)

    def on_event(self, event) -> None:
        row = {"observed_at": datetime.now(timezone.utc).isoformat(), "type": type(event).__name__, "event": str(event)}
        with (self.output / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(row) + "\n")
        if type(event).__name__ == "OrderFilled":
            self.fills += 1
            if event.commission.currency == USDT:
                self.fees += event.commission.as_decimal()

    def snapshot(self) -> dict:
        now = self.clock.timestamp_ns()
        account = self.cache.account_for_venue(Venue("OKX"))
        cash = float(account.balance_total(USDT).as_decimal()) if account else float(self.payload["capital_usdt"])
        positions = []
        nav = cash
        for position in self.cache.positions_open(strategy_id=self.id):
            quote = self.quotes.get(position.instrument_id)
            qty = float(position.quantity.as_decimal())
            mark = float(quote.bid_price.as_decimal()) if quote else float(position.avg_px_open)
            unrealized = qty * (mark - float(position.avg_px_open))
            if self.payload["account_type"] == "CASH":
                nav += qty * mark
            else:
                nav += unrealized
            positions.append({"instrument": str(position.instrument_id), "quantity": qty, "bid_mark": mark, "unrealized_pnl_usdt": unrealized})
        self.peak_nav = max(self.peak_nav, nav)
        self.max_drawdown = min(self.max_drawdown, nav / self.peak_nav - 1)
        return {
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "execution": "nautilus_sandbox", "market_data": "okx_public_live_rest_l2_snapshots",
            "strategy_started": self.started, "signal_as_of": self.payload["signal_as_of"],
            "nav_usdt": nav, "cash_usdt": cash, "pnl_usdt": nav - float(self.payload["capital_usdt"]),
            "max_drawdown": self.max_drawdown, "fees_usdt": float(self.fees), "fills": self.fills,
            "quote_count": self.quote_count, "instruments_seen": len(self.quotes),
            "quote_age_seconds": {str(k): (now - v.ts_init) / 1e9 for k, v in self.quotes.items()},
            "positions": positions, "asset_status": self.reasons,
            "rss_bytes": psutil.Process().memory_info().rss,
        }

    def on_stop(self) -> None:
        for instrument_id in self.targets:
            self.cancel_all_orders(instrument_id)


async def run(args) -> None:
    payload = json.loads(args.signals.read_text())
    assert payload["execution"] == "nautilus_sandbox"
    assert payload["account_type"] in {"CASH", "MARGIN"}
    assert payload["leverage"] == 1
    assert 0 < args.seconds <= 86400
    expected_instruments = int(payload.get("expected_instruments", 25))
    max_single_weight = float(payload.get("max_single_weight", 0.10))
    assert len(payload["instruments"]) == expected_instruments
    assert all(0 <= row["weight"] <= max_single_weight for row in payload["instruments"].values())
    assert sum(row["weight"] for row in payload["instruments"].values()) <= 1.000000001
    instrument_type_name = payload.get("instrument_type", "SPOT")
    instrument_type = getattr(nautilus_pyo3.OKXInstrumentType, instrument_type_name)
    contract_types = (
        (nautilus_pyo3.OKXContractType.LINEAR,)
        if instrument_type == nautilus_pyo3.OKXInstrumentType.SWAP
        else None
    )
    if instrument_type == nautilus_pyo3.OKXInstrumentType.SWAP:
        assert payload["account_type"] == "MARGIN"
    args.output.mkdir(parents=True, exist_ok=False)
    ids = frozenset(InstrumentId.from_str(k) for k in payload["instruments"])
    config = TradingNodeConfig(
        environment=Environment.SANDBOX, trader_id="SECTOR-PAPER-001",
        logging=LoggingConfig(log_level="INFO"),
        data_engine=LiveDataEngineConfig(qsize=100_000, emit_quotes_from_book=True),
        exec_engine=LiveExecEngineConfig(reconciliation=False),
        data_clients={"OKX": OKXDataClientConfig(
            environment=nautilus_pyo3.OKXEnvironment.LIVE,
            instrument_provider=InstrumentProviderConfig(load_ids=ids),
            instrument_types=(instrument_type,),
            contract_types=contract_types,
            update_instruments_interval_mins=None,
        )},
        exec_clients={"OKX": SandboxExecutionClientConfig(
            venue="OKX", starting_balances=[f"{payload['capital_usdt']} USDT"],
            account_type=payload["account_type"], default_leverage=Decimal(1),
            book_type="L2_MBP", bar_execution=False,
        )},
        timeout_connection=60, timeout_disconnection=10, timeout_post_stop=2,
    )
    node = TradingNode(config=config, loop=asyncio.get_running_loop())
    strategy = SectorPaper(payload, args.output, args.verify_only)
    node.trader.add_strategy(strategy)
    node.add_data_client_factory("OKX", PublicOKXFactory)
    node.add_exec_client_factory("OKX", SandboxLiveExecClientFactory)
    node.build()
    probe = strategy.order_factory.limit(
        instrument_id=next(iter(ids)), order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1), price=Price.from_int(1),
    )
    clients = node.kernel.exec_engine.get_clients_for_orders([probe])
    if len(clients) != 1:
        raise RuntimeError("Expected one sandbox execution route")
    client = next(iter(clients))
    if type(client).__module__ != "nautilus_trader.adapters.sandbox.execution":
        raise RuntimeError("Execution client is not the Nautilus sandbox")
    started_at = time.time()
    manifest = {
        **payload, "run_started_at": datetime.now(timezone.utc).isoformat(),
        "planned_seconds": args.seconds, "stop_at_unix": started_at + args.seconds,
        "signals_sha256": hashlib.sha256(args.signals.read_bytes()).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "pid": os.getpid(), "process_create_time": psutil.Process().create_time(),
        "execution_client_class": f"{type(client).__module__}.{type(client).__name__}",
        "orders_enabled": not args.verify_only,
        "engine_root": str(Path(__import__("nautilus_trader").__file__).parent),
        "fee_assumption": {"maker_bps": 10, "taker_bps": 10, "source": "research cost assumption, not authenticated account fee tier"},
    }
    write_json(args.output / "manifest.json", manifest)
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    task = asyncio.create_task(node.run_async())
    try:
        while not stop.is_set() and time.time() < manifest["stop_at_unix"]:
            if task.done():
                await task
                raise RuntimeError("Trading node stopped before trial deadline")
            snapshot = strategy.snapshot()
            write_json(args.output / "status.json", snapshot)
            with (args.output / "equity.jsonl").open("a") as stream:
                stream.write(json.dumps(snapshot, default=str) + "\n")
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(10, max(0.01, manifest["stop_at_unix"] - time.time())))
            except TimeoutError:
                pass
    finally:
        await node.stop_async()
        await asyncio.wait_for(task, timeout=20)
        snapshot = strategy.snapshot()
        snapshot["completed_at"] = datetime.now(timezone.utc).isoformat()
        snapshot["deadline_reached"] = time.time() >= manifest["stop_at_unix"]
        write_json(args.output / "final.json", snapshot)
        node.trader.generate_orders_report().to_csv(args.output / "orders.csv")
        node.trader.generate_fills_report().to_csv(args.output / "fills.csv")
        node.trader.generate_positions_report().to_csv(args.output / "positions.csv")
        node.trader.generate_account_report(venue=Venue("OKX")).to_csv(args.output / "account.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=86400)
    parser.add_argument("--verify-only", action="store_true")
    asyncio.run(run(parser.parse_args()))
