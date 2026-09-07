#!/usr/bin/env python3
"""Native Nautilus execution fed by a resident, bounded model subprocess."""
from __future__ import annotations

import argparse
import asyncio
from collections import deque
from decimal import Decimal
import json
from pathlib import Path
import threading
import time
import uuid
import sys

from live_hft_worker import registered, venue_snapshot, public_snapshot, run
from model_client import ModelGuardClient
from model_market_actor import (MARKETS, fetch_bar_history, parse_completed_candles,
                                request_public_candles, rdt_feature_module, rdt_window_from_markets)
from model_release import load_manifest
from model_strategy import checked_fraction


class ModelFeed:
    def __init__(self, spec):
        self.spec = spec
        self.manifest = load_manifest(spec["releaseManifest"], spec["manifestSha256"])
        if "live" not in self.manifest["policy"]["allowedModes"]:
            raise ValueError("模型版本未发布为实盘运行")
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.failure = None
        self.latest = None
        self.recent = deque(maxlen=240)
        self.thread = threading.Thread(target=self.work, name="model-feed", daemon=False)
        self.thread.start()

    def work(self):
        try:
            asyncio.run(self.service())
        except BaseException as error:
            self.failure = str(error)
        finally:
            self.ready.set()

    async def service(self):
        import pandas as pd
        spec, manifest = self.spec, self.manifest
        contract_key = spec["contractKey"]
        if contract_key != "crypto:BTC-USDT":
            raise ValueError("该原生模型执行器当前映射 BTC-USDT")
        feature_module = rdt_feature_module(manifest, spec["runnerRoot"], contract_key)
        client = ModelGuardClient(spec["releaseManifest"], spec["manifestSha256"], spec["guardBinary"],
                                  spec["runnerRoot"], contract_key=contract_key)
        self.client = client
        try:
            await client.start()
            contract = manifest["domainContracts"][contract_key]
            # Venue candle endpoints publish independently. Keep overlap beyond
            # the minimum so a one-bar delivery skew does not truncate warmup.
            rows = 480 + contract["sequenceBars"] + 8
            histories = await asyncio.gather(*[asyncio.to_thread(fetch_bar_history, instrument, "1m", 60, rows)
                                                for instrument in MARKETS])
            markets = dict(zip(MARKETS, histories))
            last_asof = 0
            while not self.stop.is_set():
                window = rdt_window_from_markets(manifest, markets, feature_module, contract_key)
                if window["asOfNs"] > last_asof:
                    request = {**window, "requestId": uuid.uuid4().hex, "releaseId": manifest["releaseId"],
                               "modelHash": manifest["model"]["sha256"], "mode": "live", "normalized": False,
                               "deadlineNs": time.time_ns() + manifest["runtime"]["timeoutMs"] * 1_000_000}
                    result = await client.predict(request)
                    value = {**result["prediction"], "targetFraction": checked_fraction(result["checked"]),
                             "completedAtNs": time.time_ns()}
                    with self.lock:
                        self.latest = value
                        self.recent.append(value)
                    last_asof = window["asOfNs"]
                    self.ready.set()
                await asyncio.to_thread(self.stop.wait, 2.0)
                if self.stop.is_set():
                    break
                # Refresh only after the next completed minute, retaining warm history.
                if time.time_ns() // 60_000_000_000 * 60_000_000_000 <= last_asof:
                    continue
                payloads = await asyncio.gather(*[asyncio.to_thread(request_public_candles, i, "1m") for i in MARKETS])
                for instrument, payload in zip(MARKETS, payloads):
                    fresh = parse_completed_candles(payload, time.time_ns(), 60, 1)
                    combined = pd.concat([markets[instrument], fresh]).sort_index()
                    markets[instrument] = combined[~combined.index.duplicated(keep="last")].iloc[-rows:]
        finally:
            await client.close()

    def snapshot(self):
        with self.lock:
            latest = dict(self.latest) if self.latest else None
            recent = list(self.recent)
        return {"releaseId": self.manifest["releaseId"], "modelHash": self.manifest["model"]["sha256"],
                "modelVersion": self.manifest["modelVersion"], "device": "cpu", "backend": "montlok.cpp v2",
                "mode": "live", "status": "failed" if self.failure else "stopped" if self.stop.is_set() else "running" if latest else "warming",
                "warmupComplete": latest is not None, "windowReady": latest is not None,
                "lastInferenceAt": latest["completedAtNs"] // 1_000_000 if latest else None,
                "latencyMs": latest.get("inferenceMs") if latest else None,
                "queueMs": latest.get("queueMs") if latest else None, "latestTarget": latest,
                "recentInferences": recent, "lastInferenceError": self.failure,
                "budgetWindow": {"limitMs": self.manifest["runtime"]["timeoutMs"],
                                 "usedMs": latest.get("inferenceMs") if latest else None}}

    def close(self):
        self.stop.set()
        self.thread.join(timeout=40)
        if self.thread.is_alive():
            raise RuntimeError("模型进程未按时停止")

    def target(self):
        with self.lock:
            return dict(self.latest) if self.latest else None


def make_strategy(node, runtime, spec, inventory, config):
    from nautilus_trader.core import nautilus_pyo3 as n
    feed = ModelFeed(spec)
    row = inventory["pairs"][0]
    identifier = n.InstrumentId.from_str(row["instrument"] + ".OKX")
    limited = config.get('maxQuoteExposureUsdt') is not None
    maximum = Decimal(row.get('maxPosition',row['tradeSize'])) - (Decimal(row.get('minPosition','0')) if limited else 0)
    lot = Decimal(row["lotSize"])
    tick = Decimal(row["tickSize"])

    class NativeModelStrategy(n.Strategy):
        def __init__(self, strategy_config):
            super().__init__(strategy_config)
            self.position = Decimal(0) if limited else Decimal(row.get('baseAvailable','0'))
            self.cash = (Decimal(config['maxQuoteExposureUsdt']) if limited else
                         Decimal(row['quoteAvailable']) * Decimal(str(config['capitalUtilization'])))
            self.last_submit = 0.0
            self.quote_count = 0

        def on_start(self):
            self.subscribe_quotes(identifier)
        def on_resume(self): self.on_start()

        def on_order_filled(self, event):
            amount = Decimal(str(event.last_qty))
            notional = amount * Decimal(str(event.last_px))
            self.position += amount if event.order_side == n.OrderSide.BUY else -amount
            self.cash += -notional if event.order_side == n.OrderSide.BUY else notional
            if str(event.commission.currency) == row["base"]:
                # Keep fee-rounding dust outside sellable strategy inventory.
                fee = max(Decimal(str(event.commission.as_decimal())),
                          amount * Decimal(row['makerFeeBps']) / 10000)
                self.position -= fee + Decimal(1).scaleb(-event.commission.currency.precision)
            elif str(event.commission.currency) == 'USDT':
                fee = max(Decimal(str(event.commission.as_decimal())),
                          notional * Decimal(row['makerFeeBps']) / 10000)
                self.cash -= fee + Decimal(1).scaleb(-event.commission.currency.precision)

        def on_quote(self, quote):
            self.quote_count += 1
            if runtime.state not in {'ACTIVE','REDUCING'}:
                return
            now = time.time_ns()
            target = feed.target()
            if not target or feed.failure or now - target["asOfNs"] > 120_000_000_000:
                if self.cache.orders_open():
                    self.cancel_all_orders(identifier)
                return
            if now - quote.ts_event > 2_000_000_000 or time.monotonic() - self.last_submit < .5:
                return
            desired = maximum * Decimal(str(target["targetFraction"]))
            delta = desired - self.position
            if runtime.state == 'REDUCING' and delta > 0:
                return
            amount = (abs(delta) // lot) * lot
            amount = min(amount,Decimal(row['tradeSize']))
            side = n.OrderSide.BUY if delta > 0 else n.OrderSide.SELL
            price = Decimal(str(quote.bid_price if delta > 0 else quote.ask_price))
            price = (price // tick) * tick
            pending = [o for o in self.cache.orders() if str(o.instrument_id) == str(identifier) and str(o.status) in {
                'INITIALIZED', 'SUBMITTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'PENDING_UPDATE', 'PENDING_CANCEL'}]
            if pending:
                if any(str(o.status) == 'ACCEPTED' and (amount < Decimal(row['minSize']) or o.side != side or
                    abs(Decimal(str(o.price)) / price - 1) * 10000 >= config['requoteThresholdBps']) for o in pending):
                    self.cancel_all_orders(identifier)
                    self.last_submit = time.monotonic()
                return
            cap = Decimal(config.get("maxQuoteExposureUsdt", row["plannedQuoteExposureUsdt"]))
            if delta > 0:
                available = max(Decimal(0), min(cap, self.cash))
                amount = min(amount, (available * Decimal("0.99") / price // lot) * lot)
            if amount < Decimal(row["minSize"]):
                return
            order = self.order_factory.limit(identifier, side, n.Quantity.from_str(format(amount, 'f')),
                                             n.Price.from_str(format(price, 'f')), post_only=True)
            self.submit_order(order)
            self.last_submit = time.monotonic()

        def on_stop(self):
            self.cancel_all_orders(identifier)
            self.unsubscribe_quotes(identifier)

    strategy_config = n.StrategyConfig(strategy_id=n.StrategyId("RDT-BTC"), order_id_tag="RDT",
                                      use_uuid_client_order_ids=True, use_hyphens_in_client_order_ids=False)
    strategy = NativeModelStrategy(strategy_config)
    def reconcile(positions, cash):
        if row['instrument'] in positions:
            strategy.position = Decimal(str(positions[row['instrument']]))
            strategy.cash = Decimal(str(cash))
    feed.reconcile_allocation = reconcile
    node.add_strategy(strategy)
    return feed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--group-id', required=True)
    parser.add_argument('--request', type=Path)
    parser.add_argument('--preview', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, '/usr/local/lib/montlok-groups/server')
    from group_runtime import LaunchRegistry
    registry = LaunchRegistry(args.registry)
    spec = registry.validate_inputs(args.group_id)
    _, spec, config = registered(args, __file__, spec)
    manifest = load_manifest(spec['releaseManifest'], spec['manifestSha256'])
    if spec.get('modelHash') != manifest['model']['sha256'] or 'live' not in manifest['policy']['allowedModes']:
        raise ValueError('实盘模型发布版本不一致')
    snapshot = asyncio.run(venue_snapshot(spec, config))
    if args.preview:
        print(json.dumps(public_snapshot(snapshot), ensure_ascii=False))
        return
    run(args, spec, config, snapshot, lambda node, runtime: make_strategy(node, runtime, spec, snapshot, config))


if __name__ == '__main__':
    main()
