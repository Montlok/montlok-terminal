#!/usr/bin/env python3
"""Root-registered native OKX live inventory maker.

The browser selects only the published group and duration. Credentials, account identity,
instruments and risk bounds come from root-owned paths in the registry. A fresh read-only venue
snapshot is hashed into the confirmation request and rechecked before the native node starts.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import threading
import time

from control import ControlServer


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_json(path, value):
    temporary = Path(str(path) + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    os.replace(temporary, path)


def floor_step(value, step):
    from decimal import Decimal, ROUND_DOWN
    value, step = Decimal(str(value)), Decimal(str(step))
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def run_deadline(request, spec, now):
    duration = request.get("durationSeconds")
    maximum = spec.get("maxDurationSeconds", 86400)
    if type(duration) is not int or not 0 <= duration <= 9007199254740991:
        raise ValueError("运行时长必须为整数秒数")
    if duration == 0:
        if spec.get("durationPolicyVersion") != 1 or maximum is not None:
            raise ValueError("运行程序尚未发布持续运行能力")
        return None
    if maximum is not None and duration > maximum:
        raise ValueError("运行时长超过已发布设置")
    return now + duration


def object_file(path, maximum=256 * 1024):
    path = Path(path)
    if not path.is_file() or path.is_symlink() or path.stat().st_size > maximum:
        raise ValueError("固定配置文件不可用")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("固定配置必须为 JSON 对象")
    return value


def registered(args, worker_path=None, spec_override=None):
    registry = object_file(args.registry)
    spec = spec_override or next((row for row in registry.get("groups", []) if row.get("id") == args.group_id), None)
    if not spec or spec.get("kind") != "live" or spec.get("environment") != "live":
        raise ValueError("实盘运行组尚未注册")
    worker_path = Path(worker_path or __file__)
    if Path(spec["workerPath"]).resolve() != worker_path.resolve() or sha(worker_path) != spec["workerSha256"]:
        raise ValueError("实盘运行程序与注册版本不一致")
    config = object_file(spec["configPath"])
    if sha(spec["configPath"]) != spec["configSha256"]:
        raise ValueError("实盘配置与注册版本不一致")
    required = {"schemaVersion", "account", "instruments", "capitalUtilization", "numLevels",
               "gridStepBps", "minimumEdgeBps", "requoteThresholdBps", "expireTimeSecs",
               "maxOrderSubmitRate", "maxOrderModifyRate", "cancelAllAfterSecs",
               "cancelAllAfterHeartbeatSecs"}
    optional = {"maxQuoteExposureUsdt", "preserveInitialInventory", "useQuoteBalance", "executionCheck",
                "executionCheckSide", "executionCheckQuantity", "inventoryFloor", "marketDataAgeSecs",
                "startupReconciliationLookbackMins"}
    account_fields = {"uid", "mainUid", "type", "acctLv", "posMode", "ip"}
    if (not required.issubset(config) or set(config) - required - optional or config["schemaVersion"] != 1
            or set(config.get("account", {})) != account_fields):
        raise ValueError("实盘配置 schema 不正确")
    instruments = config.get("instruments")
    if "marketDataAgeSecs" in config and (type(config["marketDataAgeSecs"]) not in (int, float)
            or not 0.1 <= config["marketDataAgeSecs"] <= 15):
        raise ValueError("行情时效范围为 0.1 至 15 秒")
    if ("startupReconciliationLookbackMins" in config
            and (type(config["startupReconciliationLookbackMins"]) is not int
                 or not 0 <= config["startupReconciliationLookbackMins"] <= 60)):
        raise ValueError("启动对账回看范围为 0 至 60 分钟")
    if (not isinstance(instruments, list) or not 1 <= len(instruments) <= 128 or len(set(instruments)) != len(instruments)
            or any(not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9]+-USDT", value) for value in instruments)):
        raise ValueError("实盘交易品种配置不正确")
    if (not isinstance(config["capitalUtilization"], (int, float)) or isinstance(config["capitalUtilization"], bool)
            or not 0 < config["capitalUtilization"] <= 1):
        raise ValueError("资金使用比例必须大于 0 且不超过 1")
    for key, low, high in (("numLevels", 1, 8), ("gridStepBps", 1, 100), ("minimumEdgeBps", 0, 100),
                           ("requoteThresholdBps", 1, 100), ("expireTimeSecs", 10, 300),
                           ("cancelAllAfterSecs", 10, 120), ("cancelAllAfterHeartbeatSecs", 1, 30)):
        if type(config[key]) is not int or not low <= config[key] <= high:
            raise ValueError(f"{key} 超出发布范围")
    if config["cancelAllAfterHeartbeatSecs"] * 3 >= config["cancelAllAfterSecs"]:
        raise ValueError("CAA 续期时间余量不足")
    if "maxQuoteExposureUsdt" in config:
        from decimal import Decimal, InvalidOperation
        try:
            cap = Decimal(config["maxQuoteExposureUsdt"])
        except (InvalidOperation, TypeError):
            raise ValueError("新增净敞口上限必须是 USDT 数值") from None
        if not cap.is_finite() or cap <= 0:
            raise ValueError("新增净敞口上限必须为有限正数")
        if config.get("preserveInitialInventory") is not True:
            raise ValueError("新增净敞口模式需要保留已有库存")
    elif "preserveInitialInventory" in config:
        raise ValueError("库存保留需要同时设置新增净敞口上限")
    if "useQuoteBalance" in config and type(config["useQuoteBalance"]) is not bool:
        raise ValueError("useQuoteBalance 必须是布尔值")
    if config.get("useQuoteBalance") and len(instruments) != 1:
        raise ValueError("受限测试的 USDT 只允许分配给一个交易对")
    if "executionCheck" in config and (config["executionCheck"] is not True or
            "maxQuoteExposureUsdt" not in config or len(instruments) != 1):
        raise ValueError("执行验证仅允许单一品种和明确的 1 USDT 内上限")
    if any(k in config for k in ('executionCheckSide', 'executionCheckQuantity', 'inventoryFloor')):
        if config.get('executionCheck') is not True or config.get('executionCheckSide') != 'sell':
            raise ValueError('回收测试资金须明确 sell 执行验证')
        from decimal import Decimal
        if Decimal(config.get('executionCheckQuantity', '0')) <= 0 or Decimal(config.get('inventoryFloor', '-1')) < 0:
            raise ValueError('回收数量与原有库存下限无效')
    for key in ("maxOrderSubmitRate", "maxOrderModifyRate"):
        if not isinstance(config[key], str) or not re.fullmatch(r"[1-9]\d*/\d\d:\d\d:\d\d", config[key]):
            raise ValueError(f"{key} 格式不正确")
    return registry, spec, config


async def venue_snapshot(spec, config):
    sys.path.insert(0, spec["operatorServerPath"])
    from exchange import rest
    import aiohttp
    from cryptography.fernet import Fernet
    from decimal import Decimal

    state_root = Path(spec["profileStatePath"])
    key_path, profile_path = state_root / "master.key", state_root / "profiles.enc"
    for path, maximum in ((key_path, 4096), (profile_path, 1024 * 1024)):
        if not path.is_file() or path.is_symlink() or path.stat().st_size > maximum:
            raise ValueError("账户密钥库不可用")
    state = json.loads(Fernet(key_path.read_bytes()).decrypt(profile_path.read_bytes()))
    profile = dict(state.get("profiles", {}).get(spec["profileId"], {}))
    if not profile:
        raise ValueError("实盘账户连接未发布")
    if profile["mode"] not in ("live", "live_readonly") or profile["site"] != "global":
        raise ValueError("绑定账户模式或站点与实盘注册不一致")
    async with aiohttp.ClientSession() as session:
        async def call(path, parameters=None):
            for attempt in range(4):
                try:
                    return await rest(session, profile, "GET", path, parameters)
                except RuntimeError as error:
                    if "50011" not in str(error) or attempt == 3:
                        raise
                    await asyncio.sleep(.5 * (2 ** attempt))
        account = await call("/api/v5/account/config")
        balance = await call("/api/v5/account/balance")
        orders = await call("/api/v5/trade/orders-pending", {"instType": "SPOT"})
        account = account["data"][0]
        identity = {key: account.get(key) for key in ("uid", "mainUid", "type", "acctLv", "posMode", "perm", "ip")}
        expected = config["account"]
        if any(str(identity.get(key)) != str(expected[key]) for key in ("uid", "mainUid", "type", "acctLv", "posMode")):
            raise ValueError("账户身份与发布配置不一致")
        permissions = set(str(identity["perm"] or "").split(","))
        if "read_only" not in permissions or "trade" not in permissions or identity.get("ip") != expected["ip"]:
            raise ValueError("账户权限或 IP 绑定与发布配置不一致")
        owned = {}
        for row in balance["data"][0].get("details", []):
            available = Decimal(row.get("availBal") or "0")
            if available > 0:
                owned[row["ccy"]] = available
        pairs = []
        for instrument in config["instruments"]:
            base = instrument.split("-", 1)[0]
            metadata = await call("/api/v5/public/instruments", {"instType": "SPOT", "instId": instrument})
            ticker = await call("/api/v5/market/ticker", {"instId": instrument})
            book = await call("/api/v5/market/books", {"instId": instrument, "sz": 20})
            fee = await call("/api/v5/account/trade-fee", {"instType": "SPOT", "instId": instrument})
            meta = metadata["data"][0]
            if meta.get("state") != "live" or meta.get("baseCcy") != base or meta.get("quoteCcy") != "USDT":
                raise ValueError(f"{instrument} 当前不可用于 USDT 现货交易")
            depth = book["data"][0]
            bid, ask = Decimal(depth["bids"][0][0]), Decimal(depth["asks"][0][0])
            mid = (bid + ask) / 2
            base_available = owned.get(base, Decimal(0))
            quote_available = (owned.get("USDT", Decimal(0))
                               if instrument == "BTC-USDT" or config.get("useQuoteBalance") else Decimal(0))
            quote_budget = quote_available * Decimal(str(config["capitalUtilization"]))
            exposure_cap = config.get("maxQuoteExposureUsdt")
            if exposure_cap is not None:
                # Leave 5% below the published hard cap so a small move between preflight and
                # order admission cannot make a nominally 1 USDT test cross that boundary.
                quote_budget = min(quote_budget, Decimal(exposure_cap) * Decimal("0.95"))
            minimum_position = base_available if config.get("preserveInitialInventory") else Decimal(0)
            maximum = base_available + quote_budget / ask
            levels = config["numLevels"]
            capacity = maximum - minimum_position
            trade_size = floor_step(capacity / levels, meta["lotSz"])
            minimum = Decimal(meta["minSz"])
            if trade_size < minimum:
                if capacity < minimum:
                    continue
                trade_size = minimum
            maker = abs(Decimal(fee["data"][0]["maker"]))
            configured_step = Decimal(config["gridStepBps"])
            fee_floor = maker * 10_000 + Decimal(config["minimumEdgeBps"])
            grid_step = max(configured_step, fee_floor)
            tick = ticker["data"][0]
            pairs.append({"instrument": instrument, "base": base, "baseAvailable": str(base_available),
                          "quoteAvailable": str(quote_available), "bid": str(bid), "ask": str(ask),
                          "spreadBps": str((ask - bid) / mid * 10_000), "makerFeeBps": str(maker * 10_000),
                          "takerFeeBps": str(abs(Decimal(fee['data'][0]['taker'])) * 10000),
                          "volume24hQuote": tick.get("volCcy24h"), "tickSize": meta["tickSz"],
                          "lotSize": meta["lotSz"], "minSize": meta["minSz"], "tradeSize": str(trade_size),
                          "initialPosition": str(base_available), "minPosition": str(minimum_position),
                          "maxPosition": str(base_available + trade_size * levels
                                             if config.get("preserveInitialInventory")
                                             else maximum),
                          "quoteExposureCapUsdt": str(exposure_cap) if exposure_cap is not None else None,
                          "plannedQuoteExposureUsdt": str(trade_size * levels * ask),
                          "gridStepBps": str(grid_step)})
        open_orders = [row for row in orders["data"] if row.get("instId") in config["instruments"]]
        snapshot = {"schemaVersion": 1, "profileId": spec["profileId"], "account": identity,
                    "totalEqUsd": balance["data"][0].get("totalEq"), "pairs": pairs,
                    "excluded": [{"asset": ccy, "available": str(value),
                                  "reason": "余额低于最小下单量" if ccy == "XMU" else "未在发布交易范围"}
                                 for ccy, value in owned.items() if ccy not in {"USDT", *(row["base"] for row in pairs)}],
                    "openManagedPairOrders": len(open_orders), "observedAtMs": int(time.time() * 1000)}
        comparable = {"schemaVersion": snapshot["schemaVersion"], "profileId": snapshot["profileId"],
                      "account": snapshot["account"], "openManagedPairOrders": snapshot["openManagedPairOrders"],
                      "allocations": [{key: row[key] for key in ("instrument", "base", "baseAvailable",
                          "quoteAvailable", "lotSize", "minSize")} for row in pairs]}
        snapshot["inventoryHash"] = hashlib.sha256(encode(comparable).encode()).hexdigest()
        snapshot["credentials"] = {key: profile[key] for key in ("apiKey", "secret", "passphrase")}
        return snapshot


def public_snapshot(snapshot):
    return {key: value for key, value in snapshot.items() if key != "credentials"}


def owned_orders(node):
    # Reconciliation imports historical venue fills as EXTERNAL orders. They
    # must not count as current-run submissions, fills, inventory or completion.
    return [order for order in node.cache.orders() if not str(order.strategy_id).startswith('EXTERNAL')]


def risk_notional_limits(snapshot):
    """Return run-frozen absolute caps, never caps derived from a stale quote.

    Continuous executors size every order against the same absolute USDT cap
    before risk admission. Single-allocation runs retain their finite,
    snapshot-sized bound because they complete against the startup market.
    """
    from decimal import Decimal
    settings = snapshot.get("executionSettings") or {}
    if settings.get("mode") in {"continuous_quotes", "continuous_rebalance"}:
        cap = Decimal(str(settings["maxOrderNotionalUsdt"]))
        return {row["instrument"] + ".OKX": cap for row in snapshot["pairs"]}
    return {row["instrument"] + ".OKX": Decimal(row["tradeSize"]) * Decimal(row["ask"]) * Decimal("1.02")
            for row in snapshot["pairs"]}


def build_node(snapshot, config, *, add_strategies=True):
    from decimal import Decimal
    from nautilus_trader.core import nautilus_pyo3 as n

    credentials = snapshot["credentials"]
    account = snapshot["account"]
    trader_id = n.TraderId("MONTLOK-HFT")
    account_id = n.AccountId("OKX-HFT")
    data = n.OKXDataClientConfig(instrument_types=[n.OKXInstrumentType.SPOT], environment=n.OKXEnvironment.LIVE,
        region=n.OKXRegion.GLOBAL, api_key=credentials["apiKey"], api_secret=credentials["secret"],
        api_passphrase=credentials["passphrase"], http_timeout_secs=15, max_retries=2,
        book_stale_threshold_secs=5, book_snapshot_timeout_secs=3)
    execution = n.OKXExecClientConfig(trader_id, account_id, instrument_types=[n.OKXInstrumentType.SPOT],
        environment=n.OKXEnvironment.LIVE, region=n.OKXRegion.GLOBAL, api_key=credentials["apiKey"],
        api_secret=credentials["secret"], api_passphrase=credentials["passphrase"],
        allow_leveraged_products=False, expected_uid=account["uid"], expected_main_uid=account["mainUid"],
        expected_account_type=int(account["type"]), expected_account_level=int(account["acctLv"]),
        expected_position_mode=n.OKXPositionMode.NET_MODE, require_ip_binding=True,
        cancel_all_after_secs=config["cancelAllAfterSecs"],
        cancel_all_after_heartbeat_secs=config["cancelAllAfterHeartbeatSecs"],
        reconnect_policy=n.OKXReconnectPolicy.RECONCILE, reconnect_reconcile_lookback_secs=60,
        use_spot_margin=False, http_timeout_secs=15, max_retries=2)
    limits = risk_notional_limits(snapshot)
    startup_reconciliation_mins = config.get("startupReconciliationLookbackMins", 0)
    builder = n.LiveNode.builder("MONTLOK-HFT-LIVE", trader_id, n.Environment.LIVE)
    builder.with_logging(n.LoggerConfig(stdout_level=n.LogLevel.INFO, buffered_stdout=True))
    builder.with_reconciliation(True).with_reconciliation_lookback_mins(startup_reconciliation_mins)
    builder.with_timeout_connection(45).with_timeout_reconciliation(30).with_timeout_portfolio(15).with_timeout_disconnection_secs(15)
    builder.with_data_engine_config(n.LiveDataEngineConfig(validate_data_sequence=True, emit_quotes_from_book=True))
    builder.with_risk_engine_config(n.LiveRiskEngineConfig(bypass=False,
        max_order_submit_rate=config["maxOrderSubmitRate"], max_order_modify_rate=config["maxOrderModifyRate"],
        max_notional_per_order=limits, max_market_data_age_secs=config.get('marketDataAgeSecs', 2), market_order_price_bound_bps=25))
    builder.with_exec_engine_config(n.LiveExecEngineConfig(reconciliation=True,
        reconciliation_lookback_mins=startup_reconciliation_mins,
        inflight_check_interval_ms=500, inflight_check_threshold_ms=2000, open_check_interval_secs=2,
        open_check_open_only=True, position_check_interval_secs=30, generate_missing_orders=False,
        filter_unclaimed_external_orders=False))
    builder.add_data_client("OKX", n.OKXDataClientFactory(), data)
    builder.add_exec_client("OKX", n.OKXExecutionClientFactory(), execution)
    node = builder.build()
    for index, row in enumerate(snapshot["pairs"] if add_strategies else [], 1):
        strategy = n.GridMarketMakerConfig(n.InstrumentId.from_str(row["instrument"] + ".OKX"),
            n.Quantity.from_str(row["maxPosition"]), strategy_id=n.StrategyId(f"HFT-{row['base']}"),
            order_id_tag=f"{index:03d}", trade_size=n.Quantity.from_str(row["tradeSize"]),
            num_levels=config["numLevels"], grid_step_bps=math.ceil(float(row["gridStepBps"])),
            skew_factor=0.0, requote_threshold_bps=config["requoteThresholdBps"],
            expire_time_secs=config["expireTimeSecs"], on_cancel_resubmit=True,
            initial_position=n.Quantity.from_str(row["initialPosition"]),
            min_position=n.Quantity.from_str(row["minPosition"]), close_positions_on_stop=False,
            use_uuid_client_order_ids=True, use_hyphens_in_client_order_ids=False)
        node.add_builtin_strategy("GridMarketMaker", strategy)
    return node


class Runtime:
    def __init__(self, directory, request, snapshot, config):
        self.directory, self.request, self.inventory, self.config = directory, request, snapshot, config
        self.lock, self.stop = threading.Lock(), threading.Event()
        self.state = "STARTING"
        self.model_feed = None
        self.controls = None
        self.allocated_positions = {}
        self.allocated_cash = 0.0
        self.peak_nav = None
        self.max_drawdown = 0.0
        self.fee_overrides = {}
        self.snapshot = {"ok": True, "pid": os.getpid(), "environment": "live", "tradingState": self.state,
                         "profileId": snapshot["profileId"], "inventory": public_snapshot(snapshot),
                         "marketReady": False, "executionReady": False,
                         "connected": {"data": False, "exec": False},
                         "ordersEnabled": False, "pollEvents": 0, "pollCycles": 0}

    def status(self, _request=None):
        with self.lock:
            model = self.model_feed.snapshot() if self.model_feed else None
            if model:
                self.snapshot["model"] = model
            return dict(self.snapshot)

    def request_stop(self, _request=None):
        self.stop.set()
        with self.lock:
            self.state = "STOPPING"
            self.snapshot["tradingState"] = self.state
        return {"tradingState": self.state}

    def update(self, node, started):
        from nautilus_trader.core import nautilus_pyo3 as n
        quotes = []
        for row in self.inventory["pairs"]:
            quote = node.cache.quote(n.InstrumentId.from_str(row["instrument"] + ".OKX"))
            quotes.append({"instrument": row["instrument"], "tsEvent": quote.ts_event if quote else None,
                           "bid": str(quote.bid_price) if quote else None, "ask": str(quote.ask_price) if quote else None})
        account = node.cache.account_for_venue(n.Venue("OKX"))
        executor=getattr(getattr(self.model_feed,'strategy',None),'executor',None)
        native_orders = ([order for oid in executor.order_ids if (order:=node.cache.order(oid)) is not None]
                         if executor is not None else owned_orders(node))
        orders_denied = sum(str(order.status) == "DENIED" for order in native_orders)
        orders_rejected = sum(str(order.status) == "REJECTED" for order in native_orders)
        orders_accepted = sum(order.venue_order_id is not None for order in native_orders)
        orders = [{"id": str(order.client_order_id), "clientOrderId": str(order.client_order_id),
                   "venueOrderId": str(order.venue_order_id) if order.venue_order_id else None,
                   "strategy": str(order.strategy_id), "instrument": str(order.instrument_id),
                   "side": str(order.side), "quantity": str(order.quantity),
                   "filledQuantity": str(order.filled_qty), "leavesQuantity": str(order.leaves_qty),
                   "price": str(order.price) if order.price else None,
                   "averagePrice": str(order.avg_px) if order.avg_px is not None else None,
                   "status": str(order.status), "time": order.ts_init} for order in native_orders[-1000:]]
        fills = []
        last_order_error = None
        for order in native_orders[-1000:]:
            for event in order.events():
                if str(order.status) in {'DENIED', 'REJECTED'} and hasattr(event, 'reason'):
                    last_order_error = str(event.reason)
                if not hasattr(event, 'trade_id'):
                    continue
                fills.append({'id': str(event.trade_id), 'orderId': str(event.client_order_id),
                    'venueOrderId': str(event.venue_order_id), 'instrument': str(event.instrument_id),
                    'side': str(event.order_side), 'quantity': str(event.last_qty), 'price': str(event.last_px),
                    'fee': str(event.commission.as_decimal()), 'feeCurrency': str(event.commission.currency),
                    'time': event.ts_event, 'source': 'okx_live_fill'})
        for client_id, commission in self.fee_overrides.items():
            matching = [fill for fill in fills if fill['orderId'] == client_id]
            if len(matching) == 1:
                matching[0].update(fee=commission['amount'], feeCurrency=commission['currency'])
        marks = {row["instrument"]: (float(quote["bid"] or row["bid"]) + float(quote["ask"] or row["ask"])) / 2
                 for row, quote in zip(self.inventory["pairs"], quotes)}
        limited = self.config.get("maxQuoteExposureUsdt") is not None
        quantities = {row["instrument"]: (max(0.0, float(row['baseAvailable']) - float(self.config['inventoryFloor']))
                      if self.config.get('executionCheckSide') == 'sell' else 0.0 if limited else float(row['baseAvailable']))
                      for row in self.inventory["pairs"]}
        initial_assets = sum(quantities[row["instrument"]] * (float(row["bid"]) + float(row["ask"])) / 2
                             for row in self.inventory["pairs"])
        capital = (float(self.config["maxQuoteExposureUsdt"]) if limited else initial_assets + sum(
            max(0.0, float(row["maxPosition"]) - float(row["baseAvailable"])) * float(row["ask"])
            for row in self.inventory["pairs"]))
        if self.inventory.get('allocationCapitalUsdt') is not None:
            capital = float(self.inventory['allocationCapitalUsdt'])
        cash, fees = capital - initial_assets, 0.0
        for order in native_orders:
            instrument = str(order.instrument_id).removesuffix('.OKX')
            if instrument not in quantities:
                continue
            filled = float(order.filled_qty)
            if filled:
                direction = 1 if str(order.side) == 'BUY' else -1
                quantities[instrument] += direction * filled
                cash -= direction * filled * float(order.avg_px)
            exact = self.fee_overrides.get(str(order.client_order_id))
            commissions = ({exact['currency']: float(exact['amount'])} if exact else
                           {str(ccy): float(value.as_decimal()) for ccy, value in order.commissions().items()})
            for currency, value in commissions.items():
                if str(currency) == 'USDT':
                    cash -= value
                    fees += value
                elif str(currency) == instrument.split('-')[0]:
                    quantities[instrument] -= value
                    fees += value * float(order.avg_px or marks[instrument])
        self.allocated_positions = quantities
        if executor is not None:
            quantities={key.removesuffix('.OKX'):float(value) for key,value in executor.positions.items()}
            cash=float(executor.cash);fees=float(executor.fees)
            fills=list(executor.fills)
            orders_accepted=executor.counts['accepted'];orders_rejected=executor.counts['rejected']
            executor.flush_events(self.directory)
            self.allocated_positions = quantities
        self.allocated_cash = cash
        nav = cash + sum(qty * marks[instrument] for instrument, qty in quantities.items())
        self.peak_nav = max(self.peak_nav or capital, nav)
        self.max_drawdown = min(self.max_drawdown, nav / self.peak_nav - 1 if self.peak_nav else 0)
        positions = [{"strategy": self.request['groupId'], "instrument": instrument + '.OKX',
                      "side": 'Long', "quantity": str(qty), "markPrice": str(marks[instrument]),
                      "notional": qty * marks[instrument], "margin": 0}
                     for instrument, qty in quantities.items() if abs(qty) > 1e-12]
        model = self.model_feed.snapshot() if self.model_feed else None
        with self.lock:
            if model:
                self.snapshot["model"] = model
                self.snapshot["warmupComplete"] = model.get("warmupComplete") is True
            allocation = self.model_feed.allocation_status() if self.model_feed and hasattr(self.model_feed, 'allocation_status') else None
            if allocation:
                self.snapshot['allocation'] = allocation
            if self.model_feed and hasattr(self.model_feed,'execution_status'):
                self.snapshot['execution'] = self.model_feed.execution_status()
            self.snapshot.update(observedAt=time.time(), uptimeSeconds=time.monotonic() - started,
                tradingState=self.state, marketReady=node.is_running and all(row["tsEvent"] for row in quotes), quotes=quotes,
                executionReady=node.is_running and account is not None, ordersTotal=executor.counts['submit'] if executor is not None else len(native_orders),
                ordersOpen=len(executor.reservations) if executor is not None else sum(str(o.status) in {'INITIALIZED', 'SUBMITTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'PENDING_CANCEL', 'PENDING_UPDATE'} for o in native_orders), fillsTotal=executor.counts['fills'] if executor is not None else len(fills),
                ordersAccepted=orders_accepted, ordersDenied=orders_denied, ordersRejected=orders_rejected,
                lastOrderError=last_order_error,
                ordersEnabled=node.is_running and self.state == "ACTIVE" and
                              (model is None or model.get("warmupComplete") is True))
            if self.controls:
                self.snapshot.update(self.controls.result(), allocatedPositions=positions)
            self.snapshot.update(observed_at=datetime.now(timezone.utc).isoformat(),
                run_id=self.request["runId"], group_id=self.request["groupId"],
                trading_state=self.state, instruments_seen=sum(row["tsEvent"] is not None for row in quotes),
                capital_usdt=capital, nav_usdt=nav, pnl_usdt=nav-capital, fees_usdt=fees,
                max_drawdown=self.max_drawdown)
            self.snapshot['feeSource'] = 'okx_order_receipt' if self.fee_overrides else 'native_stream'
            self.snapshot["connected"] = {"data": self.snapshot["marketReady"], "exec": self.snapshot["executionReady"]}
            atomic_json(self.directory / "status.json", self.snapshot)
            with (self.directory / 'equity.jsonl').open('a') as stream:
                stream.write(encode({key: self.snapshot[key] for key in
                    ('observed_at', 'nav_usdt', 'pnl_usdt', 'max_drawdown', 'fees_usdt')}) + '\n')
            atomic_json(self.directory / "view.json", {"schemaVersion": 1, "runId": self.request["runId"],
                "groupId": self.request["groupId"], "observed_at": self.snapshot["observed_at"],
                "tradingState": self.state, "completed": False, "positions": positions,
                "orders": orders, "fills": fills, "ordersTotal": self.snapshot["ordersTotal"],
                "ordersAccepted": orders_accepted, "ordersDenied": orders_denied, "ordersRejected": orders_rejected,
                "fillsTotal": self.snapshot["fillsTotal"], "eventCount": self.snapshot["pollEvents"],
                "strategies": [{"id": self.request['groupId'], "mode": self.state,
                    "runId": self.request["runId"], "fills": self.snapshot["fillsTotal"],
                    "artifact": self.inventory.get('strategy', 'inventory-grid-v1'), 'allocation': allocation}],
                "connected": {"data": self.snapshot["marketReady"], "exec": self.snapshot["executionReady"]},
                "health": {}, "mode": "live", "model": model, "inventory": public_snapshot(self.inventory)})


def control_thread(runtime, finished):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    from live_controls import ACTIONS
    control = ControlServer(runtime.directory / "control.sock", {"status": runtime.status, "stop": runtime.request_stop,
        **{action:runtime.controls.request for action in ACTIONS}})
    loop.run_until_complete(control.start())
    async def wait():
        while not finished.is_set():
            await asyncio.sleep(.05)
        await control.stop()
    loop.run_until_complete(wait())
    loop.close()


def execution_check(node, runtime):
    """One IOC limit order, reserved below the account's published test cap."""
    from decimal import Decimal
    from nautilus_trader.core import nautilus_pyo3 as n
    row = runtime.inventory['pairs'][0]
    identifier = n.InstrumentId.from_str(row['instrument'] + '.OKX')
    cap = Decimal(runtime.config['maxQuoteExposureUsdt'])

    class ExecutionCheck(n.Strategy):
        def __init__(self, config):
            super().__init__(config)
            self.sent = False

        def on_start(self):
            self.subscribe_quotes(identifier)
        def on_resume(self): self.on_start()

        def on_quote(self, quote):
            if runtime.state != 'ACTIVE' or self.sent or time.time_ns() - quote.ts_event > 2_000_000_000:
                return
            selling = runtime.config.get('executionCheckSide') == 'sell'
            price = (Decimal(str(quote.bid_price)) - Decimal(row['tickSize']) if selling else
                     Decimal(str(quote.ask_price)) + Decimal(row['tickSize']))
            lot = Decimal(row['lotSize'])
            requested = Decimal(runtime.config['executionCheckQuantity']) if selling else Decimal(row['tradeSize'])
            if selling and requested > Decimal(row['baseAvailable']) - Decimal(runtime.config['inventoryFloor']):
                raise ValueError('回收数量超过本次测试新增库存')
            quantity = min(requested, (cap * Decimal('0.99') / price // lot) * lot)
            if quantity < Decimal(row['minSize']):
                return
            self.sent = True
            order = self.order_factory.limit(identifier, n.OrderSide.SELL if selling else n.OrderSide.BUY,
                n.Quantity.from_str(format(quantity, 'f')), n.Price.from_str(format(price, 'f')),
                time_in_force=n.TimeInForce.IOC, post_only=False)
            self.submit_order(order)

        def on_stop(self):
            self.cancel_all_orders(identifier)
            self.unsubscribe_quotes(identifier)

    node.add_strategy(ExecutionCheck(n.StrategyConfig(strategy_id=n.StrategyId('EXEC-CHECK'),
        use_uuid_client_order_ids=True, use_hyphens_in_client_order_ids=False)))


async def reconcile_fees(spec, snapshot, node):
    """Preserve venue fee decimals beyond a currency's native display precision."""
    from decimal import Decimal
    import aiohttp
    sys.path.insert(0, spec['operatorServerPath'])
    from exchange import rest
    profile = {**snapshot['credentials'], 'mode': 'live_readonly', 'site': 'global'}
    result = {}
    filled = [o for o in owned_orders(node) if float(o.filled_qty) > 0 and o.venue_order_id]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
        for order in filled[-20:]:
            response = await rest(session, profile, 'GET', '/api/v5/trade/order', {
                'instId': str(order.instrument_id).removesuffix('.OKX'), 'ordId': str(order.venue_order_id)})
            row = response['data'][0]
            if Decimal(row['accFillSz']) != Decimal(str(order.filled_qty)):
                raise ValueError('交易所成交数量尚未对齐')
            result[str(order.client_order_id)] = {'currency': row['feeCcy'], 'amount': str(-Decimal(row['fee']))}
    return result


def run(args, spec, config, snapshot, strategy_factory=None):
    if config.get('executionCheck'):
        strategy_factory = execution_check
    request = object_file(args.request)
    allowed = {"groupId", "action", "registryVersion", "durationSeconds", "runId", "createdAt", "inventoryHash", "executionSettings"}
    if set(request) - allowed or request.get("action") != "start" or request.get("groupId") != args.group_id:
        raise ValueError("实盘启动请求与发布配置不一致")
    if request.get("inventoryHash") != snapshot["inventoryHash"] or snapshot["openManagedPairOrders"]:
        raise ValueError("账户余额或活动委托已变化，请重新检查配置")
    stop_at = run_deadline(request, spec, time.time())
    directory = args.request.parent
    manifest = {"schemaVersion": 1, "run_id": request["runId"], "group_id": request["groupId"],
                "environment": "live", "mode": "live", "execution": "okx_live", "account_id": spec["profileId"],
                "account_type": "CASH", "leverage": 1,
                "instruments": {row["instrument"] + ".OKX": {"inventoryMaker": True} for row in snapshot["pairs"]},
                "run_started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "stop_at_unix": stop_at,
                "duration_seconds": request["durationSeconds"],
                "termination_policy": "manual" if stop_at is None else "duration",
                "orders_enabled": True, "inventory": public_snapshot(snapshot), "worker_sha256": sha(__file__),
                "execution_config": config,
                "config_sha256": spec["configSha256"], "market_data": "okx_public_live_websocket_quotes",
                "strategy_description": "多层 post-only 库存报价", "rebalance_policy": "quote_event_inventory_grid",
                "fee_assumption": {"maker_bps": 8, "taker_bps": 10},
                "pid": os.getpid(), "process_create_time": __import__("psutil").Process().create_time()}
    if snapshot.get('allocationCapitalUsdt') is not None:
        manifest.update(capital_usdt=float(snapshot['allocationCapitalUsdt']),
            strategy_description=spec.get('description'), rebalance_policy='frozen_signals_initial_allocation',
            signal_as_of=snapshot.get('signalAsOf'), signals_sha256=spec.get('signalsSha256'),
            instruments={r['instrument']+'.OKX': {'weight': r.get('weight', 0), 'sector': r.get('sector')}
                         for r in snapshot['pairs']})
    atomic_json(directory / "manifest.json", manifest)
    if snapshot.get('executionSettings'):
        from execution_options import describe
        policy=describe(snapshot['executionSettings'])
        manifest.update(execution_settings=snapshot['executionSettings'],rebalance_policy=policy['kind'],
                        strategy_description=policy['description'])
        atomic_json(directory / "manifest.json", manifest)
    if spec.get("modelHash"):
        manifest.update(model_sha256=spec["modelHash"], model_release=spec["modelManifest"]["releaseId"],
                        model_version=spec["modelManifest"]["modelVersion"], manifest_sha256=spec["manifestSha256"])
        atomic_json(directory / "manifest.json", manifest)
    runtime = Runtime(directory, request, snapshot, config)
    if spec.get("modelHash"):
        runtime.snapshot.update(modelHash=spec["modelHash"], manifestSha256=spec["manifestSha256"], ordersEnabled=False)
    node = build_node(snapshot, config, add_strategies=strategy_factory is None)
    if strategy_factory:
        runtime.model_feed = strategy_factory(node, runtime)
    from live_controls import attach_controls
    runtime.controls = attach_controls(node, runtime)
    finished = threading.Event()
    thread = threading.Thread(target=control_thread, args=(runtime, finished), daemon=False)
    thread.start()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: runtime.stop.set())
    started = time.monotonic()
    deadline = run_deadline(request, spec, started)
    error = None
    handled_denials = 0
    try:
        node.start()
        runtime.state = "ACTIVE"
        last_write = 0.0
        while not runtime.stop.is_set() and (deadline is None or time.monotonic() < deadline):
            runtime.controls.process()
            count = node.poll()
            if runtime.model_feed and hasattr(runtime.model_feed, 'poll'):
                runtime.model_feed.poll()
            with runtime.lock:
                runtime.snapshot["pollCycles"] += 1
                runtime.snapshot["pollEvents"] += count
            now = time.monotonic()
            if now - last_write >= .25:
                runtime.update(node, started)
                # Reductions always use a ledger refreshed after the latest
                # native fills; no repeated sell against an older snapshot.
                runtime.controls.poll()
                denied = runtime.status().get('ordersDenied',0)
                if denied > handled_denials:
                    handled_denials = denied
                    if snapshot.get('executionSettings') and snapshot['executionSettings']['mode']!='initial_allocation':
                        runtime.controls.apply({'command':'halt'})
                        runtime.snapshot['riskHaltReason']=runtime.status().get('lastOrderError') or '引擎拒绝送单'
                    else:
                        raise RuntimeError(runtime.status().get('lastOrderError') or "引擎拒绝送单")
                if config.get('executionCheck') and runtime.status().get('ordersRejected', 0):
                    raise RuntimeError(runtime.status().get('lastOrderError') or 'OKX 拒绝订单')
                if runtime.model_feed and runtime.model_feed.failure:
                    raise RuntimeError(runtime.model_feed.failure)
                if config.get("executionCheck") and runtime.status().get("fillsTotal", 0):
                    runtime.stop.set()
                last_write = now
            if not count:
                time.sleep(.001)
    except BaseException as failure:
        error = repr(failure)
        runtime.state = "ERROR"
        raise
    finally:
        runtime.controls.close()
        try:
            if node.is_running:
                node.stop()
            try:
                runtime.fee_overrides = asyncio.run(asyncio.wait_for(reconcile_fees(spec, snapshot, node), 5))
            except Exception as settlement_error:
                runtime.snapshot['settlementError'] = str(settlement_error)
            runtime.state = "ERROR" if error else "STOPPED"
            runtime.update(node, started)
        finally:
            node.dispose()
            if runtime.model_feed:
                runtime.model_feed.close()
            finished.set()
            thread.join(timeout=15)
            final = {**runtime.status(), "run_id": request["runId"], "group_id": request["groupId"],
                     "trading_state": runtime.state,
                     "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "error": error,
                     "deadline_reached": deadline is not None and time.monotonic() >= deadline}
            final["validation_complete"] = bool(config.get("executionCheck") and
                final.get("fillsTotal", 0) and final.get("ordersOpen") == 0 and error is None)
            atomic_json(directory / "final.json", final)
            view = object_file(directory / "view.json", 2 * 1024 * 1024) if (directory / "view.json").exists() else {
                "schemaVersion": 1, "runId": request["runId"], "groupId": request["groupId"],
                "positions": [], "orders": [], "fills": [], "ordersTotal": 0, "fillsTotal": 0,
                "eventCount": 0, "strategies": [], "connected": {"data": False, "exec": False},
                "health": {}, "mode": "live",
            }
            view.update(completed=True, tradingState=runtime.state, observed_at=final["observed_at"])
            atomic_json(directory / "view.json", view)
            for name, header in (("orders.csv", ["client_order_id", "instrument", "status"]),
                                 ("fills.csv", ["trade_id", "instrument", "quantity", "price", "commission"]),
                                 ("positions.csv", ["instrument", "quantity"]),
                                 ("account.csv", ["currency", "total", "free", "locked"])):
                with (directory / name).open("w", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(header)
                    if name == "orders.csv":
                        writer.writerows((row["clientOrderId"], row["instrument"], row["status"]) for row in view["orders"])
                    elif name == "fills.csv":
                        writer.writerows((row["id"], row["instrument"], row["quantity"], row["price"], "") for row in view["fills"])
                    elif name == "positions.csv":
                        writer.writerows((row["instrument"], row["quantity"]) for row in view["positions"])
                    elif name == "account.csv":
                        writer.writerows((row["base"], row["baseAvailable"], row["baseAvailable"], "0") for row in snapshot["pairs"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--connect-only", action="store_true")
    args = parser.parse_args()
    _, spec, config = registered(args)
    snapshot = asyncio.run(venue_snapshot(spec, config))
    if args.preview:
        print(json.dumps(public_snapshot(snapshot), ensure_ascii=False, allow_nan=False))
        return
    if args.build_only:
        node = build_node(snapshot, config)
        node.dispose()
        print(json.dumps({"built": True, "pairs": len(snapshot["pairs"]),
                          "inventoryHash": snapshot["inventoryHash"], "ordersCreated": 0}))
        return
    if args.connect_only:
        from nautilus_trader.core import nautilus_pyo3 as n
        node = build_node(snapshot, config, add_strategies=False)
        started = time.monotonic()
        try:
            node.start()
            while time.monotonic() - started < 5:
                node.poll()
                time.sleep(.002)
            account = node.cache.account_for_venue(n.Venue("OKX"))
            if account is None:
                raise RuntimeError("实盘执行账户未进入节点缓存")
            print(json.dumps({"connected": True, "accountId": str(account.id),
                "ordersCreated": node.cache.orders_total_count(), "elapsedSeconds": time.monotonic() - started}))
        finally:
            if node.is_running:
                node.stop()
            node.dispose()
        return
    if args.request is None:
        parser.error("--request is required for a run")
    run(args, spec, config, snapshot)


if __name__ == "__main__":
    main()
