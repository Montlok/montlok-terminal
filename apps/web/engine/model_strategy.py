"""Nautilus model-target strategy, with prediction work outside event callbacks.

The upstream market actor supplies completed-bar raw feature windows. The Rust
guard owns normalization, prediction validation, and target policy. Shadow mode
publishes targets only. Sandbox orders require actual SandboxExecutionClient
route inspection, not a mode label or a caller-provided boolean.
"""
from __future__ import annotations

import asyncio
from collections import deque
from decimal import Decimal
import math
import time
import uuid
from datetime import datetime, timezone


def checked_fraction(result):
    value = result.get("targetFraction")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("Rust guard must provide a finite spot long/cash targetFraction in [0,1]")
    return value


def make_model_strategy(node, client, *, mode, instrument_id, budget_usdt, loop=None, feature_topic=None,
                        model_instrument=None, model_domain=None):
    """Create after node configuration; add to trader before node.build().

    Start client asynchronously before node.run_async(). On shutdown quiesce the
    strategy and await client.close() before disposing the Nautilus node.
    """
    if mode not in {"shadow", "sandbox"} or mode not in client.manifest["policy"]["allowedModes"]:
        raise ValueError("This model release does not allow the requested non-live mode")
    if not math.isfinite(float(budget_usdt)) or float(budget_usdt) <= 0:
        raise ValueError("Positive finite virtual budget required")
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.model.enums import OrderSide
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.trading.strategy import Strategy

    loop = loop or asyncio.get_running_loop()
    identifier = InstrumentId.from_str(instrument_id)
    if client.manifest["family"] == "recent_btc_gru":
        if instrument_id != "BTC-USDT.OKX":
            raise ValueError("GRU execution mapping is published for BTC-USDT.OKX only")
        model_instrument = "BTC-USDT"
    elif not model_instrument or not model_domain or f"{model_domain}:{model_instrument}" not in client.manifest["domainContracts"]:
        raise ValueError("RDT strategy needs an explicit published input-domain to execution-instrument mapping")
    topic = feature_topic or f"model.features.{client.manifest['releaseId']}"

    class ReleasedModelStrategy(Strategy):
        def __init__(self):
            super().__init__(StrategyConfig(order_id_tag="MODEL"))
            self.model_mode = mode
            self.execution_instrument_id = identifier
            self.model_targets = deque(maxlen=500)
            self.inference_errors = 0
            self.queue_rejected = 0
            self.last_inference_error = None
            self.pending_predictions = set()
            self.last_asof_ns = 0
            self.accepting = False
            self.instrument = None
            self.managed_fills = deque(maxlen=500)
            self.fills = 0
            self.managed_events = 0
            self.fees_usdt = 0.0
            self.fees_unconverted = False

        def on_event(self, event):
            self.managed_events += 1
            if type(event).__name__ == "OrderFilled":
                from nautilus_trader.model.enums import order_side_to_str
                self.fills += 1
                fee = float(event.commission.as_decimal())
                if str(event.commission.currency) == "USDT":
                    self.fees_usdt += fee
                elif fee:
                    self.fees_unconverted = True
                self.managed_fills.append({"id": str(event.trade_id), "orderId": str(event.client_order_id),
                    "strategy": str(event.strategy_id), "instrument": str(event.instrument_id),
                    "side": order_side_to_str(event.order_side), "quantity": str(event.last_qty),
                    "price": str(event.last_px), "fee": str(fee), "feeCurrency": str(event.commission.currency),
                    "time": datetime.fromtimestamp(event.ts_event / 1e9, timezone.utc).isoformat()})

        def assert_sandbox_route(self, order):
            clients = node.kernel.exec_engine.get_clients_for_orders([order])
            if len(clients) != 1 or type(next(iter(clients))).__module__ != "nautilus_trader.adapters.sandbox.execution":
                raise RuntimeError("Model execution route is not Nautilus SandboxExecutionClient")

        def on_start(self):
            self.instrument = self.cache.instrument(identifier)
            if self.instrument is None:
                raise RuntimeError("Model execution instrument is not loaded")
            if mode == "sandbox":
                if not isinstance(self.instrument, CurrencyPair) or str(self.instrument.quote_currency) != "USDT":
                    raise RuntimeError("Model sandbox execution is spot/CASH USDT only")
                probe = self.order_factory.market(instrument_id=identifier, order_side=OrderSide.BUY,
                    quantity=self.instrument.make_qty(1))
                self.assert_sandbox_route(probe)
            self.subscribe_quote_ticks(identifier)
            self.msgbus.subscribe(topic=topic, handler=self.on_feature_window)
            self.accepting = True

        def on_feature_window(self, window):
            if not self.accepting:
                return
            if window.get("instrument") != model_instrument or (model_domain and window.get("domain") != model_domain):
                self.inference_errors += 1
                self.last_inference_error = "Feature input does not match this strategy's instrument/domain mapping"
                return
            if len(self.pending_predictions) >= client.manifest["runtime"]["maxQueueSize"]:
                self.queue_rejected += 1
                return
            # No model load, feature construction, GPU call, or blocking IPC in
            # this event callback. Task completion re-enters the same event loop.
            task = loop.create_task(self.infer(window))
            self.pending_predictions.add(task)
            task.add_done_callback(self.pending_predictions.discard)

        async def infer(self, window):
            try:
                now = time.time_ns()
                request = {**window, "requestId": uuid.uuid4().hex,
                    "releaseId": client.manifest["releaseId"], "modelHash": client.manifest["model"]["sha256"],
                    "mode": mode, "normalized": False,
                    "deadlineNs": now + client.manifest["runtime"]["timeoutMs"] * 1_000_000}
                result = await client.predict(request)
                prediction = result["prediction"]
                fraction = checked_fraction(result["checked"])
                if not self.accepting or prediction["asOfNs"] <= self.last_asof_ns:
                    return
                self.last_asof_ns = prediction["asOfNs"]
                self.last_inference_error = None
                target = {**prediction, "targetFraction": fraction, "mode": mode, "ordersSent": False}
                self.model_targets.append(target)
                self.msgbus.publish(topic=f"model.targets.{client.manifest['releaseId']}", msg=target)
                if mode == "sandbox":
                    self.apply_sandbox_target(fraction, target)
            except asyncio.CancelledError:
                raise
            except Exception as failure:
                self.inference_errors += 1
                self.last_inference_error = str(failure)

        def apply_sandbox_target(self, fraction, target):
            # Every execution is routed and checked again at submission time.
            if self.cache.orders_open(strategy_id=self.id, instrument_id=identifier):
                target["executionSkipped"] = "Outstanding strategy order"
                return
            quote = self.cache.quote_tick(identifier)
            if quote is None or time.time_ns() - quote.ts_event > 5_000_000_000:
                target["executionSkipped"] = "Missing or stale executable quote"
                return
            ask = float(quote.ask_price)
            if ask <= 0 or not math.isfinite(ask):
                return
            current = sum(float(p.signed_qty) for p in self.cache.positions_open(strategy_id=self.id, instrument_id=identifier))
            desired = float(budget_usdt) * fraction / ask
            change = desired - current
            side = OrderSide.BUY if change > 0 else OrderSide.SELL
            amount = abs(change)
            if side == OrderSide.BUY:
                account = self.portfolio.account(identifier.venue)
                if account is None:
                    target["executionSkipped"] = "Sandbox account unavailable"
                    return
                free = account.balance_free(self.instrument.quote_currency)
                if free is None:
                    return
                # Use released instrument taker fee, never spend nonexistent
                # cash to approximate a desired weight. No leverage or shorts.
                amount = min(amount, float(free) / (ask * (1 + float(self.instrument.taker_fee))))
            else:
                amount = min(amount, max(0, current))
            qty = self.instrument.make_qty(amount, round_down=True)
            if float(qty) <= 0 or (self.instrument.min_quantity is not None and qty < self.instrument.min_quantity):
                return
            order = self.order_factory.market(instrument_id=identifier, order_side=side, quantity=qty)
            self.assert_sandbox_route(order)
            self.submit_order(order)
            target.update(ordersSent=True, clientOrderId=str(order.client_order_id))

        def on_stop(self):
            self.accepting = False
            self.msgbus.unsubscribe(topic=topic, handler=self.on_feature_window)
            self.unsubscribe_quote_ticks(identifier)
            for task in self.pending_predictions:
                task.cancel()

        def model_snapshot(self):
            target = self.model_targets[-1] if self.model_targets else None
            return {"releaseId": client.manifest["releaseId"], "modelHash": client.manifest["model"]["sha256"],
                "modelVersion": client.manifest["modelVersion"], "device": client.manifest["runtime"]["device"],
                "mode": mode, "pending": len(self.pending_predictions), "inferenceErrors": self.inference_errors,
                "queueRejected": self.queue_rejected, "lastInferenceError": self.last_inference_error,
                "latestTarget": target, "windowReady": target is not None,
                "warmupComplete": client.is_ready,
                "status": "stopped" if not self.accepting else "error" if self.last_inference_error or not client.is_ready else "active" if target else "waiting_for_features",
                "lastInferenceAt": datetime.fromtimestamp(target["completedAtNs"] / 1e9, timezone.utc).isoformat() if target else None,
                "latencyMs": target["latencyMs"] if target else None,
                "queueMs": target["queueMs"] if target else None,
                "budgetWindow": {"usedMs": target["latencyMs"] if target else None,
                    "limitMs": client.manifest["runtime"]["timeoutMs"]},
                "recentInferences": [{k: row[k] for k in ("completedAtNs", "asOfNs", "prediction", "targetFraction", "latencyMs")}
                    for row in list(self.model_targets)[-64:]]}

    return ReleasedModelStrategy()
