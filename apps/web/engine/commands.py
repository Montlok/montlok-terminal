"""Operator commands executed inside the engine process. `halt` is the kill switch; `flatten` is the second step."""
from __future__ import annotations

import os
import time
from decimal import Decimal

import nautilus_trader
from nautilus_trader.model.enums import TradingState
from nautilus_trader.model.enums import order_side_to_str
from nautilus_trader.model.enums import order_status_to_str
from nautilus_trader.model.enums import order_type_to_str
from nautilus_trader.model.enums import position_side_to_str
from nautilus_trader.model.enums import trading_state_to_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair

from alerts import Notifier
from health import HealthMonitor
from settings import Settings


class EngineCommands:
    def __init__(self, node, monitor: HealthMonitor, notifier: Notifier, settings: Settings):
        self.node = node
        self.monitor = monitor
        self.notifier = notifier
        self.settings = settings
        self.started_at = time.time()

    def handlers(self) -> dict:
        return {"status": self.status, "halt": self.halt, "flatten": self.flatten, "reduce": self.reduce,
                "resume": self.resume, "limit": self.limit}

    @property
    def risk(self):
        return self.node.kernel.risk_engine

    @property
    def state(self) -> str:
        return trading_state_to_str(self.risk.trading_state)

    def status(self, _request: dict | None = None) -> dict:
        cache = self.node.cache
        return {
            "traderId": self.settings.trader_id, "environment": self.settings.environment,
            "pid": os.getpid(), "startedAt": self.started_at, "version": nautilus_trader.__version__,
            "tradingState": self.state,
            "connected": {"data": self.node.kernel.data_engine.check_connected(),
                          "exec": self.node.kernel.exec_engine.check_connected()},
            "instruments": self.settings.instruments,
            "health": self.monitor.status(),
            "positions": [self.position_row(position) for position in cache.positions_open()],
            "openOrders": [self.order_row(order) for order in cache.orders_open()],
        }

    def position_row(self, position) -> dict:
        pnl = self.node.portfolio.unrealized_pnl(position.instrument_id)
        return {"instrument": str(position.instrument_id), "side": position_side_to_str(position.side),
                "quantity": str(position.quantity), "avgPx": str(position.avg_px_open),
                "unrealizedPnl": str(pnl) if pnl is not None else None, "strategy": str(position.strategy_id)}

    @staticmethod
    def order_row(order) -> dict:
        price = getattr(order, "price", None)
        return {"clientOrderId": str(order.client_order_id), "instrument": str(order.instrument_id),
                "side": order_side_to_str(order.side), "type": order_type_to_str(order.order_type),
                "quantity": str(order.quantity), "price": str(price) if price is not None else None,
                "status": order_status_to_str(order.status), "strategy": str(order.strategy_id)}

    def who(self, request: dict) -> str:
        return f"{request.get('operator', '?')}: {request.get('reason', '')}".strip(": ")

    def strategy_for(self, strategy_id):
        strategies = self.node.trader.strategies()
        if not strategies:
            raise ValueError("没有运行中的策略可用于发送订单")
        return next((s for s in strategies if s.id == strategy_id), strategies[0])

    def halt(self, request: dict) -> dict:
        self.risk.set_trading_state(TradingState.HALTED)
        canceled = 0
        for order in self.node.cache.orders_open():
            self.strategy_for(order.strategy_id).cancel_order(order)
            canceled += 1
        open_positions = len(self.node.cache.positions_open())
        self.notifier.send("CRITICAL", "急停已触发", f"{self.who(request)} · 撤单 {canceled} · 仍有持仓 {open_positions}")
        return {"tradingState": self.state, "canceled": canceled, "openPositions": open_positions}

    def flatten(self, request: dict) -> dict:
        if self.risk.trading_state == TradingState.ACTIVE:
            raise ValueError("请先急停（halt），确认无挂单后再平仓")
        self.risk.set_trading_state(TradingState.REDUCING)
        closing = 0
        for position in self.node.cache.positions_open():
            instrument = self.node.cache.instrument(position.instrument_id)
            self.strategy_for(position.strategy_id).close_position(position, reduce_only=not isinstance(instrument, CurrencyPair))
            closing += 1
        self.notifier.send("CRITICAL", "市价平仓已发出", f"{self.who(request)} · {closing} 个持仓 · 状态 REDUCING")
        return {"tradingState": self.state, "closing": closing}

    def reduce(self, request: dict) -> dict:
        self.risk.set_trading_state(TradingState.REDUCING)
        self.notifier.send("WARNING", "仅允许减仓", self.who(request))
        return {"tradingState": self.state}

    def resume(self, request: dict) -> dict:
        self.risk.set_trading_state(TradingState.ACTIVE)
        self.notifier.send("WARNING", "交易已恢复", self.who(request))
        return {"tradingState": self.state}

    def limit(self, request: dict) -> dict:
        """Tighten or relax one per-order notional cap without a restart; the manifest stays the source of truth."""
        instrument = str(request.get("instrument", ""))
        notional = Decimal(str(request.get("notional", 0)))
        if instrument not in self.settings.instruments or notional <= 0:
            raise ValueError("instrument 必须是已部署品种，notional 必须大于 0")
        instrument_id = InstrumentId.from_str(instrument)
        previous = self.risk.max_notional_per_order(instrument_id)
        self.risk.set_max_notional_per_order(instrument_id, notional)
        self.notifier.send("WARNING", f"单笔名义上限 {instrument}: {previous} → {notional}", self.who(request))
        return {"instrument": instrument, "previous": str(previous), "notional": str(notional)}
