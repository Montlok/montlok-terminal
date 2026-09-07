"""Operator commands for a native node, applied only by its polling thread.

Control requests carry run identity and durable operation IDs. Selection/GETs
never reach this queue. Cash reductions use this run's allocation ledger, not
the complete exchange wallet or reconciled EXTERNAL orders.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future
from decimal import Decimal, ROUND_DOWN
from queue import Empty, Queue
import threading
import time

OPEN = {'INITIALIZED', 'SUBMITTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'PENDING_CANCEL', 'PENDING_UPDATE'}
ACTIONS = ('halt', 'reduce', 'resume', 'cancel', 'flatten')


def reduction_size(allocated, available, lot, minimum, fee):
    quantity = (max(Decimal(0), min(Decimal(str(allocated)), Decimal(str(available)))) /
                (1 + Decimal(str(fee))) / Decimal(str(lot))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(lot))
    return quantity if quantity >= Decimal(str(minimum)) else Decimal(0)


class NativeControls:
    def __init__(self, node, runtime, operator, native):
        self.node, self.runtime, self.operator, self.n = node, runtime, operator, native
        self.owner_thread = threading.get_ident()
        self.queue = Queue(maxsize=32)
        self.paused = set()
        self.strategy_ids = [key for key in node.strategy_ids if str(key) not in {'MONTLOK-OPS','MONTLOK-OPS-OPS'}]
        self.closed = False
        self.flatten = None
        self.last_poll = 0.0

    async def request(self, request):
        if request.get('command') not in ACTIONS:
            raise ValueError('控制操作不支持')
        if request.get('runId') != self.runtime.request['runId']:
            raise ValueError('运行编号不一致')
        if self.closed:
            raise ValueError('运行已结束')
        future = Future()
        self.queue.put_nowait((dict(request), future))
        # A transport timeout must not cancel a command already accepted.
        return await asyncio.shield(asyncio.wrap_future(future))

    def process(self):
        if threading.get_ident() != self.owner_thread:
            raise RuntimeError('原生控制必须在引擎线程执行')
        for _ in range(8):
            try:
                request, future = self.queue.get_nowait()
            except Empty:
                break
            try:
                future.set_result(self.apply(request))
            except Exception as error:
                future.set_exception(error)

    def stop_strategies(self):
        for identifier in self.strategy_ids:
            if identifier not in self.paused:
                self.node.stop_strategy(identifier)
                self.paused.add(identifier)

    def start_strategies(self):
        feed = self.runtime.model_feed
        if feed and hasattr(feed, 'reconcile_allocation'):
            feed.reconcile_allocation(self.runtime.allocated_positions, self.runtime.allocated_cash)
        for identifier in list(self.paused):
            self.node.resume_strategy(identifier)
            self.paused.remove(identifier)

    def cancel_owned(self):
        cancelled = []
        for order in self.node.cache.orders():
            if str(order.strategy_id).startswith('EXTERNAL') or str(order.status) not in OPEN or str(order.status) == 'PENDING_CANCEL':
                continue
            # Initialized/submitted orders may not yet have an account/venue ID.
            # Retain them until acknowledgement, then cancel on the next poll.
            if not order.account_id:
                continue
            self.operator.cancel_order(order.client_order_id)
            cancelled.append(str(order.client_order_id))
        return cancelled

    def apply(self, request):
        action = request['command']
        if not self.node.is_running or self.runtime.stop.is_set():
            raise ValueError('运行尚未就绪或正在停止')
        if action == 'resume':
            if self.flatten:
                raise ValueError('平仓后的库存已变化，请停止后重新启动策略')
            if self.runtime.state not in {'HALTED', 'REDUCING'}:
                raise ValueError('当前运行无需恢复')
            # Native risk engine retains reconciliation/error latches.
            self.node.set_trading_state(self.n.TradingState.ACTIVE)
            self.runtime.state = 'ACTIVE'
            try:
                self.start_strategies()
            except Exception:
                self.runtime.state = 'HALTED'
                self.node.set_trading_state(self.n.TradingState.HALTED)
                raise
            return self.result()
        if action == 'flatten':
            symbols = request.get('instruments')
            known = {row['instrument'] for row in self.runtime.inventory['pairs']}
            if not isinstance(symbols, list) or not symbols or any(not isinstance(s, str) or s not in known for s in symbols):
                raise ValueError('请选择本次运行中的交易品种')
            if self.flatten and self.flatten['phase'] in {'cancelling', 'reducing'}:
                raise ValueError('当前平仓请求尚未结束')
        policy = 'REDUCING' if action in {'reduce', 'flatten'} else 'HALTED'
        self.node.set_trading_state(getattr(self.n.TradingState, policy))
        self.runtime.state = policy
        self.stop_strategies()
        cancelled = self.cancel_owned()
        if action == 'flatten':
            self.flatten = dict(operationId=request['operationId'], phase='cancelling',
                instruments=sorted(set(request['instruments'])), startedAt=time.time(),
                deadline=time.monotonic()+60, attempts={}, results={})
        elif action == 'reduce' and self.runtime.model_feed:
            # Python model/Alpha signals explicitly suppress buys in REDUCING.
            # Native grids remain paused; their quote replacement is two-sided.
            self.start_strategies()
        return {**self.result(), 'cancelRequested': cancelled}

    def result(self):
        return {'tradingState': self.runtime.state, 'controlVersion': 1,
                'resumeAllowed': self.flatten is None, 'flatten': self.flatten_status()}

    def flatten_status(self):
        return {k:v for k,v in self.flatten.items() if k != 'deadline'} if self.flatten else None

    def poll(self):
        now = time.monotonic()
        if now-self.last_poll < .125:
            return
        self.last_poll = now
        if self.runtime.state == 'HALTED' or self.flatten and self.flatten['phase'] == 'cancelling':
            self.cancel_owned()
        task = self.flatten
        if not task or task['phase'] not in {'cancelling', 'reducing'}:
            return
        opens = [o for o in self.node.cache.orders() if not str(o.strategy_id).startswith('EXTERNAL') and str(o.status) in OPEN]
        if now > task['deadline']:
            task['phase'] = 'incomplete'
            self.runtime.state = 'HALTED'
            self.node.set_trading_state(self.n.TradingState.HALTED)
            self.cancel_owned()
            return
        if opens:
            return
        task['phase'] = 'reducing'
        rows = {row['instrument']:row for row in self.runtime.inventory['pairs']}
        pending = False
        for symbol in task['instruments']:
            row = rows[symbol]
            allocated = Decimal(str(self.runtime.allocated_positions.get(symbol, 0)))
            if allocated < Decimal(row['minSize']):
                task['results'][symbol] = {'state': 'flat' if allocated <= 0 else 'dust', 'remaining': str(allocated)}
                continue
            pending = True
            if task['attempts'].get(symbol, 0) >= 16:
                task['results'][symbol] = {'state':'attempt_limit','remaining':str(allocated)}
                continue
            identifier = self.n.InstrumentId.from_str(symbol+'.OKX')
            quote = self.node.cache.quote(identifier)
            if not quote or time.time_ns()-quote.ts_init > 5_000_000_000:
                task['results'][symbol] = {'state':'waiting_quote','remaining':str(allocated)}
                continue
            account = self.node.cache.account_for_venue(self.n.Venue('OKX'))
            available = account.balance_free(self.n.Currency.from_str(row['base'])) if account else None
            if available is None:
                task['results'][symbol] = {'state':'waiting_balance','remaining':str(allocated)}
                continue
            quantity = reduction_size(allocated, available.as_decimal(), row['lotSize'], row['minSize'], Decimal(row['takerFeeBps'])/10000)
            # Respect the native per-order notional used by this release;
            # reduction of a large allocation is split over venue acknowledgements.
            limit = Decimal(row.get('tradeSize',str(quantity)))
            if row.get('ask'):
                limit *= min(Decimal(1), Decimal(row['ask'])/Decimal(str(quote.bid_price)))
            quantity = min(quantity, (limit/Decimal(row['lotSize'])).to_integral_value(rounding=ROUND_DOWN)*Decimal(row['lotSize']))
            if quantity < Decimal(row['minSize']):
                # Locked balance is not a completed liquidation.
                task['results'][symbol] = {'state':'balance_or_minimum','remaining':str(allocated)}
                continue
            price = (Decimal(str(quote.bid_price))*Decimal('.999')/Decimal(row['tickSize'])).to_integral_value(rounding=ROUND_DOWN)*Decimal(row['tickSize'])
            order = self.operator.order_factory.limit(identifier, self.n.OrderSide.SELL,
                self.n.Quantity.from_str(format(quantity,'f')), self.n.Price.from_str(format(price,'f')),
                time_in_force=self.n.TimeInForce.IOC)
            task['attempts'][symbol] = task['attempts'].get(symbol,0)+1
            task['results'][symbol] = {'state':'submitted','orderId':str(order.client_order_id),'quantity':str(quantity),'remaining':str(allocated)}
            self.operator.submit_order(order)
            return
        if not pending:
            task['phase'] = 'completed'
            self.runtime.state = 'HALTED'
            self.node.set_trading_state(self.n.TradingState.HALTED)

    def close(self):
        self.closed = True
        while True:
            try:
                _, future = self.queue.get_nowait()
            except Empty:
                break
            future.set_exception(RuntimeError('运行已结束，控制操作未执行'))


def attach_controls(node, runtime):
    from nautilus_trader.core import nautilus_pyo3 as n
    class OperatorStrategy(n.Strategy):
        def on_start(self):
            for row in runtime.inventory['pairs']:
                self.subscribe_quotes(n.InstrumentId.from_str(row['instrument']+'.OKX'))
        def on_stop(self):
            for row in runtime.inventory['pairs']:
                self.cancel_all_orders(n.InstrumentId.from_str(row['instrument']+'.OKX'))
    operator = OperatorStrategy(n.StrategyConfig(strategy_id=n.StrategyId('MONTLOK-OPS'), order_id_tag='OPS',
        use_uuid_client_order_ids=True, use_hyphens_in_client_order_ids=False))
    node.add_strategy(operator)
    return NativeControls(node, runtime, operator, n)
