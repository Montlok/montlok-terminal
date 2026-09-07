from decimal import Decimal
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from live_alpha_worker import attach, target_quantity


class AlphaExecutionTests(unittest.TestCase):
    def fixture(self, cash='200', spread='0.01'):
        submitted, captured, orders = [], [], {}
        quote = SimpleNamespace(ts_init=time.time_ns(), bid_price='100', ask_price=str(Decimal(100)+Decimal(spread)))

        class Strategy:
            def __init__(self, config):
                self.cache = SimpleNamespace(quote=lambda _: quote, order=orders.get)
                self.order_factory = SimpleNamespace(limit=self.limit)
            def limit(self, instrument, side, qty, px, **kw):
                return SimpleNamespace(client_order_id=str(len(submitted)), instrument=instrument, side=side, quantity=qty, price=px, **kw)
            def submit_order(self, order):
                submitted.append(order)
                orders[order.client_order_id]=SimpleNamespace(status='ACCEPTED')
        native = SimpleNamespace(Strategy=Strategy, StrategyConfig=lambda **kw: kw, StrategyId=lambda s:s,
            InstrumentId=SimpleNamespace(from_str=lambda s:s), Quantity=SimpleNamespace(from_str=Decimal),
            Price=SimpleNamespace(from_str=Decimal), OrderSide=SimpleNamespace(BUY='BUY', SELL='SELL'),
            TimeInForce=SimpleNamespace(IOC='IOC'))
        common=dict(takerFeeBps='10', tickSize='.01', lotSize='.001', minSize='.001')
        pairs=[dict(common,instrument='XNVDA-USDT',base='XNVDA',baseAvailable='1',targetQuantity='.5'),
               dict(common,instrument='XAAPL-USDT',base='XAAPL',baseAvailable='0',targetQuantity='.5')]
        with patch.dict('sys.modules', {'nautilus_trader':SimpleNamespace(),
                'nautilus_trader.core':SimpleNamespace(nautilus_pyo3=native)}):
            controller=attach(SimpleNamespace(add_strategy=captured.append),SimpleNamespace(state='ACTIVE',controls=None),{},
                              {'pairs':pairs,'allocatedCashUsdt':cash},{})
        return controller.strategy, submitted, orders, quote

    def test_tested_target_is_fee_inclusive_and_lot_rounded(self):
        qty=target_quantity(Decimal(200),Decimal('.1'),Decimal(100),Decimal('.001'),Decimal('.001'))
        self.assertEqual(qty, Decimal('.199'))
        self.assertLessEqual(qty*Decimal('100.1')*Decimal('1.001'),Decimal(20))

    def test_sells_excess_inventory_before_buys_and_waits_for_ack(self):
        strategy, submitted, orders, _=self.fixture()
        strategy.drive()
        self.assertEqual(submitted[0].side, 'SELL')
        self.assertEqual(submitted[0].quantity, Decimal('.5'))
        strategy.last_send=0
        strategy.drive()
        self.assertEqual(len(submitted),1)
        strategy.positions['XNVDA-USDT.OKX']=Decimal('.5')
        orders['0'].status='FILLED'
        strategy.drive(); strategy.drive()
        self.assertEqual(submitted[-1].side,'BUY')
        self.assertEqual(submitted[-1].instrument,'XAAPL-USDT.OKX')

    def test_stale_or_costly_quotes_do_not_create_orders(self):
        for spread, stale in (('2',False),('.01',True)):
            strategy,submitted,_,quote=self.fixture(spread=spread)
            if stale:quote.ts_init-=10_000_000_000
            strategy.drive()
            self.assertEqual(submitted,[])

    def test_buys_use_only_the_allocated_cash(self):
        strategy,submitted,_,_=self.fixture(cash='1')
        strategy.phase='BUY'
        strategy.positions['XNVDA-USDT.OKX']=Decimal('.5')
        strategy.drive()
        self.assertEqual(submitted[0].side,'BUY')
        self.assertLessEqual(submitted[0].price*submitted[0].quantity*Decimal('1.001'),Decimal(1))

    def test_one_costly_sell_does_not_block_other_symbols_cash_budget(self):
        strategy,submitted,_,quote=self.fixture(cash='1')
        costly=SimpleNamespace(ts_init=time.time_ns(),bid_price='100',ask_price='110')
        strategy.cache.quote=lambda key:costly if key.startswith('XNVDA') else quote
        strategy.drive()
        self.assertEqual(submitted[0].instrument,'XAAPL-USDT.OKX')
        self.assertEqual(submitted[0].side,'BUY')
        self.assertLessEqual(submitted[0].price*submitted[0].quantity*Decimal('1.001'),Decimal(1))

    def test_filled_target_moves_to_monitoring(self):
        strategy,submitted,_,_=self.fixture()
        strategy.positions={'XNVDA-USDT.OKX':Decimal('.5'),'XAAPL-USDT.OKX':Decimal('.5')}
        strategy.drive();strategy.drive()
        self.assertEqual(strategy.phase,'ALLOCATED')
        self.assertEqual(submitted,[])
