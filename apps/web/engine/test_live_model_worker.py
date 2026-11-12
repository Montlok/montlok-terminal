from decimal import Decimal
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from live_model_worker import PendingOrders, make_strategy
from model_market_actor import MARKETS, rdt_window_from_markets


class ModelBudgetTests(unittest.TestCase):
    def test_independent_candle_delivery_uses_common_completed_window(self):
        import pandas as pd
        index = pd.date_range('2026-01-01', periods=968, freq='min', tz='UTC')
        markets = {name: pd.DataFrame({'close': range(968)}, index=index) for name in MARKETS}
        markets['ETH-USDT'] = markets['ETH-USDT'].iloc[:-1]
        manifest = {'domainContracts': {'crypto:BTC-USDT': {'names': ['close'], 'sequenceBars': 480,
            'barSeconds': 60, 'domain': 'crypto', 'instrument': 'BTC-USDT'}}}
        module = SimpleNamespace(features=lambda aligned: aligned['BTC-USDT'][['close']])
        result = rdt_window_from_markets(manifest, markets, module, 'crypto:BTC-USDT')
        self.assertEqual(len(result['inputs']), 480)
        self.assertEqual(result['asOfNs'], index[-2].value + 60_000_000_000)
        markets['ETH-USDT'] = markets['ETH-USDT'].drop(index[500])
        with self.assertRaises(ValueError):
            rdt_window_from_markets(manifest, markets, module, 'crypto:BTC-USDT')

    def test_live_targets_reuse_only_the_assigned_cash(self):
        submitted = []

        class Strategy:
            def __init__(self, config):
                self.cache = SimpleNamespace(orders=lambda: [])
                self.order_factory = SimpleNamespace(limit=lambda instrument, side, qty, px, **kw:
                    dict(side=side, quantity=qty, price=px, **kw))

            def submit_order(self, order):
                submitted.append(order)

        class Currency:
            precision = 8
            def __init__(self, value): self.value = value
            def __str__(self): return self.value

        class Feed:
            failure = None
            fraction = 1
            def __init__(self, spec): pass
            def target(self): return dict(asOfNs=time.time_ns(), targetFraction=self.fraction)

        native = SimpleNamespace(Strategy=Strategy, StrategyConfig=lambda **kw: kw, StrategyId=lambda s: s,
            InstrumentId=SimpleNamespace(from_str=lambda s: s),
            Quantity=SimpleNamespace(from_str=Decimal), Price=SimpleNamespace(from_str=Decimal),
            OrderSide=SimpleNamespace(BUY='BUY', SELL='SELL'))
        captured = []
        row = dict(instrument='BTC-USDT', base='BTC', tradeSize='0.00001193', lotSize='0.00000001',
                   tickSize='0.1', minSize='0.00001', makerFeeBps='8', plannedQuoteExposureUsdt='0.95')
        quote = SimpleNamespace(ts_event=time.time_ns(), bid_price='79647.6', ask_price='79647.7')
        with patch('live_model_worker.ModelFeed', Feed), patch.dict('sys.modules', {
            'nautilus_trader': SimpleNamespace(), 'nautilus_trader.core': SimpleNamespace(nautilus_pyo3=native)}):
            feed = make_strategy(SimpleNamespace(add_strategy=captured.append), SimpleNamespace(state='ACTIVE'), {}, {'pairs': [row]},
                                 dict(maxQuoteExposureUsdt='1', requoteThresholdBps=2))
            strategy = captured[0]
            strategy.on_quote(quote)
            self.assertEqual(submitted[0]['side'], 'BUY')
            strategy.on_order_filled(SimpleNamespace(last_qty='0.00001193', last_px='79647.6', order_side='BUY',
                commission=SimpleNamespace(currency=Currency('BTC'), as_decimal=lambda: Decimal('0.00000001'))))
            self.assertLess(strategy.cash, Decimal('.05'))
            feed.fraction = 0
            strategy.last_submit = 0
            strategy.on_quote(quote)
            sale = submitted[-1]
            self.assertEqual(sale['side'], 'SELL')
            self.assertLessEqual(sale['quantity'], Decimal('0.00001191807'))
            strategy.on_order_filled(SimpleNamespace(last_qty=str(sale['quantity']), last_px='79000', order_side='SELL',
                commission=SimpleNamespace(currency=Currency('USDT'), as_decimal=lambda: Decimal('.00094089'))))
            remaining = strategy.cash
            self.assertLess(remaining, Decimal('1'))
            feed.fraction = 1
            strategy.last_submit = 0
            strategy.on_quote(SimpleNamespace(ts_event=time.time_ns(), bid_price='90000', ask_price='90000.1'))
            self.assertLessEqual(submitted[-1]['quantity'] * submitted[-1]['price'], remaining)
            fullrow={**row,'baseAvailable':'0.0001','quoteAvailable':'100','maxPosition':'0.00135','tradeSize':'0.0006','plannedQuoteExposureUsdt':'100'}
            make_strategy(SimpleNamespace(add_strategy=captured.append),SimpleNamespace(state='ACTIVE'),{},
                {'pairs':[fullrow]},dict(capitalUtilization=1,requoteThresholdBps=2))
            full=captured[-1]
            self.assertEqual(full.position,Decimal('.0001'))
            self.assertEqual(full.cash,Decimal(100))
            full.on_quote(quote)
            self.assertGreater(submitted[-1]['quantity']*submitted[-1]['price'],Decimal(1))
            self.assertLessEqual(submitted[-1]['quantity']*submitted[-1]['price'],Decimal(100))

    def test_pending_orders_rescan_the_cache_only_when_it_grows(self):
        scans = []
        orders = {}

        def order(key, instrument, status):
            orders[key] = SimpleNamespace(client_order_id=key, instrument_id=instrument, status=status)

        def all_orders():
            scans.append(len(orders))
            return list(orders.values())

        cache = SimpleNamespace(orders=all_orders, orders_total_count=lambda: len(orders), order=orders.get)
        for i in range(500):
            order(f'old-{i}', 'BTC-USDT.OKX', 'CANCELED')
        order('eth', 'ETH-USDT.OKX', 'ACCEPTED')
        order('a', 'BTC-USDT.OKX', 'ACCEPTED')
        pending = PendingOrders(cache, 'BTC-USDT.OKX', rescan_secs=3600)
        self.assertEqual([o.client_order_id for o in pending()], ['a'])
        for _ in range(100):
            self.assertEqual([o.client_order_id for o in pending()], ['a'])
        self.assertEqual(len(scans), 1)
        orders['a'] = SimpleNamespace(client_order_id='a', instrument_id='BTC-USDT.OKX', status='FILLED')
        self.assertEqual(pending(), [])
        order('b', 'BTC-USDT.OKX', 'SUBMITTED')
        self.assertEqual([o.client_order_id for o in pending()], ['b'])
        self.assertEqual(len(scans), 2)
        fallback = PendingOrders(SimpleNamespace(orders=all_orders), 'BTC-USDT.OKX')
        self.assertEqual([o.client_order_id for o in fallback()], ['b'])

    def test_closed_bar_actor_publishes_each_newer_window_once(self):
        import asyncio
        import model_market_actor

        published = []

        class Actor:
            def __init__(self, config):
                self.msgbus = SimpleNamespace(publish=lambda topic, msg: published.append((topic, msg['asOfNs'])))

        modules = {'nautilus_trader': SimpleNamespace(), 'nautilus_trader.common': SimpleNamespace(),
                   'nautilus_trader.common.actor': SimpleNamespace(Actor=Actor),
                   'nautilus_trader.config': SimpleNamespace(ActorConfig=lambda: None)}
        manifest = {'runnerId': 'gru_v1', 'releaseId': 'r1', 'runtime': {'maxInputAgeMs': 10 ** 15}}
        stamps = iter([5, 5, 7])
        real_sleep = asyncio.sleep

        async def scenario():
            actor = model_market_actor.make_gru_market_actor(manifest, '/reviewed', loop=asyncio.get_running_loop(),
                                                             fetcher=lambda name: name)

            def window(release, frames, root):
                self.assertEqual(list(frames), list(MARKETS))
                stamp = next(stamps, None)
                if stamp is None:
                    actor.accepting = False
                    raise ValueError('market closed')
                return {'asOfNs': stamp}

            with patch.object(model_market_actor, 'gru_window_from_markets', window), \
                    patch.object(model_market_actor.asyncio, 'sleep', lambda _: real_sleep(0)):
                actor.on_start()
                await actor.task
            return actor

        with patch.dict('sys.modules', modules):
            actor = asyncio.run(scenario())
        self.assertEqual(published, [('model.features.r1', 5), ('model.features.r1', 7)])
        snapshot = actor.model_market_snapshot()
        self.assertEqual((snapshot['asOfNs'], snapshot['fetchErrors'], snapshot['lastError']), (7, 1, 'market closed'))
        self.assertFalse(snapshot['windowReady'])


if __name__ == '__main__': unittest.main()
