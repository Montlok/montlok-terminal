from decimal import Decimal
from types import SimpleNamespace as NS
import time
import unittest
from unittest.mock import patch

from alpha_execution import ContinuousExecutor
from execution_options import DEFAULTS, settings


def fixture(cash='100', position='.5', count=2, **options):
    submitted=[];cancelled=[];cache={}
    quotes={}
    rows=[]
    for i in range(count):
        symbol=f'X{i}-USDT'
        quotes[symbol+'.OKX']=NS(instrument_id=symbol+'.OKX',ts_init=time.time_ns(),bid_price='100',ask_price='100')
        rows.append(dict(instrument=symbol,base=f'X{i}',baseAvailable=position,weight=.25,
                         bid='100',ask='100',tickSize='.01',lotSize='.001',minSize='.01',makerFeeBps='8'))
    def limit(instrument,side,quantity,price,**kwargs):
        return NS(client_order_id=str(len(submitted)),instrument=instrument,side=side,
                  quantity=quantity,price=price,**kwargs)
    def submit(order):
        submitted.append(order);cache[order.client_order_id]=NS(status='SUBMITTED')
    def cancel(identifier):
        cancelled.append(identifier);cache[identifier].status='PENDING_CANCEL'
    strategy=NS(cache=NS(order=cache.get,quote=quotes.get),order_factory=NS(limit=limit),submit_order=submit,cancel_order=cancel)
    native=NS(InstrumentId=NS(from_str=str),Quantity=NS(from_str=Decimal),Price=NS(from_str=Decimal),OrderSide=NS(BUY='BUY',SELL='SELL'))
    runtime=NS(state='ACTIVE')
    executor=ContinuousExecutor(strategy,native,runtime,dict(pairs=rows,allocatedCashUsdt=cash),{**DEFAULTS,**options})
    return executor,submitted,cancelled,cache,quotes,runtime


def drive(executor,now=100):
    with patch('alpha_execution.time.monotonic',return_value=now):executor.drive()


class ContinuousAlphaTests(unittest.TestCase):
    def test_unchanged_targets_have_live_quotes_not_allocated_dead_end(self):
        e,orders,_,cache,_,_=fixture()
        drive(e)
        self.assertEqual(len(orders),4)
        self.assertEqual({o.instrument for o in orders},{'X0-USDT.OKX','X1-USDT.OKX'})
        self.assertTrue(all(o.post_only for o in orders))
        self.assertTrue(all(o.price<100 if o.side=='BUY' else o.price>100 for o in orders))
        drive(e,101)
        self.assertEqual(len(orders),4)  # Outstanding acknowledgements retain each slot.
        self.assertEqual(e.metrics()['phase'],'QUOTING')

    def test_pending_ack_for_one_instrument_does_not_block_others(self):
        e,orders,*_=fixture(count=26)
        drive(e)
        self.assertEqual(len({o.instrument for o in orders}),26)
        self.assertGreater(len(orders),1)

    def test_cash_is_reserved_across_instruments_before_ack(self):
        e,orders,*_=fixture(cash='1',position='0')
        drive(e)
        self.assertEqual(len(orders),1)
        self.assertLessEqual(e.reserved_cash(),Decimal(1))
        drive(e,101)
        self.assertEqual(len(orders),1)

    def test_cancellation_does_not_release_cash_until_terminal_ack(self):
        e,orders,cancelled,cache,quotes,_=fixture(cash='1',position='0')
        drive(e);oid=orders[0].client_order_id;cache[oid].status='ACCEPTED'
        reserved=e.reserved_cash()
        for quote in quotes.values():quote.bid_price=quote.ask_price='101'
        drive(e,101)
        self.assertEqual(cancelled,[oid]);self.assertEqual(e.reserved_cash(),reserved)
        self.assertEqual(len(orders),1)
        cache[oid].status='CANCELED'
        drive(e,102)
        self.assertEqual(e.reserved_cash(),Decimal(0))  # New minimum order exceeds the available cash.

    def test_partial_fill_updates_cash_inventory_and_remaining_reservation(self):
        e,orders,*_=fixture();drive(e)
        buy=next(o for o in orders if o.side=='BUY')
        before=e.positions[buy.instrument]
        event=NS(instrument_id=buy.instrument,client_order_id=buy.client_order_id,order_side='BUY',
                 last_qty='.004',last_px=str(buy.price),commission=NS(currency=e.rows[buy.instrument]['base'],as_decimal=lambda:Decimal('.0000032')))
        e.on_filled(event)
        self.assertEqual(e.positions[buy.instrument],before+Decimal('.0039968'))
        self.assertEqual(e.reservations[buy.client_order_id]['remaining'],Decimal('.006'))
        self.assertEqual(e.cash,Decimal(100)-Decimal('.004')*buy.price)

    def test_local_denial_releases_the_executor_slot_before_halt(self):
        e,orders,*_=fixture();drive(e)
        order=orders[0]
        self.assertIn(order.client_order_id,e.reservations)
        e.on_rejected(NS(client_order_id=order.client_order_id,reason='risk cap'))
        self.assertNotIn(order.client_order_id,e.reservations)
        self.assertNotIn((order.instrument,order.side),e.pending)
        self.assertEqual(e.counts['rejected'],1)

    def test_filled_or_cancelled_quotes_are_replenished(self):
        e,orders,_,cache,_,_=fixture();drive(e)
        for row in cache.values():row.status='CANCELED'
        drive(e,101)
        self.assertEqual(len(orders),8)

    def test_targets_recalculate_when_market_moves(self):
        e,orders,_,cache,quotes,_=fixture(mode='continuous_rebalance')
        drive(e);self.assertEqual(orders,[])
        old=e.targets['X0-USDT.OKX']
        quotes['X0-USDT.OKX'].bid_price=quotes['X0-USDT.OKX'].ask_price='110'
        drive(e,101)
        self.assertLess(e.targets['X0-USDT.OKX'],old)
        self.assertTrue(any(o.side=='SELL' and o.instrument=='X0-USDT.OKX' for o in orders))

    def test_current_price_sizes_every_order_below_the_absolute_notional_cap(self):
        e,orders,_,_,quotes,_=fixture(count=1,position='1',mode='continuous_rebalance',
                                      quoteSizeLots=1000,inventoryBandLots=1000,maxOrderNotionalUsdt=10)
        quote=quotes['X0-USDT.OKX'];quote.bid_price=quote.ask_price='200'
        drive(e)
        self.assertTrue(orders)
        self.assertTrue(all(order.quantity*order.price<=Decimal(10) for order in orders))

    def test_minimum_lot_above_absolute_cap_waits_without_risk_rejection(self):
        e,orders,_,_,quotes,_=fixture(count=1,position='1',mode='continuous_rebalance',
                                      quoteSizeLots=1000,inventoryBandLots=1000,maxOrderNotionalUsdt=1)
        quote=quotes['X0-USDT.OKX'];quote.bid_price=quote.ask_price='200'
        drive(e)
        self.assertEqual(orders,[])
        self.assertEqual(e.reasons['X0-USDT.OKX'],'order_notional_cap')

    def test_stale_quotes_cancel_working_orders_and_report_reason(self):
        e,orders,cancelled,cache,quotes,_=fixture();drive(e)
        for row in cache.values():row.status='ACCEPTED'
        for quote in quotes.values():quote.ts_init=0
        drive(e,101)
        self.assertEqual(len(cancelled),4)
        self.assertEqual(set(e.reasons.values()),{'waiting_quote'})

    def test_reducing_mode_never_submits_buys(self):
        e,orders,_,_,_,runtime=fixture();runtime.state='REDUCING'
        drive(e)
        self.assertTrue(orders);self.assertTrue(all(o.side=='SELL' for o in orders))

    def test_rate_budgets_wait_instead_of_submitting_into_risk_rejection(self):
        e,orders,*_=fixture(maxOrdersPerSecond=1)
        drive(e);drive(e,100.2)
        self.assertEqual(len(orders),1)
        drive(e,101.1)
        self.assertEqual(len(orders),2)
        self.assertTrue(e.reasons)

    def test_parameters_are_named_data_not_executable_overrides(self):
        spec={'executionOptionsVersion':1}
        self.assertEqual(settings(spec)['mode'],'continuous_quotes')
        for supplied in ({'mode':'mystery'},{'quoteIntervalMs':True},{'quoteSizeLots':0},
                         {'maxOrderNotionalUsdt':0},{'maxOrderNotionalUsdt':1001},
                         {'maxOrdersPerSecond':501},{'workerPath':'evil.py'},
                         {'quoteSizeLots':3,'inventoryBandLots':2}):
            with self.assertRaises(ValueError):settings(spec,supplied)


if __name__=='__main__':unittest.main()
