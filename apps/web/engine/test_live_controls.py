import asyncio
from decimal import Decimal
from types import SimpleNamespace as NS
import threading
import time
import unittest
from unittest.mock import Mock

from live_controls import NativeControls, reduction_size


def fixture():
    orders = []
    submitted = []
    states, stopped, started, cancelled = [], [], [], []
    node = NS(is_running=True, strategy_ids=['ALPHA','MONTLOK-OPS'],
        set_trading_state=states.append, stop_strategy=stopped.append, resume_strategy=started.append)
    node.cache = NS(orders=lambda:orders, quote=lambda _:NS(ts_init=time.time_ns(),bid_price='100'),
        account_for_venue=lambda _:NS(balance_free=lambda _:NS(as_decimal=lambda:Decimal(10))))
    runtime = NS(request={'runId':'run-one'}, state='ACTIVE',stop=threading.Event(),model_feed=None,
        inventory={'pairs':[dict(instrument='XNVDA-USDT',base='XNVDA',lotSize='.001',minSize='.001',tickSize='.01',takerFeeBps='10')]},
        allocated_positions={'XNVDA-USDT':Decimal('0.25')}, allocated_cash=10)
    def limit(i, side, q, price, **kw):
        return NS(client_order_id='OPS1',instrument=i,side=side,quantity=q,price=price,**kw)
    operator = NS(cancel_order=cancelled.append,order_factory=NS(limit=limit),submit_order=submitted.append)
    native = NS(TradingState=NS(ACTIVE='ACTIVE',REDUCING='REDUCING',HALTED='HALTED'),
        InstrumentId=NS(from_str=str),Currency=NS(from_str=str),Venue=str,Quantity=NS(from_str=Decimal),
        Price=NS(from_str=Decimal),OrderSide=NS(SELL='SELL'),TimeInForce=NS(IOC='IOC'))
    control = NativeControls(node,runtime,operator,native)
    return control,node,runtime,orders,submitted,states,stopped,started,cancelled


class NativeControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_socket_request_waits_for_poll_thread_and_checks_run(self):
        c,_,r,_,_,states,*_=fixture()
        with self.assertRaisesRegex(ValueError,'运行编号'):
            await c.request(dict(command='halt',runId='other',operationId='operation1'))
        pending=asyncio.create_task(c.request(dict(command='halt',runId='run-one',operationId='operation2')))
        await asyncio.sleep(0)
        self.assertEqual(states,[])
        c.process()
        self.assertEqual((await pending)['tradingState'],'HALTED')
        self.assertEqual(r.state,'HALTED')

    async def test_shutdown_finishes_queued_request_without_applying(self):
        c,_,_,_,_,states,*_=fixture()
        pending=asyncio.create_task(c.request(dict(command='halt',runId='run-one',operationId='operation3')))
        await asyncio.sleep(0);c.close()
        with self.assertRaisesRegex(RuntimeError,'未执行'):await pending
        self.assertEqual(states,[])

    def test_halt_and_resume_preserve_strategy_identity_and_native_risk_latch(self):
        c,node,r,_,_,states,stopped,started,_=fixture()
        c.apply(dict(command='halt'))
        self.assertEqual(stopped,['ALPHA'])
        self.assertEqual(states,['HALTED'])
        node.set_trading_state=Mock(side_effect=RuntimeError('reconciliation pending'))
        with self.assertRaisesRegex(RuntimeError,'reconciliation'):c.apply(dict(command='resume'))
        self.assertEqual(r.state,'HALTED');self.assertEqual(started,[])
        node.set_trading_state=states.append
        c.apply(dict(command='resume'))
        self.assertEqual(started,['ALPHA']);self.assertEqual(states[-1],'ACTIVE')

    def test_cancellation_preserves_external_orders_and_waits_for_account_id(self):
        c,_,_,orders,_,_,_,_,cancelled=fixture()
        for strategy,account,status,cid in [('EXTERNAL', 'OKX','ACCEPTED','external'),('ALPHA',None,'SUBMITTED','pending'),
            ('ALPHA','OKX','ACCEPTED','owned'),('ALPHA','OKX','PENDING_CANCEL','already')]:
            orders.append(NS(strategy_id=strategy,account_id=account,status=status,client_order_id=cid))
        c.apply(dict(command='cancel'))
        self.assertEqual(cancelled,['owned'])

    def test_flatten_waits_for_cancels_and_never_sells_whole_wallet(self):
        c,_,r,orders,submitted,*_=fixture()
        order=NS(strategy_id='ALPHA',account_id='OKX',status='PENDING_CANCEL',client_order_id='old')
        orders.append(order)
        c.apply(dict(command='flatten',instruments=['XNVDA-USDT'],operationId='operation4'))
        c.poll();self.assertEqual(submitted,[])
        orders.clear();c.last_poll=0;c.poll()
        self.assertEqual(len(submitted),1)
        self.assertEqual(submitted[0].side,'SELL')
        self.assertLessEqual(submitted[0].quantity,Decimal('.25'))
        self.assertEqual(c.flatten['phase'],'reducing')
        self.assertEqual(submitted[0].price,Decimal('99.90'))
        with self.assertRaisesRegex(ValueError,'库存已变化'):c.apply(dict(command='resume'))
        r.allocated_positions['XNVDA-USDT']=Decimal('0.0002')
        c.last_poll=0;c.poll()
        self.assertEqual(c.flatten['phase'],'completed')
        self.assertEqual(c.flatten['results']['XNVDA-USDT']['state'],'dust')
        self.assertEqual(r.state,'HALTED')

    def test_flatten_rejects_unknown_scope_and_stale_quotes_do_not_submit(self):
        c,node,_,_,submitted,*_=fixture()
        with self.assertRaises(ValueError):c.apply(dict(command='flatten',instruments=['BTC-USDT']))
        self.assertIsNone(c.flatten)
        c.apply(dict(command='flatten',instruments=['XNVDA-USDT'],operationId='operation5'))
        node.cache.quote=lambda _:NS(ts_init=0,bid_price='100')
        c.poll();self.assertEqual(submitted,[])
        self.assertEqual(c.flatten['results']['XNVDA-USDT']['state'],'waiting_quote')
        c.flatten['deadline']=0;c.last_poll=0;c.poll()
        self.assertEqual(c.flatten['phase'],'incomplete')

    def test_cash_reduction_uses_free_balance_lots_and_fee_reserve(self):
        self.assertEqual(reduction_size('2','.5','.01','.01','.001'),Decimal('.49'))
        self.assertEqual(reduction_size('0','10','.01','.01','.001'),0)
        self.assertEqual(reduction_size('-1','10','.01','.01','.001'),0)
        self.assertEqual(reduction_size('.001','10','.001','.001','.001'),0)
