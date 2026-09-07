#!/usr/bin/env python3
"""Four-sector Alpha allocation with selectable continuous native-backed execution."""
from __future__ import annotations
import argparse
import asyncio
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import time

from live_hft_worker import registered, run, sha, encode, public_snapshot


def target_quantity(capital, weight, ask, fee, lot):
    # Matches the tested sector strategy: 10bp IOC limit and fee-inclusive size.
    limit = ask * Decimal('1.001')
    return (capital * weight / limit / (1 + fee) // lot) * lot


async def capture(spec, config):
    import aiohttp
    from cryptography.fernet import Fernet
    sys.path.insert(0, spec['operatorServerPath'])
    from exchange import rest
    signal_path = Path(spec['signalsPath'])
    if sha(signal_path) != spec['signalsSha256']:
        raise ValueError('Alpha 权重版本不一致')
    signal = json.loads(signal_path.read_text())
    weights = {k.removesuffix('.OKX'): v for k, v in signal['instruments'].items()}
    if any(not 0 <= Decimal(str(v['weight'])) <= Decimal('.10') for v in weights.values()):
        raise ValueError('Alpha 单标的权重超过已发布的 10% 配置')
    if sum(Decimal(str(v['weight'])) for v in weights.values()) > Decimal('.951'):
        raise ValueError('Alpha 总权重不一致')
    root = Path(spec['profileStatePath'])
    state = json.loads(Fernet((root/'master.key').read_bytes()).decrypt((root/'profiles.enc').read_bytes()))
    profile = state['profiles'][spec['profileId']]
    if profile['mode'] not in ('live', 'live_readonly') or profile['site'] != 'global':
        raise ValueError('Alpha 实盘账户环境不一致')
    async with aiohttp.ClientSession() as session:
        async def call(path, params=None):
            for attempt in range(4):
                try: return (await rest(session, profile, 'GET', path, params))['data']
                except RuntimeError as error:
                    if '50011' not in str(error) or attempt == 3: raise
                    await asyncio.sleep(.5 * (2 ** attempt))
        account, balance, pending, instruments, tickers, fee_rows = await asyncio.gather(
            call('/api/v5/account/config'), call('/api/v5/account/balance'),
            call('/api/v5/trade/orders-pending', {'instType': 'SPOT'}),
            call('/api/v5/public/instruments', {'instType': 'SPOT'}),
            call('/api/v5/market/tickers', {'instType': 'SPOT'}),
            call('/api/v5/account/trade-fee', {'instType':'SPOT'}))
        identity = {k: account[0].get(k) for k in ('uid','mainUid','type','acctLv','posMode','perm','ip')}
        if any(str(identity.get(k)) != str(v) for k, v in config['account'].items()):
            raise ValueError('Alpha 账户身份或 IP 不一致')
        if not {'read_only','trade'}.issubset(set(identity['perm'].split(','))):
            raise ValueError('Alpha 账户缺少交易权限')
        if 'withdraw' in set(identity['perm'].split(',')):
            raise ValueError('策略账户 API 含提现权限')
        owned = {r['ccy']: Decimal(r.get('availBal') or '0') for r in balance[0]['details']}
        metas = {r['instId']: r for r in instruments}; quotes = {r['instId']: r for r in tickers}
        universe = set(weights) | {f'{ccy}-USDT' for ccy, qty in owned.items() if ccy.startswith('X') and qty > 0 and f'{ccy}-USDT' in metas}
        if not universe.issubset(set(config['instruments'])):
            raise ValueError('账户股票范围已变化，需要更新已发布范围')
        if any(r['instId'] in universe for r in pending):
            raise ValueError('目标股票存在未完成委托，请先核对')
        cash = owned.get('USDT', Decimal(0)); capital = cash
        for symbol in universe:
            if symbol not in quotes or metas[symbol]['state'] != 'live':
                raise ValueError(f'{symbol} 暂不可交易')
            q = quotes[symbol]; bid, ask = Decimal(q['bidPx'] or '0'), Decimal(q['askPx'] or '0')
            if bid <= 0 or ask < bid: raise ValueError(f'{symbol} 报价无效')
            capital += owned.get(symbol.split('-')[0], Decimal(0)) * (bid + ask) / 2
        pairs = []; plan = []
        fee_summary=fee_rows[0]
        fee_groups={str(row['groupId']):row for row in fee_summary.get('feeGroup',[])}
        for symbol in sorted(universe):
            m, q = metas[symbol], quotes[symbol]
            # Normal fee tiers share explicit fee groups. Incentive programs
            # retain the instrument-specific lookup required by OKX.
            fee_row=fee_groups.get(str(m.get('groupId'))) if fee_summary.get('ruleType')=='normal' else None
            if fee_row is None:
                fee_row = (await call('/api/v5/account/trade-fee', {'instType':'SPOT','instId':symbol}))[0]
                await asyncio.sleep(.42)
            fee = abs(Decimal(fee_row['taker'])); maker = abs(Decimal(fee_row['maker']))
            bid, ask = Decimal(q['bidPx']), Decimal(q['askPx']); lot = Decimal(m['lotSz']); minimum = Decimal(m['minSz'])
            base = symbol.split('-')[0]; initial = owned.get(base, Decimal(0))
            w = Decimal(str(weights.get(symbol, {}).get('weight', 0)))
            target = target_quantity(capital, w, ask, fee, lot); delta = target - initial
            quantity = (abs(delta) // lot) * lot
            cost = (ask-bid)/((ask+bid)/2)*10000 + fee*20000 + 20
            row = dict(instrument=symbol, base=base, baseAvailable=str(initial), quoteAvailable=str(cash),
                bid=str(bid), ask=str(ask), tickSize=m['tickSz'], lotSize=m['lotSz'], minSize=m['minSz'],
                tradeSize=str(max(quantity,minimum*spec.get('_executionSettings',{}).get('quoteSizeLots',1))), initialPosition=str(initial), minPosition='0',
                maxPosition=str(max(initial,target)), plannedQuoteExposureUsdt=str(max(delta,Decimal(0))*ask),
                makerFeeBps=str(maker*10000), takerFeeBps=str(fee*10000), gridStepBps='12',
                targetQuantity=str(target), weight=float(w), sector=weights.get(symbol,{}).get('sector'),
                roundtripCostBps=str(cost), volume24hQuote=q.get('volCcy24h'))
            pairs.append(row)
            if quantity >= minimum:
                plan.append(dict(instrument=symbol, side='buy' if delta>0 else 'sell', quantity=str(quantity),
                                 estimatedUsdt=str(quantity*(ask if delta>0 else bid)), costBps=str(cost)))
        comparable = {'account': identity, 'cash': str(cash), 'inventory': {r['instrument']:r['baseAvailable'] for r in pairs},
                      'signals': spec['signalsSha256']}
        return dict(schemaVersion=1, profileId=spec['profileId'], account=identity, totalEqUsd=balance[0].get('totalEq'),
            allocationCapitalUsdt=str(capital), allocatedCashUsdt=str(cash), strategy=signal['strategy'],
            signalAsOf=signal['signal_as_of'], pairs=pairs, orderPlan=plan, openManagedPairOrders=0,
            excluded=[{'asset':c,'available':str(q),'reason':'未纳入股票组合'} for c,q in owned.items() if q>0 and c!='USDT' and not c.startswith('X')],
            inventoryHash=hashlib.sha256(encode(comparable).encode()).hexdigest(), observedAtMs=int(time.time()*1000),
            executionSettings=spec.get('_executionSettings'),
            credentials={k:profile[k] for k in ('apiKey','secret','passphrase')})


class AlphaController:
    def __init__(self, runtime):
        self.strategy = None
        self.failure = None
        self.runtime = runtime
    def snapshot(self): return None
    def close(self): pass
    def poll(self):
        if self.runtime.state in {'ACTIVE','REDUCING'} and not (self.runtime.controls and self.runtime.controls.paused):
            self.strategy.drive()
    def reconcile_allocation(self, positions, cash):
        if self.strategy.executor is not None:
            # Native fills update this ledger ahead of observer snapshots.
            return
        if positions:
            self.strategy.positions.update({key+'.OKX':Decimal(str(value)) for key,value in positions.items()})
            self.strategy.cash = Decimal(str(cash))
    def allocation_status(self):
        s = self.strategy
        if s.executor is not None:
            value=s.executor.metrics()
            return dict(phase=value['phase'],remaining=dict(s.executor.reasons),cashUsdt=str(s.executor.cash))
        return {'phase': s.phase, 'remaining': dict(s.reasons), 'cashUsdt': str(s.cash), 'attempts': dict(s.attempts)}
    def execution_status(self):
        return self.strategy.executor.metrics() if self.strategy.executor is not None else None
    def external_fill(self,event):
        if self.strategy.executor is not None:self.strategy.executor.on_filled(event)


def attach(node, runtime, spec, snapshot, config):
    from nautilus_trader.core import nautilus_pyo3 as n
    control = AlphaController(runtime)
    rows = {r['instrument']+'.OKX':r for r in snapshot['pairs']}
    class SectorAlpha(n.Strategy):
        def __init__(self, cfg):
            super().__init__(cfg)
            self.positions = {key:Decimal(row['baseAvailable']) for key,row in rows.items()}
            self.cash = Decimal(snapshot['allocatedCashUsdt'])
            self.phase = 'SELL'; self.pending = None; self.last_send=0.0
            self.attempts={}; self.last_attempt={}; self.reasons={}
            self.executor=None
            options=snapshot.get('executionSettings')
            if options and options['mode']!='initial_allocation':
                from alpha_execution import ContinuousExecutor
                self.executor=ContinuousExecutor(self,n,runtime,snapshot,options)
        def on_start(self):
            for key in rows: self.subscribe_quotes(n.InstrumentId.from_str(key))
        def on_resume(self): self.on_start()
        def on_quote(self,event):
            if self.executor is not None:self.executor.on_quote(event)
        def on_order_accepted(self,event):
            if self.executor is not None:self.executor.on_accepted(event)
        def on_order_filled(self, event):
            if self.executor is not None:
                self.executor.on_filled(event);return
            key=str(event.instrument_id); row=rows[key]; qty=Decimal(str(event.last_qty)); price=Decimal(str(event.last_px))
            buying=event.order_side==n.OrderSide.BUY
            self.positions[key]+=qty if buying else -qty
            self.cash+=-qty*price if buying else qty*price
            fee=Decimal(str(event.commission.as_decimal())); quantum=Decimal(1).scaleb(-event.commission.currency.precision)
            if str(event.commission.currency)==row['base']:
                self.positions[key]-=max(fee,qty*Decimal(row['takerFeeBps'])/10000)+quantum
            elif str(event.commission.currency)=='USDT':
                self.cash-=max(fee,qty*price*Decimal(row['takerFeeBps'])/10000)+quantum
        def on_order_rejected(self,event):
            if self.executor is not None:
                self.executor.on_rejected(event);return
            control.failure=str(event.reason)
        def on_order_denied(self,event):
            if self.executor is not None:self.executor.on_rejected(event)
        def drive(self):
            if self.executor is not None:
                self.executor.drive();return
            if self.phase=='ALLOCATED' or time.monotonic()-self.last_send<.125: return
            if self.pending:
                previous=self.cache.order(self.pending)
                if previous is None or str(previous.status) in {'INITIALIZED','SUBMITTED','ACCEPTED','PARTIALLY_FILLED','PENDING_CANCEL','PENDING_UPDATE'}: return
                self.pending=None
            candidates=[]
            for key,row in rows.items():
                delta=Decimal(row['targetQuantity'])-self.positions[key]
                quantity=(abs(delta)//Decimal(row['lotSize']))*Decimal(row['lotSize'])
                if quantity<Decimal(row['minSize']): self.reasons[key]='allocated';continue
                candidates.append((key,row,delta,quantity))
            if not candidates:
                self.phase='ALLOCATED'
                return
            # Reduce excess inventory first, but an illiquid sell must not
            # prevent other symbols from using cash already available.
            candidates.sort(key=lambda item: item[2] >= 0)
            for key,row,delta,quantity in candidates:
                if runtime.state == 'REDUCING' and delta > 0:
                    self.reasons[key]='paused_buy';continue
                if self.attempts.get(key,0)>=5: self.reasons[key]='attempt_limit';continue
                if time.monotonic()-self.last_attempt.get(key,0)<5:continue
                identifier=n.InstrumentId.from_str(key); quote=self.cache.quote(identifier)
                if quote is None or time.time_ns()-quote.ts_init>5_000_000_000: self.reasons[key]='awaiting_fresh_quote';continue
                bid,ask=Decimal(str(quote.bid_price)),Decimal(str(quote.ask_price)); fee=Decimal(row['takerFeeBps'])/10000
                if (ask-bid)/((ask+bid)/2)*10000+fee*20000+20>60: self.reasons[key]='cost_over_60bps';continue
                buying=delta>0; tick=Decimal(row['tickSize']); lot=Decimal(row['lotSize'])
                px=(ask*Decimal('1.001') if buying else bid*Decimal('.999'))
                px=((px/tick).to_integral_value(rounding='ROUND_CEILING' if buying else 'ROUND_FLOOR'))*tick
                if buying: quantity=min(quantity,(max(Decimal(0),self.cash)*Decimal('.999')/(px*(1+fee))//lot)*lot)
                if quantity<Decimal(row['minSize']):self.reasons[key]='below_minimum';continue
                order=self.order_factory.limit(identifier,n.OrderSide.BUY if buying else n.OrderSide.SELL,
                    n.Quantity.from_str(format(quantity,'f')),n.Price.from_str(format(px,'f')),time_in_force=n.TimeInForce.IOC)
                self.pending=order.client_order_id;self.attempts[key]=self.attempts.get(key,0)+1
                self.last_attempt[key]=self.last_send=time.monotonic();self.reasons[key]='submitted'
                self.phase='BUY' if buying else 'SELL'
                self.submit_order(order);return
            self.phase='WAITING'
        def on_stop(self):
            for key in rows:
                identifier=n.InstrumentId.from_str(key)
                self.cancel_all_orders(identifier);self.unsubscribe_quotes(identifier)
    strategy=SectorAlpha(n.StrategyConfig(strategy_id=n.StrategyId('ALPHA-6040'),order_id_tag='ALPHA',
        use_uuid_client_order_ids=True,use_hyphens_in_client_order_ids=False))
    control.strategy=strategy;node.add_strategy(strategy);return control


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--group-id',required=True);parser.add_argument('--request',type=Path);parser.add_argument('--preview',action='store_true')
    parser.add_argument('--execution-settings')
    args=parser.parse_args();_,spec,config=registered(args,__file__)
    sys.path.insert(0,spec['operatorServerPath'])
    from execution_options import settings, rate_config
    supplied=(json.loads(args.request.read_text()).get('executionSettings') if args.request else
              json.loads(args.execution_settings) if args.execution_settings else None)
    options=settings(spec,supplied)
    if options:spec={**spec,'_executionSettings':options};config=rate_config(config,options)
    snapshot=asyncio.run(capture(spec,config))
    if args.preview:print(json.dumps(public_snapshot(snapshot),ensure_ascii=False));return
    run(args,spec,config,snapshot,lambda node,runtime:attach(node,runtime,spec,snapshot,config))

if __name__=='__main__':main()
