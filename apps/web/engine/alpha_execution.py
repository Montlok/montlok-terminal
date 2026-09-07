"""Continuous Alpha execution on Nautilus's native quotes, risk and order state.

One reservation per instrument/side, retained through cancel acknowledgement.
Portfolio weights remain the released Alpha; this module does not invent a model.
"""
from collections import Counter, deque
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import time

OPEN = {'INITIALIZED','SUBMITTED','ACCEPTED','PARTIALLY_FILLED','PENDING_CANCEL','PENDING_UPDATE'}


def rounded(value, quantum, up=False):
    return (value/quantum).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR)*quantum


class ContinuousExecutor:
    def __init__(self, strategy, native, runtime, snapshot, options):
        self.strategy,self.n,self.runtime,self.options=strategy,native,runtime,options
        self.rows={r['instrument']+'.OKX':r for r in snapshot['pairs']}
        self.positions={k:Decimal(r['baseAvailable']) for k,r in self.rows.items()}
        self.cash=Decimal(snapshot['allocatedCashUsdt'])
        self.quotes={};self.targets={};self.pending={};self.reservations={}
        self.reasons={};self.backoff={};self.last_error=None;self.last_cycle=0.;self.cursor=0
        self.counts=Counter();self.rates={k:deque() for k in ('submit','cancel','fill')}
        self.instrument_requests={};self.ack_ms=deque(maxlen=2048);self.history=deque(maxlen=120)
        self.last_history=0.;self.last_metrics=None
        self.order_ids=deque(maxlen=1000);self.fills=deque(maxlen=1000);self.fees=Decimal(0)
        self.journal=deque()

    def on_quote(self, quote):
        key=str(quote.instrument_id)
        if key in self.rows:
            self.quotes[key]=quote
            self.counts['quoteEvents']+=1

    def on_accepted(self, event):
        slot=self.reservations.get(str(event.client_order_id))
        if slot and not slot.get('acked'):
            slot['acked']=True
            self.counts['accepted']+=1
            self.ack_ms.append((time.monotonic_ns()-slot['sent_ns'])/1e6)

    def on_filled(self, event):
        key=str(event.instrument_id)
        if key not in self.rows:return
        qty=Decimal(str(event.last_qty));price=Decimal(str(event.last_px));buy=event.order_side==self.n.OrderSide.BUY
        self.positions[key]+=qty if buy else -qty
        self.cash+=-qty*price if buy else qty*price
        fee=Decimal(str(event.commission.as_decimal()));ccy=str(event.commission.currency)
        if ccy==self.rows[key]['base']:self.positions[key]-=fee
        elif ccy=='USDT':self.cash-=fee
        self.fees+=fee*price if ccy==self.rows[key]['base'] else fee if ccy=='USDT' else Decimal(0)
        slot=self.reservations.get(str(event.client_order_id))
        if slot:slot['remaining']=max(Decimal(0),slot['remaining']-qty)
        self.counts['fills']+=1;self.rates['fill'].append(time.monotonic())
        record=dict(id=str(getattr(event,'trade_id',self.counts['fills'])),orderId=str(event.client_order_id),
                    venueOrderId=str(getattr(event,'venue_order_id','')),instrument=key,side='BUY' if buy else 'SELL',
                    quantity=str(qty),price=str(price),fee=str(fee),feeCurrency=ccy,
                    time=getattr(event,'ts_event',time.time_ns()),source='okx_live_fill')
        self.fills.append(record);self.journal.append({'type':'fill',**record})

    def on_rejected(self,event):
        identifier=str(event.client_order_id)
        slot=self.reservations.pop(identifier,None)
        if slot:
            self.pending.pop((slot['key'],slot['side']),None)
            self.backoff[slot['key']]=time.monotonic()+1
        self.last_error=str(getattr(event,'reason','订单未接受'))
        self.counts['rejected']+=1
        self.journal.append(dict(type='rejected',orderId=identifier,reason=self.last_error,time=time.time_ns()))

    def refresh_pending(self):
        for oid,slot in list(self.reservations.items()):
            order=self.strategy.cache.order(slot['identifier'])
            if order is not None and str(order.status) not in OPEN:
                self.pending.pop((slot['key'],slot['side']),None)
                del self.reservations[oid]

    def reserved_cash(self):
        return sum((s['remaining']*s['price']*(1+s['fee']) for s in self.reservations.values()
                    if s['side']=='BUY'),Decimal(0))

    def allowed(self,key,kind,now):
        history=self.rates[kind]
        while history and now-history[0]>=60:history.popleft()
        per=self.instrument_requests.setdefault((key,kind),deque())
        while per and now-per[0]>=2:per.popleft()
        # Submit and cancel are independent venue buckets, shared across sides.
        if len(per)>=60:return False
        return sum(now-t<1 for t in history)<self.options['maxOrdersPerSecond']

    def record_request(self,key,kind,now):
        self.rates[kind].append(now);self.instrument_requests.setdefault((key,kind),deque()).append(now)
        self.counts[kind]+=1

    def cancel(self,slot,now):
        if slot['cancel_requested'] or not self.allowed(slot['key'],'cancel',now):return
        order=self.strategy.cache.order(slot['identifier'])
        if order is None or str(order.status) in {'INITIALIZED','SUBMITTED','PENDING_CANCEL','PENDING_UPDATE'}:return
        self.strategy.cancel_order(slot['identifier'])
        slot['cancel_requested']=True
        self.record_request(slot['key'],'cancel',now)
        self.journal.append(dict(type='cancel_requested',orderId=str(slot['identifier']),time=time.time_ns()))

    def intents(self,key,bid,ask,nav,reducing):
        row=self.rows[key];lot=Decimal(row['lotSize']);minimum=Decimal(row['minSize'])
        position=self.positions[key];mid=(bid+ask)/2;weight=Decimal(str(row.get('weight',0)))
        target=rounded(nav*weight/mid,lot);self.targets[key]=target
        size=rounded(minimum*self.options['quoteSizeLots'],lot,True)
        fee=Decimal(row['makerFeeBps'])/10000
        band=minimum*self.options['inventoryBandLots']*(1+fee)
        if self.options['mode']=='continuous_rebalance':lower=upper=target
        else:lower=max(Decimal(0),target-band);upper=target+band if weight>0 else Decimal(0)
        half=(Decimal(row['makerFeeBps'])+Decimal(self.options['minimumEdgeBps'])/2)/10000
        tick=Decimal(row['tickSize'])
        buy_px=rounded(min(bid,mid*(1-half)),tick)
        sell_px=rounded(max(ask,mid*(1+half)),tick,True)
        # Outside the released target, join the best passive price to rebalance.
        if position<lower:buy_px=rounded(bid,tick)
        if position>upper:sell_px=rounded(ask,tick,True)
        buy_requested=rounded(min(size,max(Decimal(0),upper-position)),lot)
        sell_requested=rounded(min(size,max(Decimal(0),position-lower)/(1+fee)),lot)
        notional_cap=Decimal(str(self.options['maxOrderNotionalUsdt']))
        buy_qty=rounded(min(buy_requested,notional_cap/buy_px),lot)
        sell_qty=rounded(min(sell_requested,notional_cap/sell_px),lot)
        intents={}
        if buy_qty>=minimum and buy_px>0 and not reducing:intents['BUY']=(buy_qty,buy_px,fee)
        if sell_qty>=minimum and sell_px>0:intents['SELL']=(sell_qty,sell_px,fee)
        capped = ((buy_requested>=minimum and buy_qty<minimum and not reducing)
                  or (sell_requested>=minimum and sell_qty<minimum))
        return intents, 'order_notional_cap' if capped and not intents else None

    def drive(self):
        now=time.monotonic()
        if now-self.last_cycle<self.options['quoteIntervalMs']/1000:return
        self.last_cycle=now;self.refresh_pending();self.counts['decisionCycles']+=1
        rows=list(self.rows);self.cursor=(self.cursor+1)%max(1,len(rows));rows=rows[self.cursor:]+rows[:self.cursor]
        nav=self.cash
        for key,row in self.rows.items():
            quote=self.quotes.get(key) or self.strategy.cache.quote(self.n.InstrumentId.from_str(key))
            if quote:self.quotes[key]=quote
            mark=(Decimal(str(quote.bid_price))+Decimal(str(quote.ask_price)))/2 if quote else (Decimal(row['bid'])+Decimal(row['ask']))/2
            nav+=self.positions[key]*mark
        reducing=self.runtime.state=='REDUCING'
        for key in rows:
            row=self.rows[key];quote=self.quotes.get(key);reason=None
            fresh=quote is not None and 0<=time.time_ns()-quote.ts_init<=5_000_000_000
            if not fresh:
                desired={};reason='waiting_quote'
            elif now<self.backoff.get(key,0):
                desired={};reason='retry_backoff'
            else:
                bid,ask=Decimal(str(quote.bid_price)),Decimal(str(quote.ask_price))
                if bid<=0 or ask<bid:desired={};reason='invalid_quote'
                else:desired,reason=self.intents(key,bid,ask,max(nav,Decimal(0)),reducing)
            for side in ('SELL','BUY'):
                oid=self.pending.get((key,side));slot=self.reservations.get(oid)
                intent=desired.get(side)
                if slot:
                    if (intent is None or now-slot['sent_at']>=self.options['orderExpirySecs'] or
                        intent and (slot['remaining']>intent[0] or abs(intent[1]/slot['price']-1)*10000>=self.options['requoteThresholdBps']
                                    and intent[1]!=slot['price'])):
                        self.cancel(slot,now)
                    continue
                if not intent:continue
                if not self.allowed(key,'submit',now):reason='request_budget';continue
                quantity,price,fee=intent
                if side=='BUY':
                    available=max(Decimal(0),self.cash-self.reserved_cash())
                    quantity=min(quantity,rounded(available/(price*(1+fee)),Decimal(row['lotSize'])))
                if quantity<Decimal(row['minSize']):reason='available_cash' if side=='BUY' else 'minimum_size';continue
                order=self.strategy.order_factory.limit(self.n.InstrumentId.from_str(key),getattr(self.n.OrderSide,side),
                    self.n.Quantity.from_str(format(quantity,'f')),self.n.Price.from_str(format(price,'f')),post_only=True)
                oid=str(order.client_order_id)
                slot=dict(key=key,side=side,remaining=quantity,price=price,fee=fee,identifier=order.client_order_id,
                          sent_ns=time.monotonic_ns(),sent_at=now,cancel_requested=False)
                self.reservations[oid]=slot;self.pending[(key,side)]=oid
                try:self.strategy.submit_order(order)
                except Exception:
                    self.reservations.pop(oid,None);self.pending.pop((key,side),None);raise
                self.record_request(key,'submit',now)
                self.order_ids.append(order.client_order_id)
                self.journal.append(dict(type='submitted',orderId=oid,instrument=key,side=side,
                                         quantity=str(quantity),price=str(price),time=time.time_ns()))
            working=any((key,s) in self.pending for s in ('BUY','SELL'))
            self.reasons[key]=reason or ('quoting' if working else 'target_within_minimum')

    def metrics(self):
        now=time.monotonic()
        # Separate observer frequency from the native market/execution loop.
        if self.last_metrics and now-self.last_history<1:return self.last_metrics
        self.last_history=now
        for history in self.rates.values():
            while history and now-history[0]>=60:history.popleft()
        recent={k:sum(now-t<60 for t in v) for k,v in self.rates.items()}
        per_second={k:sum(now-t<1 for t in v) for k,v in self.rates.items()}
        self.history.append(dict(time=int(time.time()),orders=per_second['submit'],fills=per_second['fill'],cancels=per_second['cancel']))
        timings=sorted(self.ack_ms)
        self.last_metrics=dict(mode=self.options['mode'],phase='QUOTING' if self.reservations else 'TRACKING',
            **self.counts,openOrders=len(self.reservations),reservedCashUsdt=str(self.reserved_cash()),
            freeCashUsdt=str(max(Decimal(0),self.cash-self.reserved_cash())),lastMinute=recent,perSecond=per_second,
            ackP50Ms=timings[len(timings)//2] if timings else None,
            ackP95Ms=timings[min(len(timings)-1,int(len(timings)*.95))] if timings else None,
            lastError=self.last_error,history=list(self.history),parameters=self.options,
            instruments=[dict(instrument=k.removesuffix('.OKX'),position=str(self.positions[k]),
                              target=str(self.targets[k]) if k in self.targets else None,reason=self.reasons.get(k,'waiting_quote')) for k in self.rows])
        return self.last_metrics

    def flush_events(self,directory):
        if not self.journal:return
        import json,os
        records=list(self.journal)
        with (directory/'execution-events.jsonl').open('a') as stream:
            stream.write(''.join(json.dumps(row,separators=(',',':'))+'\n' for row in records))
            stream.flush();os.fsync(stream.fileno())
        for _ in records:self.journal.popleft()
