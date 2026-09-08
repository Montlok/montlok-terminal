import { afterEach,describe,expect,it,vi } from 'vitest';
import { Table,tableToIPC,vectorFromArray,Binary } from 'apache-arrow';
import { TerminalStreamStore } from '@montlok/sdk';
import { EventEnvelope,EventType,ServerMessage } from '@montlok/sdk/protocol';

function event(sequence:bigint,type=EventType.FILL_RECEIVED){return EventEnvelope.fromPartial({
  schemaVersion:2,stream:'run.a',streamSeq:sequence,eventId:`event-${sequence}`,eventType:type,
  occurredAtNs:1800000000000000000n+sequence,receivedAtNs:1800000000000000100n+sequence,
  instrumentId:'XTSLA-USDT',runId:'a',
});}
function frame(value:EventEnvelope):ServerMessage{return {message:{$case:'event',event:value}};}
afterEach(()=>vi.useRealTimers());

describe('shared terminal stream',()=>{
  it('retains exact nanoseconds and 64-bit sequence values through the generated codec',()=>{
    const value=event(9007199254740993n);const decoded=EventEnvelope.decode(EventEnvelope.encode(value).finish());
    expect(decoded.streamSeq).toBe(9007199254740993n);expect(decoded.occurredAtNs).toBe(value.occurredAtNs);
  });
  it('does not advance across gaps and accepts only a contiguous replay',()=>{
    vi.useFakeTimers();const recover=vi.fn();const store=new TerminalStreamStore(recover);const critical=vi.fn();store.onCritical(critical);
    store.ingest(frame(event(1n)));store.ingest(frame(event(3n)));
    expect(recover).toHaveBeenCalledWith({stream:'run.a',after:1n});expect(store.resumePositions()[0].lastStreamSeq).toBe(1n);
    store.ingest(frame(event(2n)));store.ingest(frame(event(3n)));store.ingest(frame(event(2n)));
    expect(critical).toHaveBeenCalledTimes(3);expect(store.resumePositions()[0].lastStreamSeq).toBe(3n);store.dispose();
  });
  it('processes 10000 critical events without dropping them or repainting per event',()=>{
    vi.useFakeTimers();const recover=vi.fn();const store=new TerminalStreamStore(recover);let delivered=0;
    const repaint=vi.fn();store.subscribe(repaint);store.onCritical(()=>delivered++);const prior=store.getSnapshot();
    for(let n=1;n<=10000;n++)store.ingest(frame(event(BigInt(n))));
    expect(delivered).toBe(10000);expect(recover).not.toHaveBeenCalled();expect(repaint).not.toHaveBeenCalled();
    expect(prior.events.size).toBe(0);vi.advanceTimersByTime(50);expect(repaint).toHaveBeenCalledTimes(1);
    expect(store.getSnapshot().events.size).toBe(10000);expect(store.getSnapshot().positions.get('run.a')).toBe(10000n);store.dispose();
  });
  it('conflates market rendering separately from sequence gaps',()=>{
    vi.useFakeTimers();const store=new TerminalStreamStore(vi.fn());
    for(let n=1;n<=200;n++){const value=event(BigInt(n),EventType.MARKET_QUOTE);value.payload={$case:'marketQuote',marketQuote:{venueTimeNs:value.occurredAtNs}};store.ingest(frame(value));}
    vi.advanceTimersByTime(50);expect(store.getSnapshot().conflated).toBe(199);expect(store.getSnapshot().gaps).toBe(0);expect(store.getSnapshot().events.size).toBe(1);store.dispose();
  });
  it('publishes a complete Arrow snapshot atomically and retains the last good snapshot during transfer',()=>{
    vi.useFakeTimers();const store=new TerminalStreamStore(vi.fn());store.ingest(frame(event(1n)));vi.advanceTimersByTime(50);
    const positions=[{stream:'run.a',lastStreamSeq:2n}];
    store.ingest({message:{$case:'snapshotBegin',snapshotBegin:{subscriptionId:'snapshot',snapshotSeq:0n,arrowSchema:new Uint8Array(),watermarks:positions}}});
    const data=new Table({payload_protobuf:vectorFromArray([EventEnvelope.encode(event(2n)).finish()],new Binary())});
    store.ingest({message:{$case:'snapshotBatch',snapshotBatch:{subscriptionId:'snapshot',rowCount:1,arrowIpc:tableToIPC(data,'stream')}}});
    vi.advanceTimersByTime(50);expect(store.getSnapshot().events.has('run.a/event-1')).toBe(true);
    store.ingest({message:{$case:'snapshotEnd',snapshotEnd:{subscriptionId:'snapshot',snapshotSeq:0n,lastStreamSeq:0n,watermarks:positions}}});
    vi.advanceTimersByTime(50);expect(store.getSnapshot().events.has('run.a/event-1')).toBe(false);expect(store.getSnapshot().events.has('run.a/event-2')).toBe(true);store.dispose();
  });
});
