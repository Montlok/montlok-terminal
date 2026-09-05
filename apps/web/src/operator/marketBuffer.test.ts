import { describe, expect, it } from 'vitest';
import { MarketBuffer } from './marketBuffer';

describe('market frame coalescing', () => {
  it('preserves ticker data when a React updater is deferred beyond the buffer reset', () => {
    const buffer = new MarketBuffer();
    buffer.pending.ticker = {last:'100'};
    const first = buffer.drain();
    const deferred = () => ({...first});
    buffer.pending.ticker = {last:'101'};
    const second = buffer.drain();
    expect(deferred().ticker.last).toBe('100');
    expect(second?.ticker.last).toBe('101');
  });
  it('does not dispatch another update without a new event', () => {
    const buffer = new MarketBuffer();
    buffer.appendTrades([{px:'100'}]);
    expect(buffer.drain()?.trades).toHaveLength(1);
    for (let tick=0; tick<1000; tick++) expect(buffer.drain()).toBeUndefined();
  });
  it('retains only the latest bounded trade list', () => {
    const buffer = new MarketBuffer();
    buffer.appendTrades(Array.from({length:100},(_,i)=>({id:i})));
    expect(buffer.drain()?.trades).toHaveLength(60);
  });
});
