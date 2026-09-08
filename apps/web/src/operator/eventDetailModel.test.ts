import { describe, it, expect } from 'vitest';
import { EventEnvelope, EventType, Side } from '@montlok/sdk/protocol';
import {
  decodeRelated,
  elapsedMs,
  eventFields,
  eventTime,
  sameRun,
} from './eventDetailModel';
describe('event detail', () => {
  it('keeps nanoseconds and sub-millisecond intervals exact', () => {
    expect(eventTime(1800000000123456789n)).toMatch(/\.123456789$/);
    expect(elapsedMs(1800000000123456789n, 1800000000123456790n)).toBe(
      '0.000001',
    );
    expect(elapsedMs(2n, 1n)).toBe('—');
  });
  it('keeps Decimal values as supplied and does not invent price or quantity', () => {
    const event = EventEnvelope.fromPartial({
      eventType: EventType.FILL_RECEIVED,
      payload: {
        $case: 'fillReceived',
        fillReceived: {
          price: { value: '123456789.123456789' },
          fee: { value: '0' },
          feeCurrency: 'USDT',
          side: Side.SELL,
        },
      },
    });
    const fields = Object.fromEntries(eventFields(event));
    expect(fields['成交价格']).toBe('123456789.123456789');
    expect(fields['成交数量']).toBe('');
    expect(fields['费用']).toBe('0 USDT');
  });
  it('decodes related history without JSON precision loss and checks run scope', () => {
    const event = EventEnvelope.fromPartial({
      streamSeq: 9007199254740993n,
      runId: 'run',
      accountId: 'a',
      strategyGroupId: 'g',
    });
    const bytes = EventEnvelope.encode(event).finish();
    const encoded = btoa(String.fromCharCode(...bytes));
    expect(
      decodeRelated({
        encoding: 'protobuf-base64',
        events: [encoded],
        truncated: false,
      })[0].streamSeq,
    ).toBe(event.streamSeq);
    expect(sameRun(event, { ...event, accountId: 'b' })).toBe(false);
    expect(() =>
      decodeRelated({ encoding: 'json', events: [], truncated: false }),
    ).toThrow();
  });
});
