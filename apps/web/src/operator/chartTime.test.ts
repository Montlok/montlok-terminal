import { TickMarkType, type UTCTimestamp } from 'lightweight-charts';
import { describe, expect, it } from 'vitest';
import { chartCrosshairFormatter, chartTickFormatter } from './chartTime';

const stamp = (value: string) => (Date.parse(value) / 1000) as UTCTimestamp;
describe('Shanghai chart labels', () => {
  it('rolls over the date at Shanghai midnight without mutating epoch seconds', () => {
    const before = stamp('2026-09-05T15:59:59Z');
    const after = stamp('2026-09-05T16:00:00Z');
    expect(chartCrosshairFormatter(before)).toBe('2026-09-05 23:59:59 UTC+8');
    expect(chartCrosshairFormatter(after)).toBe('2026-09-06 00:00:00 UTC+8');
    expect(chartTickFormatter(after, TickMarkType.Time, 'en-US')).toBe('00:00');
    expect(chartTickFormatter(after, TickMarkType.DayOfMonth, 'en-US')).toBe(
      '09/06',
    );
    expect(before + 1).toBe(after);
  });
  it('formats year/month rollover and second ticks with at most 8 characters', () => {
    const time = stamp('2026-12-31T16:00:01Z');
    expect(chartTickFormatter(time, TickMarkType.Year, '')).toBe('2027');
    expect(chartTickFormatter(time, TickMarkType.Month, '')).toBe('1月');
    expect(chartTickFormatter(time, TickMarkType.TimeWithSeconds, '')).toBe(
      '00:00:01',
    );
  });
  it('preserves BusinessDay and date-string calendar labels without adding a time', () => {
    const date = { year: 2026, month: 9, day: 6 };
    expect(chartCrosshairFormatter(date)).toBe('2026-09-06 UTC+8');
    expect(chartCrosshairFormatter('2026-09-06')).toBe('2026-09-06 UTC+8');
    expect(chartTickFormatter(date, TickMarkType.DayOfMonth, '')).toBe('09/06');
    expect(chartTickFormatter('2026-09-06', TickMarkType.Time, '')).toBe(
      '09/06',
    );
    expect(date).toEqual({ year: 2026, month: 9, day: 6 });
  });
  it('does not fabricate a date from unsupported or invalid inputs', () => {
    expect(chartCrosshairFormatter('2026-02-30')).toBe('—');
    expect(chartCrosshairFormatter('not-a-date')).toBe('—');
    expect(chartCrosshairFormatter(NaN as UTCTimestamp)).toBe('—');
  });
});
