import {
  type BusinessDay,
  type TickMarkFormatter,
  TickMarkType,
  type Time,
  type TimeFormatterFn,
} from 'lightweight-charts';

export const CHART_TIME_ZONE = 'Asia/Shanghai';
export const CHART_TIME_ZONE_LABEL = 'UTC+8';
const formatter = new Intl.DateTimeFormat('en-GB', {
  timeZone: CHART_TIME_ZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
});
type Parts = {
  year: string;
  month: string;
  day: string;
  hour?: string;
  minute?: string;
  second?: string;
};

function calendarParts(day: BusinessDay): Parts | undefined {
  const date = new Date(Date.UTC(day.year, day.month - 1, day.day));
  if (
    date.getUTCFullYear() !== day.year ||
    date.getUTCMonth() + 1 !== day.month ||
    date.getUTCDate() !== day.day
  )
    return undefined;
  return {
    year: String(day.year),
    month: String(day.month).padStart(2, '0'),
    day: String(day.day).padStart(2, '0'),
  };
}

function parts(time: Time): Parts | undefined {
  if (typeof time === 'number') {
    const date = new Date(time * 1000);
    if (!Number.isFinite(date.getTime())) return undefined;
    return Object.fromEntries(
      formatter.formatToParts(date).map((part) => [part.type, part.value]),
    ) as Parts;
  }
  // BusinessDay is a calendar label, not an instant. Keep its date as supplied.
  if (typeof time === 'string') {
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(time);
    return match
      ? calendarParts({
          year: Number(match[1]),
          month: Number(match[2]),
          day: Number(match[3]),
        })
      : undefined;
  }
  return calendarParts(time);
}

/** Format labels only: UTC epoch values passed to the chart remain unmodified. */
export const chartTickFormatter: TickMarkFormatter = (time, tickType) => {
  const value = parts(time);
  if (!value) return '—';
  if (tickType === TickMarkType.Year) return value.year;
  if (tickType === TickMarkType.Month) return `${Number(value.month)}月`;
  if (tickType === TickMarkType.DayOfMonth || !value.hour)
    return `${value.month}/${value.day}`;
  const clock = `${value.hour}:${value.minute}`;
  return tickType === TickMarkType.TimeWithSeconds
    ? `${clock}:${value.second}`
    : clock;
};

export const chartCrosshairFormatter: TimeFormatterFn = (time) => {
  const value = parts(time);
  if (!value) return '—';
  const date = `${value.year}-${value.month}-${value.day}`;
  return `${date}${value.hour ? ` ${value.hour}:${value.minute}:${value.second}` : ''} ${CHART_TIME_ZONE_LABEL}`;
};
