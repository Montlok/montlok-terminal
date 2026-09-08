import { dataOf, type Row } from './api';

export type TimePoint = { time: number; value: number };
export type ResultChart =
  | { kind: 'candles'; label: string; candles: Row[]; showVolume: boolean }
  | { kind: 'depth'; label: string; book: Row }
  | { kind: 'series'; label: string; points: TimePoint[] };

function finite(value: unknown): number | undefined {
  if (value === '' || value === null || value === undefined) return undefined;
  const result = Number(value);
  return Number.isFinite(result) ? result : undefined;
}

export function chartTime(value: unknown): number | undefined {
  const numeric = finite(value);
  const seconds =
    numeric === undefined ? NaN : numeric > 1e12 ? numeric / 1000 : numeric;
  return seconds >= 946684800 && seconds < 7258118400
    ? Math.floor(seconds)
    : undefined;
}

/** The chart library requires unique ascending timestamps. Do not interpolate gaps. */
export function orderedPoints(points: TimePoint[]): TimePoint[] {
  if (
    points.every(
      (point, index) =>
        Number.isFinite(point.time) &&
        Number.isFinite(point.value) &&
        (index === 0 || point.time > points[index - 1].time),
    )
  )
    return points;
  return [
    ...new Map(
      points
        .filter(
          (point) =>
            Number.isFinite(point.time) && Number.isFinite(point.value),
        )
        .map((point) => [point.time, point]),
    ).values(),
  ].sort((a, b) => a.time - b.time);
}

/** Only the unchanged prefix permits incremental chart updates; corrections reset the series. */
export function appendedPoints(
  previous: TimePoint[],
  next: TimePoint[],
): TimePoint[] | undefined {
  if (!previous.length || next.length < previous.length) return undefined;
  for (let index = 0; index < previous.length - 1; index++)
    if (
      previous[index].time !== next[index].time ||
      previous[index].value !== next[index].value
    )
      return undefined;
  const last = previous.length - 1;
  if (previous[last].time !== next[last]?.time) return undefined;
  return next.slice(
    previous[last].value === next[last].value ? previous.length : last,
  );
}

function candleOf(row: unknown, priceOnly: boolean): Row | undefined {
  if (!row || typeof row !== 'object') return undefined;
  const record = row as Row;
  if (Array.isArray(row)) {
    // Index/mark-price responses end in confirm, not volume. Unknown six-field
    // arrays are ambiguous and must not acquire a manufactured volume series.
    if (
      priceOnly
        ? row.length !== 6 || !['0', '1'].includes(String(row[5]))
        : row.length < 7 || row.length > 9
    )
      return undefined;
  }
  const values = Array.isArray(row)
    ? row
    : [
        record.time ?? record.ts,
        record.open,
        record.high,
        record.low,
        record.close,
        record.volume,
      ];
  const time = chartTime(values[0]);
  const [open, high, low, close, volume] = values.slice(1, 6).map(finite);
  if (
    time === undefined ||
    open === undefined ||
    high === undefined ||
    low === undefined ||
    close === undefined ||
    (!priceOnly && (volume === undefined || volume < 0)) ||
    low <= 0 ||
    high < Math.max(open, close) ||
    low > Math.min(open, close)
  )
    return undefined;
  return { time, open, high, low, close, ...(!priceOnly ? { volume } : {}) };
}

function validLevels(value: unknown): value is unknown[][] {
  return (
    Array.isArray(value) &&
    value.every(
      (row) =>
        Array.isArray(row) &&
        row.length >= 2 &&
        (finite(row[0]) ?? 0) > 0 &&
        (finite(row[1]) ?? -1) >= 0,
    )
  );
}

/** Recognize explicit exchange schemas only; arbitrary numeric results remain tables. */
export function resultCharts(value: unknown, operation = ''): ResultChart[] {
  let data = dataOf(value);
  if (data?.candles) data = data.candles;
  const rows: unknown[] = Array.isArray(data)
    ? data
    : data && typeof data === 'object'
      ? [data]
      : [];
  if (!rows.length) return [];
  const first = rows[0] as Row;
  if (
    rows.length === 1 &&
    first &&
    validLevels(first.bids) &&
    validLevels(first.asks) &&
    (first.bids.length || first.asks.length)
  ) {
    return [
      {
        kind: 'depth',
        label: `${first.instId || ''} 累计深度`.trim(),
        book: {
          ...first,
          bids: [...first.bids].sort((a, b) => Number(b[0]) - Number(a[0])),
          asks: [...first.asks].sort((a, b) => Number(a[0]) - Number(b[0])),
        },
      },
    ];
  }
  const priceOnly = /(?:index|mark[-_]price)[-_]candles/.test(operation);
  const candles = rows.map((row) => candleOf(row, priceOnly));
  if (candles.every((row): row is Row => !!row)) {
    return [
      {
        kind: 'candles',
        label: priceOnly
          ? /index/.test(operation)
            ? '指数价格 K 线'
            : '标记价格 K 线'
          : 'K 线与成交量',
        showVolume: !priceOnly,
        candles: [
          ...new Map(candles.map((row) => [row.time, row])).values(),
        ].sort((a, b) => a.time - b.time),
      },
    ];
  }
  // Rubik historical open interest is a positional schema: [ts, oi, oiCcy, oiUsd].
  if (
    /contracts\/open-interest-history|market_get_oi_history/.test(operation) &&
    rows.every((row) => Array.isArray(row) && row.length === 4)
  ) {
    const points = rows.flatMap((row) => {
      const values = row as unknown[];
      const time = chartTime(values[0]);
      const value = finite(values[3]);
      return time === undefined || value === undefined ? [] : [{ time, value }];
    });
    return points.length
      ? [
          {
            kind: 'series',
            label: '持仓量 / USD',
            points: orderedPoints(points),
          },
        ]
      : [];
  }
  const charts: ResultChart[] = [];
  const metrics = [
    {
      key: 'realizedRate',
      label: '已结算资金费率 / %',
      time: 'fundingTime',
      scale: 100,
    },
    {
      key: 'fundingRate',
      label: '资金费率 / %',
      time: 'fundingTime',
      scale: 100,
    },
    { key: 'oiUsd', label: '持仓量 / USD', time: 'ts', scale: 1 },
    { key: 'oi', label: '持仓量 / 张', time: 'ts', scale: 1 },
  ];
  for (const metric of metrics) {
    const series = new Map<string, TimePoint[]>();
    for (const item of rows) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
      const row = item as Row;
      const time = chartTime(row[metric.time] ?? row.ts);
      const value = finite(row[metric.key]);
      if (time === undefined || value === undefined) continue;
      const instrument = String(row.instId || row.instFamily || '');
      const points = series.get(instrument) || [];
      points.push({ time, value: value * metric.scale });
      series.set(instrument, points);
    }
    for (const [instrument, points] of series) {
      charts.push({
        kind: 'series',
        label: `${instrument} ${metric.label}`.trim(),
        points: orderedPoints(points),
      });
    }
    // Prefer USD-valued OI when supplied, avoiding two charts of the same quantity.
    if (metric.key === 'oiUsd' && series.size) break;
  }
  return charts;
}
