import type { Row } from './api';

export type SeriesPoint = { time: number; value: number };

/** Simple moving average of `close`; one point per bar from index `period - 1`. */
export function movingAverage(candles: Row[], period: number): SeriesPoint[] {
  const points: SeriesPoint[] = [];
  let sum = 0;
  for (let index = 0; index < candles.length; index++) {
    sum += Number(candles[index].close);
    if (index >= period) sum -= Number(candles[index - period].close);
    if (index + 1 >= period)
      points.push({ time: candles[index].time, value: sum / period });
  }
  return points;
}

/** Latest moving-average point, reading only the trailing window. */
export function lastMovingAverage(
  candles: Row[],
  period: number,
): SeriesPoint | undefined {
  if (candles.length < period) return undefined;
  let sum = 0;
  for (let index = candles.length - period; index < candles.length; index++)
    sum += Number(candles[index].close);
  return { time: candles[candles.length - 1].time, value: sum / period };
}

export type DepthLevel = { price: number; cumulative: number };
export type DepthProfile = { bids: DepthLevel[]; asks: DepthLevel[] };

/** Cumulative size per side, ordered outward from the spread as OKX delivers levels. */
export function depthProfile(book: Row | undefined): DepthProfile {
  const cumulate = (levels: string[][] | undefined) => {
    let cumulative = 0;
    return (levels || []).map((row) => {
      cumulative += Number(row[1]);
      return { price: Number(row[0]), cumulative };
    });
  };
  return { bids: cumulate(book?.bids), asks: cumulate(book?.asks) };
}
