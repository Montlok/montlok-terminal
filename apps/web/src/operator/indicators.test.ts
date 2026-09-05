import { describe, expect, it } from 'vitest';
import { depthProfile, lastMovingAverage, movingAverage } from './indicators';

const candles = [1, 2, 3, 4, 5, 6].map((close, index) => ({
  time: index,
  close: String(close),
}));

describe('moving averages', () => {
  it('starts emitting once a full window is available', () => {
    expect(movingAverage(candles, 3)).toEqual([
      { time: 2, value: 2 },
      { time: 3, value: 3 },
      { time: 4, value: 4 },
      { time: 5, value: 5 },
    ]);
    expect(movingAverage(candles.slice(0, 2), 3)).toEqual([]);
  });
  it('computes the trailing point identically to the full series', () => {
    for (const period of [1, 3, 6]) {
      expect(lastMovingAverage(candles, period)).toEqual(
        movingAverage(candles, period).at(-1),
      );
    }
    expect(lastMovingAverage(candles, 7)).toBeUndefined();
  });
});

describe('depth profile', () => {
  it('accumulates each side outward from the spread', () => {
    const book = {
      bids: [
        ['100', '1'],
        ['99', '2'],
      ],
      asks: [
        ['101', '0.5'],
        ['102', '1.5'],
      ],
    };
    expect(depthProfile(book)).toEqual({
      bids: [
        { price: 100, cumulative: 1 },
        { price: 99, cumulative: 3 },
      ],
      asks: [
        { price: 101, cumulative: 0.5 },
        { price: 102, cumulative: 2 },
      ],
    });
  });
  it('tolerates a missing or partial book', () => {
    expect(depthProfile(undefined)).toEqual({ bids: [], asks: [] });
    expect(depthProfile({ bids: [['1', '1']] }).asks).toEqual([]);
  });
});
