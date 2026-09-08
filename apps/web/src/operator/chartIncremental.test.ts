import { describe, it, expect } from 'vitest';
import { appendedPoints, orderedPoints } from './resultChartModel';
describe('incremental chart updates', () => {
  const points = Array.from({ length: 10_000 }, (_, time) => ({
    time,
    value: time / 2,
  }));
  it('does not copy or sort an already ordered source', () =>
    expect(orderedPoints(points)).toBe(points));
  it('sends no redraw for the same observations', () =>
    expect(appendedPoints(points, [...points])).toEqual([]));
  it('updates only the changed last bar and new observations', () => {
    const next = [
      ...points.slice(0, -1),
      { time: 9999, value: 2 },
      { time: 10_000, value: 3 },
    ];
    expect(appendedPoints(points, next)).toEqual(next.slice(-2));
  });
  it('resets on historical corrections, time changes and shortened windows', () => {
    expect(
      appendedPoints(points, [{ time: 0, value: 9 }, ...points.slice(1)]),
    ).toBeUndefined();
    expect(appendedPoints(points, points.slice(1))).toBeUndefined();
    expect(
      appendedPoints(points, [
        ...points.slice(0, -1),
        { time: 20_000, value: 3 },
      ]),
    ).toBeUndefined();
  });
});
