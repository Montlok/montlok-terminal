import { describe, expect, it } from 'vitest';
import { modelRuntimeCharts } from './modelRuntimeCharts';

describe('model inference observation charts', () => {
  it('converts observed returns and target fractions to separate bps and percent series', () => {
    const result = modelRuntimeCharts([
      {
        completedAtNs: '1788690000000000000',
        prediction: 0.002,
        targetFraction: 0.1,
      },
      {
        completedAtNs: '1788690060000000000',
        prediction: -0.001,
        targetFraction: 0,
      },
    ]);
    expect(result.predictions).toEqual([
      { time: 1788690000, value: 20 },
      { time: 1788690060, value: -10 },
    ]);
    expect(result.targets).toEqual([
      { time: 1788690000, value: 10 },
      { time: 1788690060, value: 0 },
    ]);
  });
  it('does not invent history, missing values or zeroes', () => {
    expect(modelRuntimeCharts()).toEqual({ predictions: [], targets: [] });
    const result = modelRuntimeCharts([
      { completedAtNs: '1788690000000000000', prediction: 0 },
      { completedAtNs: 0, prediction: 1, targetFraction: 0.1 },
    ]);
    expect(result.predictions).toHaveLength(1);
    expect(result.targets).toEqual([]);
  });
});
