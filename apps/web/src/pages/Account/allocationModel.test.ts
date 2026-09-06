import { describe, expect, it } from 'vitest';
import { assetAllocation } from './allocationModel';

describe('equity allocation', () => {
  it('uses eqUsd, not balance quantities, to calculate percentages', () => {
    const allocation = assetAllocation([
      { ccy: 'BTC', cashBal: '1', eqUsd: '80000' },
      { ccy: 'USDT', cashBal: '20000', eqUsd: '20000' },
    ]);
    expect(allocation.positive).toBe(100000);
    expect(allocation.slices[0]).toEqual({
      currency: 'BTC',
      value: 80000,
      share: 0.8,
    });
  });
  it('reports negative and missing values separately, without inventing valuations', () => {
    const result = assetAllocation([
      { ccy: 'BTC', eqUsd: '50' },
      { ccy: 'USDT', eqUsd: '-10' },
      { ccy: 'X', cashBal: '500' },
      { ccy: 'Y', eqUsd: '' },
      { ccy: 'Z', eqUsd: 'NaN' },
    ]);
    expect(result).toMatchObject({ positive: 50, negative: -10, missing: 3 });
    expect(result.slices).toHaveLength(1);
  });
  it('handles zero portfolios and aggregates a repeated asset', () => {
    expect(assetAllocation([{ ccy: 'USDT', eqUsd: '0' }]).slices).toEqual([]);
    expect(
      assetAllocation([
        { ccy: 'BTC', eqUsd: '1' },
        { ccy: 'BTC', eqUsd: '2' },
      ]).slices,
    ).toEqual([{ currency: 'BTC', value: 3, share: 1 }]);
  });
});
