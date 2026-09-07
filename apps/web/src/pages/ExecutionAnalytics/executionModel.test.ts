import { describe, expect, it } from 'vitest';
import {
  type ExecutionDetail,
  executionAnalytics,
  executionTime,
  fillNotional,
} from './executionModel';

const start = 1788631200;
function detail(changes: Partial<ExecutionDetail> = {}): ExecutionDetail {
  return {
    id: 'baseline',
    runId: 'run-01',
    sources: { orders: true, fills: true },
    ordersTotal: 3,
    fillsTotal: 3,
    orders: [
      { status: 'FILLED' },
      { status: 'REJECTED' },
      { status: 'DENIED' },
    ],
    fills: [
      {
        instrument: 'BTC-USDT.OKX',
        quantity: '0.01',
        price: '80000',
        fee: '0.8',
        feeCurrency: 'USDT',
        time: start,
      },
      {
        instrument: 'ETH-USDT.OKX',
        quantity: '1',
        price: '2500',
        fee: '2.5',
        feeCurrency: 'USDT',
        time: (start + 20) * 1000,
      },
      {
        instrument: 'BTC-USDT.OKX',
        quantity: '0.02',
        price: '80000',
        fee: '-0.1',
        feeCurrency: 'USDT',
        time: new Date((start + 620) * 1000).toISOString(),
      },
    ],
    ...changes,
  };
}

describe('run-scoped execution analytics', () => {
  it('buckets actual spot fill notionals and preserves commission/rebate signs', () => {
    const result = executionAnalytics(detail());
    expect(result.notional).toBe(4900);
    expect(result.volume).toEqual([
      { time: start, value: 3300 },
      { time: start + 600, value: 1600 },
    ]);
    expect(result.usdtFee).toBeCloseTo(3.2);
    expect(result.feesByInstrument).toHaveLength(2);
    expect(
      result.feesByInstrument.find((item) => item.label === 'BTC-USDT.OKX')
        ?.value,
    ).toBeCloseTo(0.7);
    expect(result.rejected).toBe(2);
    expect(result.ordersTotal).toBe(3);
    expect(result.fillsPartial).toBe(false);
  });

  it('does not turn missing source metadata or missing total counts into zero', () => {
    for (const sources of [undefined, {}, { orders: false, fills: false }]) {
      const result = executionAnalytics(
        detail({
          sources,
          orders: [],
          fills: [],
          ordersTotal: undefined,
          fillsTotal: undefined,
        }),
      );
      expect(result.ordersTotal).toBeNull();
      expect(result.fillsTotal).toBeNull();
      expect(result.rejected).toBeNull();
      expect(result.notional).toBeNull();
      expect(result.usdtFee).toBeNull();
    }
  });

  it('reports zero only for a confirmed empty instance', () => {
    const result = executionAnalytics(
      detail({ ordersTotal: 0, fillsTotal: 0, orders: [], fills: [] }),
    );
    expect(result.ordersTotal).toBe(0);
    expect(result.fillsTotal).toBe(0);
    expect(result.rejected).toBe(0);
    expect(result.notional).toBe(0);
    expect(result.usdtFee).toBe(0);
  });

  it('never treats swap contract quantity as a base-asset amount', () => {
    const swap = {
      instrument: 'BTC-USDT-SWAP.OKX',
      quantity: 10,
      price: 80000,
    };
    expect(fillNotional(swap)).toBeUndefined();
    expect(fillNotional({ ...swap, notional: 8000 })).toBe(8000);
    expect(
      fillNotional({ instrument: 'BTC-USD.OKX', quantity: 1, price: 80000 }),
    ).toBeUndefined();
    expect(
      fillNotional({
        instrument: 'BTC-USDT.OKX',
        quantity: true,
        price: 80000,
      }),
    ).toBeUndefined();
  });

  it('does not mix non-USDT or unknown-currency fees into USDT totals', () => {
    const result = executionAnalytics(
      detail({
        fills: [
          { instrument: 'BTC-USDT.OKX', fee: 0.01, feeCurrency: 'BTC' },
          { instrument: 'ETH-USDT.OKX', fee: 10 },
          { instrument: 'SOL-USDT.OKX', fee: null },
        ],
      }),
    );
    expect(result.usdtFee).toBeNull();
    expect(result.feesOtherCurrency).toBe(1);
    expect(result.feesWithoutCurrency).toBe(1);
    expect(result.unknownFee).toBe(1);
    expect(result.feesByInstrument).toEqual([]);
    expect(result.missingNotional).toBe(3);
  });

  it('labels partial windows and does not invent timestamps or empty bins', () => {
    const input = detail({ fillsTotal: 900, ordersTotal: 700 });
    input.fills = [
      ...(input.fills || []),
      { instrument: 'SOL-USDT.OKX', quantity: 1, price: 100, time: 'invalid' },
    ];
    const result = executionAnalytics(input);
    expect(result.fillsPartial).toBe(true);
    expect(result.ordersPartial).toBe(true);
    expect(result.amountWithoutTime).toBe(1);
    expect(result.volume).toHaveLength(2);
    expect(executionTime(null)).toBeUndefined();
    expect(executionTime(true)).toBeUndefined();
    expect(executionTime('invalid')).toBeUndefined();
    expect(executionTime('1788748954123456789')).toBe(1788748954);
    expect(executionTime('1788748954123456')).toBe(1788748954);
    expect(executionTime('1788748954123')).toBe(1788748954);
  });

  it('retains unknown order states without claiming zero rejects', () => {
    const result = executionAnalytics(
      detail({ orders: [{ status: '' }, { status: 'unexpected' }] }),
    );
    expect(result.statuses).toEqual([
      { label: '未知状态', value: 2, tone: 'warning' },
    ]);
    expect(result.rejected).toBeNull();
  });
});
