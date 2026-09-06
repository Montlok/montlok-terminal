import { describe, expect, it } from 'vitest';
import { chartTime, orderedPoints, resultCharts } from './resultChartModel';

const T = 1788600000000;
const candle = (time: number) => [
  String(time),
  '100',
  '105',
  '98',
  '103',
  '40',
  '4120',
  '4120',
  '1',
];

describe('schema-aware charts', () => {
  it('unwraps MCP results, sorts candle timestamps and keeps one bar per timestamp', () => {
    const data = [candle(T + 60_000), candle(T), candle(T)];
    const charts = resultCharts({
      result: { content: [{ type: 'text', text: JSON.stringify({ data }) }] },
    });
    expect(charts).toHaveLength(1);
    expect(charts[0]).toMatchObject({
      kind: 'candles',
      candles: [{ time: T / 1000 }, { time: T / 1000 + 60 }],
    });
  });
  it('does not infer candles from arbitrary arrays or malformed OHLC', () => {
    expect(resultCharts([[1, 2, 3, 4, 5, 6]])).toEqual([]);
    expect(resultCharts([[T, 100, 90, 80, 110, 5]])).toEqual([]);
    expect(resultCharts([null, {}])).toEqual([]);
    expect(resultCharts({ data: [] })).toEqual([]);
    expect(resultCharts([{ eqUsd: '100', ts: T }])).toEqual([]);
  });
  it.each([
    'GET /api/v5/market/index-candles',
    'GET /api/v5/market/history-index-candles',
    'GET /api/v5/market/mark-price-candles',
    'GET /api/v5/market/history-mark-price-candles',
    'market_get_index_candles',
  ])('treats %s confirm fields as price-only, never as volume', (operation) => {
    const charts = resultCharts(
      [
        [T, '100', '105', '98', '103', '1'],
        [T + 60_000, '103', '106', '100', '104', '0'],
      ],
      operation,
    );
    expect(charts).toHaveLength(1);
    const chart = charts[0];
    if (chart.kind !== 'candles') throw new Error('Expected price chart');
    expect(chart.showVolume).toBe(false);
    expect(chart.label).not.toContain('成交量');
    expect(chart.candles.every((row) => !('volume' in row))).toBe(true);
  });
  it('rejects ambiguous six-field arrays and malformed confirm while retaining real volume', () => {
    const ambiguous = [[T, '100', '105', '98', '103', '1']];
    expect(resultCharts(ambiguous)).toEqual([]);
    expect(resultCharts(ambiguous, 'GET /api/v5/market/candles')).toEqual([]);
    expect(
      resultCharts(
        [[T, '100', '105', '98', '103', '40']],
        'market_get_index_candles',
      ),
    ).toEqual([]);
    expect(
      resultCharts([candle(T)], 'GET /api/v5/market/candles')[0],
    ).toMatchObject({
      kind: 'candles',
      showVolume: true,
      candles: [{ volume: 40 }],
    });
  });
  it('normalizes book sorting without mutating exchange results', () => {
    const book = {
      bids: [
        ['99', '1'],
        ['100', '2'],
      ],
      asks: [
        ['102', '3'],
        ['101', '4'],
      ],
    };
    expect(resultCharts({ data: [book] })[0]).toMatchObject({
      kind: 'depth',
      book: {
        bids: [
          ['100', '2'],
          ['99', '1'],
        ],
        asks: [
          ['101', '4'],
          ['102', '3'],
        ],
      },
    });
    expect(book.bids[0][0]).toBe('99');
    expect(resultCharts({ bids: [['bad', '2']], asks: [] })).toEqual([]);
  });
  it('funding rates preserve negative and zero observations and separate instruments', () => {
    const charts = resultCharts([
      { instId: 'BTC-USDT-SWAP', fundingTime: T, fundingRate: '-0.001' },
      { instId: 'BTC-USDT-SWAP', fundingTime: T + 1000, fundingRate: '0' },
      { instId: 'ETH-USDT-SWAP', fundingTime: T, fundingRate: '0.002' },
    ]);
    expect(charts).toHaveLength(2);
    expect(charts[0]).toMatchObject({
      kind: 'series',
      points: [
        { time: T / 1000, value: -0.1 },
        { time: T / 1000 + 1, value: 0 },
      ],
    });
  });
  it('uses timestamped USD OI and requires schema context for positional arrays', () => {
    expect(
      resultCharts([
        { instId: 'BTC-USDT-SWAP', oiUsd: '1234', oi: '2', ts: T },
      ]),
    ).toEqual([
      {
        kind: 'series',
        label: 'BTC-USDT-SWAP 持仓量 / USD',
        points: [{ time: T / 1000, value: 1234 }],
      },
    ]);
    expect(resultCharts([[T, '2', '3', '1234']])).toEqual([]);
    expect(
      resultCharts(
        [[T, '2', '3', '1234']],
        'GET /api/v5/rubik/stat/option/open-interest-volume-expiry',
      ),
    ).toEqual([]);
    expect(
      resultCharts([[T, '2', '3', '1234']], 'market_get_oi_history')[0],
    ).toMatchObject({
      label: '持仓量 / USD',
      points: [{ time: T / 1000, value: 1234 }],
    });
    expect(resultCharts([{ oiUsd: '1234' }])).toEqual([]);
  });
  it('normalizes timestamps and filters non-finite values without filling missing time', () => {
    expect(chartTime(T)).toBe(T / 1000);
    expect(chartTime(T / 1000)).toBe(T / 1000);
    expect(chartTime('')).toBeUndefined();
    expect(
      orderedPoints([
        { time: 2, value: 4 },
        { time: 1, value: 0 },
        { time: 2, value: 6 },
        { time: 3, value: NaN },
      ]),
    ).toEqual([
      { time: 1, value: 0 },
      { time: 2, value: 6 },
    ]);
  });
});
