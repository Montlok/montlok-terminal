import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MarketChart } from './MarketChart';

const calls = vi.hoisted(() => ({
  add: vi.fn(),
  data: vi.fn(),
  remove: vi.fn(),
  options: vi.fn(),
}));
vi.mock('lightweight-charts', () => ({
  ColorType: { Solid: 'solid' },
  CandlestickSeries: 'candles',
  HistogramSeries: 'volume',
  LineSeries: 'line',
  TickMarkType: {
    Year: 0,
    Month: 1,
    DayOfMonth: 2,
    Time: 3,
    TimeWithSeconds: 4,
  },
  createChart: (_container: HTMLElement, options: unknown) => {
    calls.options(options);
    return {
      addSeries: (type: string) => {
        calls.add(type);
        return { setData: calls.data, update: vi.fn(), applyOptions: vi.fn() };
      },
      priceScale: () => ({ applyOptions: vi.fn() }),
      timeScale: () => ({ fitContent: vi.fn() }),
      remove: calls.remove,
    };
  },
}));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});
const price = { time: 1788600000, open: 100, high: 105, low: 98, close: 103 };
describe('price-only market candles', () => {
  it('bounds future scrolling at the newest candle without disabling historical pan or zoom', () => {
    render(<MarketChart candles={[{ ...price, volume: 40 }]} />);
    expect(calls.options).toHaveBeenCalledWith(
      expect.objectContaining({
        timeScale: expect.objectContaining({
          fixRightEdge: true,
          rightOffset: 0,
          fixLeftEdge: false,
        }),
      }),
    );
    const options = calls.options.mock.calls[0][0];
    expect(options.handleScroll).not.toBe(false);
    expect(options.handleScale).not.toBe(false);
  });
  it('does not create a volume series for index/mark candles', () => {
    render(<MarketChart candles={[price]} showVolume={false} />);
    expect(calls.add).not.toHaveBeenCalledWith('volume');
    expect(
      screen.getByRole('img', { name: '价格 K 线图 · UTC+8' }),
    ).toBeInTheDocument();
    expect(calls.data).toHaveBeenCalledWith([price]);
  });
  it('keeps real OHLCV unchanged and removes the histogram when switching schema', () => {
    const { rerender } = render(
      <MarketChart candles={[{ ...price, volume: 40 }]} />,
    );
    expect(calls.add).toHaveBeenCalledWith('volume');
    expect(calls.data).toHaveBeenCalledWith([
      expect.objectContaining({ time: price.time, value: 40 }),
    ]);
    calls.add.mockClear();
    rerender(<MarketChart candles={[price]} showVolume={false} />);
    expect(calls.remove).toHaveBeenCalledTimes(1);
    expect(calls.add).not.toHaveBeenCalledWith('volume');
  });
});
