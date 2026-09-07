import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TimeSeriesChart } from './TimeSeriesChart';

const calls = vi.hoisted(() => ({
  data: vi.fn(),
  fit: vi.fn(),
  remove: vi.fn(),
  options: vi.fn(),
  seriesOptions: vi.fn(),
}));
vi.mock('lightweight-charts', () => ({
  ColorType: { Solid: 'solid' },
  LineSeries: {},
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
      addSeries: () => ({
        setData: calls.data,
        applyOptions: calls.seriesOptions,
      }),
      timeScale: () => ({ fitContent: calls.fit }),
      remove: calls.remove,
    };
  },
}));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe('time series rendering', () => {
  it('bounds the future edge while keeping normal pan and scale interactions', () => {
    render(
      <TimeSeriesChart
        label="净值"
        points={[{ time: 1788600000, value: 2000 }]}
      />,
    );
    expect(calls.options).toHaveBeenCalledWith(
      expect.objectContaining({
        timeScale: expect.objectContaining({
          fixRightEdge: true,
          rightOffset: 0,
          minBarSpacing: 0.05,
          fixLeftEdge: false,
        }),
      }),
    );
    const options = calls.options.mock.calls[0][0];
    expect(options.handleScroll).not.toBe(false);
    expect(options.handleScale).not.toBe(false);
  });
  it('uses only observed points, orders timestamps and does not reset zoom on each tick', () => {
    const { rerender, unmount } = render(
      <TimeSeriesChart
        label="净值"
        points={[
          { time: 2, value: 4 },
          { time: 1, value: 3 },
        ]}
      />,
    );
    expect(calls.data).toHaveBeenLastCalledWith([
      { time: 1, value: 3 },
      { time: 2, value: 4 },
    ]);
    expect(calls.fit).toHaveBeenCalledTimes(1);
    rerender(
      <TimeSeriesChart
        label="净值"
        points={[
          { time: 1, value: 3 },
          { time: 2, value: 4 },
          { time: 3, value: 5 },
        ]}
      />,
    );
    expect(calls.fit).toHaveBeenCalledTimes(1);
    unmount();
    expect(calls.remove).toHaveBeenCalledTimes(1);
  });
  it('shows an explicit empty state rather than generating history', () => {
    render(<TimeSeriesChart label="净值" points={[]} />);
    expect(screen.getByText('等待首个时间序列采样点')).toBeInTheDocument();
    expect(calls.data).toHaveBeenLastCalledWith([]);
  });
});
