import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ExecutionAnalytics from '.';
import type { ExecutionDetail } from './executionModel';

const mocks = vi.hoisted(() => ({
  api: vi.fn(),
  selectedGroup: 'baseline',
  selectedRuns: { baseline: 'run-first', enhanced: 'run-second' } as Record<
    string,
    string
  >,
}));
vi.mock('@umijs/max', () => ({ useModel: () => mocks }));
vi.mock('../../operator/api', () => ({
  api: mocks.api,
  number: (value: unknown) =>
    value === null || value === undefined ? '—' : String(value),
  timeOf: (value: unknown) =>
    value === null || value === undefined ? '—' : String(value),
}));
vi.mock('../../operator/TimeSeriesChart', () => ({
  TimeSeriesChart: ({
    label,
    points,
  }: {
    label: string;
    points: unknown[];
  }) => (
    <div role="img" aria-label={label}>
      {points.length} buckets
    </div>
  ),
}));
vi.mock('../../operator/DataGrid', () => ({
  DataGrid: ({ rows }: { rows: Record<string, unknown>[] }) => (
    <section aria-label="成交记录">
      {rows.map((row, index) => (
        <span key={String(row.id || index)}>{String(row.instrument)}</span>
      ))}
    </section>
  ),
}));

function result(id: string, runId: string, name: string): ExecutionDetail {
  return {
    id,
    runId,
    name,
    modeLabel: '本地模拟',
    sources: { fills: true, orders: true },
    ordersTotal: 0,
    fillsTotal: 0,
    orders: [],
    fills: [],
    health: {},
    systems: [],
  };
}

describe('execution analytics page', () => {
  it('labels nonempty legacy records as read records and uses the explicit USDT run fee report without inventing per-fill currency', async () => {
    mocks.api.mockResolvedValue({
      id: 'baseline',
      runId: 'run-first',
      name: '历史运行',
      metrics: { fees: 1.25 },
      orders: [{ status: 'FILLED' }],
      fills: [
        {
          instrument: 'BTC-USDT.OKX',
          price: 100,
          quantity: 1,
          fee: 1.25,
          time: '2026-09-05T01:00:00Z',
        },
      ],
    });
    render(<ExecutionAnalytics />);
    await screen.findByText('历史运行');
    expect(screen.getByText('已读取成交')).toBeInTheDocument();
    expect(screen.getByText('运行报表手续费 / USDT')).toBeInTheDocument();
    expect(
      screen.getByText('USDT 手续费记录将随成交回报更新'),
    ).toBeInTheDocument();
  });
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.selectedGroup = 'baseline';
    mocks.selectedRuns = { baseline: 'run-first', enhanced: 'run-second' };
  });
  afterEach(cleanup);

  it('reads the shared selected run, shows actual scope and never posts an operation', async () => {
    mocks.api.mockResolvedValue(
      result('baseline', 'run-first', '四板块测试组'),
    );
    render(<ExecutionAnalytics />);
    await screen.findByText('四板块测试组');
    expect(mocks.api).toHaveBeenCalledExactlyOnceWith(
      'strategy-groups/baseline?runId=run-first',
    );
    expect(screen.getByText('实例 run-first')).toBeInTheDocument();
    expect(screen.getAllByText('未采集')).toHaveLength(3);
    expect(screen.getByRole('img', { name: '执行成交额' })).toHaveTextContent(
      '0 buckets',
    );
  });

  it('reports missing source and totals as unknown, not as a successful empty account', async () => {
    mocks.api.mockResolvedValue({
      id: 'baseline',
      runId: 'run-first',
      name: '缺失数据组',
      orders: [],
      fills: [],
    });
    render(<ExecutionAnalytics />);
    await screen.findByText('缺失数据组');
    expect(screen.getByText('数据来源未确认')).toBeInTheDocument();
    expect(screen.getByText('订单状态核对中')).toBeInTheDocument();
    expect(screen.getByText('委托总数').nextElementSibling).toHaveTextContent(
      '未知',
    );
    expect(screen.getByText('成交总数').nextElementSibling).toHaveTextContent(
      '未知',
    );
    expect(screen.getByText('记录内拒单').nextElementSibling).toHaveTextContent(
      '未知',
    );
  });

  it('hides the old group immediately and ignores its delayed response', async () => {
    let complete: ((value: ExecutionDetail) => void) | undefined;
    mocks.api.mockImplementation((path: string) =>
      path.includes('baseline')
        ? new Promise((resolve) => {
            complete = resolve;
          })
        : Promise.resolve(result('enhanced', 'run-second', '当前增强组')),
    );
    const view = render(<ExecutionAnalytics />);
    mocks.selectedGroup = 'enhanced';
    view.rerender(<ExecutionAnalytics />);
    await screen.findByText('当前增强组');
    await act(async () =>
      complete?.(result('baseline', 'run-first', '迟到基础组')),
    );
    expect(screen.queryByText('迟到基础组')).not.toBeInTheDocument();
    expect(screen.getByText('当前增强组')).toBeInTheDocument();
    expect(screen.getByText('实例 run-second')).toBeInTheDocument();
  });

  it('clears displayed data when the selected run changes within the same group', async () => {
    mocks.api.mockResolvedValue(result('baseline', 'run-first', '旧运行数据'));
    const view = render(<ExecutionAnalytics />);
    await screen.findByText('旧运行数据');
    mocks.api.mockImplementation(() => new Promise(() => {}));
    mocks.selectedRuns = { ...mocks.selectedRuns, baseline: 'run-third' };
    view.rerender(<ExecutionAnalytics />);
    expect(screen.queryByText('旧运行数据')).not.toBeInTheDocument();
    expect(screen.getByText('实例 run-third')).toBeInTheDocument();
    await waitFor(() =>
      expect(mocks.api).toHaveBeenLastCalledWith(
        'strategy-groups/baseline?runId=run-third',
      ),
    );
    expect(screen.getByText('成交总数').nextElementSibling).toHaveTextContent(
      '未知',
    );
  });
});
