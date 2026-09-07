import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { useEffect, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Portfolio from './index';

const mocks = vi.hoisted(() => ({
  selected: 'baseline',
  api: vi.fn(),
  push: vi.fn(),
  charts: vi.fn(),
  liveBalance: 9999999,
  mismatch: false,
}));
vi.mock('@umijs/max', () => ({
  history: { push: mocks.push },
  useModel: () => {
    const [selectedGroup, setSelectedGroup] = useState(mocks.selected);
    const [selectedRuns, setSelectedRuns] = useState<Record<string, string>>(
      {},
    );
    return {
      selectedGroup,
      setSelectedGroup,
      selectedRuns,
      setSelectedRuns,
      account: {
        mode: 'live_readonly',
        balances: [{ ccy: 'USDT', availBal: mocks.liveBalance }],
      },
    };
  },
}));
vi.mock('../../operator/api', () => ({
  api: mocks.api,
  number: (value: unknown) => String(value ?? '—'),
}));
vi.mock('../../operator/usePoll', () => ({
  usePoll: (task: () => Promise<void>) =>
    useEffect(() => {
      void task();
    }, [task]),
}));
vi.mock('../../operator/TimeSeriesChart', () => ({
  TimeSeriesChart: ({
    points,
    label,
  }: {
    points: unknown[];
    label: string;
  }) => {
    mocks.charts(label, points);
    return <div role="img" aria-label={label} />;
  },
}));
const group = (id: string, runId: string | null, nav: number | null) => ({
  id,
  name: id === 'baseline' ? '四板块 60/40' : '四板块 Alpha 叠加',
  runId,
  modeLabel: runId ? '本地模拟' : '未配置',
  accountId: runId ? 'PAPER-OKX-001' : null,
  status: runId ? 'running' : 'pending_validation',
  signalAsOf: '2026-09-03',
  observedAt: 1788600000,
  metrics: { nav, pnl: 3, returnPct: 0.15, maxDrawdownPct: -0.25, fees: 1.9 },
  capabilities: { reason: '组合验证尚未发布' },
  sources: { view: !!runId },
  health: { detailAvailable: !!runId },
  positions: runId
    ? [
        { instrument: 'XNVDA-USDT.OKX', notional: 300, unrealizedPnl: -2 },
        { instrument: 'XMU-USDT.OKX', notional: 150, unrealizedPnl: 5 },
      ]
    : [],
});

beforeEach(() => {
  vi.clearAllMocks();
  mocks.selected = 'baseline';
  mocks.mismatch = false;
  mocks.api.mockImplementation(async (path: string) => {
    const url = new URL(path, 'https://montlok.test');
    const id = url.pathname.split('/')[2];
    const runId =
      url.searchParams.get('runId') ||
      (id === 'baseline' ? 'run-24h-02' : null);
    if (path === 'strategy-groups')
      return {
        groups: [
          group('baseline', 'run-24h-02', 2003),
          group('enhanced', null, null),
        ],
      };
    if (url.pathname.endsWith('/runtime'))
      return {
        groups: [
          {
            groupId: id,
            runs: [{ groupId: id, runId: 'new-run-1', status: 'running' }],
          },
        ],
      };
    if (url.pathname.endsWith('/equity'))
      return {
        groupId: id,
        runId: mocks.mismatch ? 'wrong-run' : runId,
        points: runId
          ? [{ time: 1788600000, value: runId === 'new-run-1' ? 123 : 2003 }]
          : [],
        drawdown: runId ? [{ time: 1788600000, value: -0.25 }] : [],
        sourceAvailable: !!runId,
        sampleCount: runId ? 1 : 0,
      };
    return group(
      id,
      runId,
      runId ? (runId === 'new-run-1' ? 123 : 2003) : null,
    );
  });
});
afterEach(cleanup);

describe('portfolio workspace', () => {
  it('shows run-owned NAV and signed attribution without reading the live API balance', async () => {
    render(<Portfolio />);
    expect(await screen.findByText('2003')).toBeInTheDocument();
    expect(screen.getByText('450')).toBeInTheDocument();
    expect(screen.getByText('-2')).toBeInTheDocument();
    expect(screen.getByText('+5')).toBeInTheDocument();
    expect(screen.getAllByText('按品种')).toHaveLength(2);
    expect(
      screen.getByRole('region', { name: '组合运行来源' }),
    ).toHaveTextContent('run-24h-02');
    expect(
      screen.queryByText(String(mocks.liveBalance)),
    ).not.toBeInTheDocument();
    expect(
      mocks.api.mock.calls.every(([path]) =>
        String(path).startsWith('strategy-groups'),
      ),
    ).toBe(true);
    expect(mocks.charts).toHaveBeenCalledWith('组合净值', [
      { time: 1788600000, value: 2003 },
    ]);
  });
  it('changes to a selected new instance without carrying over the original run curve', async () => {
    render(<Portfolio />);
    await screen.findByText('2003');
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '组合运行实例' }));
    fireEvent.click(await screen.findByText('new-run-1 · 运行中'));
    expect(screen.queryByText('2003')).not.toBeInTheDocument();
    expect(await screen.findByText('123')).toBeInTheDocument();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline/equity?runId=new-run-1',
    );
    expect(mocks.charts).toHaveBeenCalledWith('组合净值', [
      { time: 1788600000, value: 123 },
    ]);
  });
  it('does not invent portfolio metrics or positions for an unpublished group', async () => {
    mocks.selected = 'enhanced';
    render(<Portfolio />);
    expect(await screen.findByText('组合配置检查中')).toBeInTheDocument();
    expect(screen.getAllByText('持仓数据正在对齐')).toHaveLength(2);
    expect(screen.queryByText('450')).not.toBeInTheDocument();
    expect(mocks.charts).toHaveBeenCalledWith('组合净值', []);
  });
  it('rejects mismatched history instead of plotting it as the selected portfolio', async () => {
    mocks.mismatch = true;
    render(<Portfolio />);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '运行实例数据不一致',
    );
    expect(mocks.charts).not.toHaveBeenCalled();
  });
});
