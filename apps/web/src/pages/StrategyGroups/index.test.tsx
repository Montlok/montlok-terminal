import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { useEffect, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import StrategyGroups from './index';

const mocks = vi.hoisted(() => ({
  selected: 'baseline',
  setSelected: vi.fn(),
  push: vi.fn(),
  api: vi.fn(),
  liveAccount: 'live-account-a',
}));
vi.mock('@umijs/max', () => ({
  history: { push: mocks.push },
  useModel: () => {
    const [selectedRuns, setSelectedRuns] = useState<Record<string, string>>(
      {},
    );
    return {
      selectedGroup: mocks.selected,
      setSelectedGroup: mocks.setSelected,
      selectedRuns,
      setSelectedRuns,
      account: { accountId: mocks.liveAccount, mode: 'live_readonly' },
    };
  },
}));
vi.mock('../../operator/api', () => ({
  api: mocks.api,
  number: (value: unknown) => String(value ?? '—'),
  timeOf: (value: unknown) => String(value ?? '—'),
}));
vi.mock('../../operator/usePoll', () => ({
  usePoll: (load: () => Promise<void>) =>
    useEffect(() => {
      void load();
    }, [load]),
}));
vi.mock('../../operator/TimeSeriesChart', () => ({
  TimeSeriesChart: ({ label }: { label: string }) => (
    <div role="img" aria-label={label} />
  ),
}));
vi.mock('../../operator/DataGrid', () => ({
  DataGrid: ({ rows }: { rows: unknown[] }) => (
    <pre>{JSON.stringify(rows)}</pre>
  ),
}));

const group = (id = 'baseline') => ({
  id,
  name: id === 'baseline' ? '四板块 60/40' : '四板块 Alpha 叠加',
  description: '板块趋势 · 逆波动 · 20 日动量',
  modeLabel: '本地模拟',
  accountId: 'PAPER-OKX-001',
  runId: 'run-24h-02',
  status: 'running',
  signalAsOf: '2026-09-03',
  signalVersion: 'c6df7357be002da20937233c001ac0f1',
  startedAt: Date.parse('2026-09-05T01:00:00Z') / 1000,
  scheduledStopAt: Date.parse('2026-09-06T01:00:00Z') / 1000,
  observedAt: Date.parse('2026-09-05T23:00:00Z') / 1000,
  elapsedSeconds: 79200,
  metrics: { nav: 2011 },
  capabilities: { start: false, reason: 'Existing local simulation' },
  positions: [{ instrument: 'XNVDA-USDT.OKX', quantity: 2 }],
  orders: [],
  fills: [],
  alpha: [],
  version: [],
  health: {},
});

beforeEach(() => {
  vi.clearAllMocks();
  mocks.selected = 'baseline';
  mocks.liveAccount = 'live-account-a';
  mocks.api.mockImplementation(async (path: string) => {
    const url = new URL(path, 'https://montlok.test');
    const groupId = url.pathname.split('/')[2];
    const selectedRun = url.searchParams.get('runId');
    if (path === 'strategy-groups')
      return { groups: [group(), group('enhanced')] };
    if (url.pathname.endsWith('/runtime'))
      return {
        groups: [
          {
            groupId,
            runs: [
              { groupId, runId: `${groupId}-runtime-1`, status: 'running' },
            ],
          },
        ],
      };
    if (url.pathname.endsWith('/equity'))
      return {
        groupId,
        runId: selectedRun || 'run-24h-02',
        points: [],
        drawdown: [],
        sampleCount: 0,
        sourceAvailable: true,
      };
    return {
      ...group(groupId),
      runId: selectedRun || 'run-24h-02',
      metrics: { nav: selectedRun ? 3015 : 2011 },
    };
  });
});
afterEach(cleanup);

describe('versioned strategy group view', () => {
  it('shows durable versions and observed times while retaining the independent paper account', async () => {
    const view = render(<StrategyGroups />);
    expect(await screen.findByText('c6df7357be00')).toBeInTheDocument();
    expect(screen.getByText('2026-09-03')).toBeInTheDocument();
    expect(screen.getByText('2026/09/05 09:00:00')).toBeInTheDocument();
    expect(screen.getByText('2026/09/06 07:00:00')).toBeInTheDocument();
    expect(screen.getByText('22h 0m')).toBeInTheDocument();
    expect(screen.getByText(/PAPER-OKX-001/)).toBeInTheDocument();
    expect(screen.queryByText(/昨日 Alpha/)).not.toBeInTheDocument();
    mocks.liveAccount = 'live-account-b';
    view.rerender(<StrategyGroups />);
    expect(screen.getByText(/PAPER-OKX-001/)).toBeInTheDocument();
    expect(screen.queryByText('live-account-b')).not.toBeInTheDocument();
  });

  it('uses the shared selected group, and routes upload to the resource library', async () => {
    render(<StrategyGroups />);
    await screen.findByText('c6df7357be00');
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '策略组' }));
    fireEvent.click(await screen.findByText('四板块 Alpha 叠加 · 运行中'));
    expect(mocks.setSelected).toHaveBeenCalledWith(
      'enhanced',
      expect.anything(),
    );
    fireEvent.click(screen.getByRole('button', { name: '模型与因子' }));
    expect(mocks.push).toHaveBeenCalledWith('/strategies/resources/artifacts');
  });

  it('keeps the original run by default and queries another run only after selection', async () => {
    render(<StrategyGroups />);
    expect(await screen.findByText('2011')).toBeInTheDocument();
    expect(
      mocks.api.mock.calls.some(([path]) => String(path).includes('?runId=')),
    ).toBe(false);
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '运行实例' }));
    fireEvent.click(await screen.findByText('baseline-runtime-1 · 运行中'));
    expect(screen.queryByText('2011')).not.toBeInTheDocument();
    expect(await screen.findByText('3015')).toBeInTheDocument();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline?runId=baseline-runtime-1',
    );
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline/equity?runId=baseline-runtime-1',
    );
    expect(screen.getByText(/PAPER-OKX-001/)).toBeInTheDocument();
  });
});
