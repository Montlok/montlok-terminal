import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import Engine from '.';

const mocks = vi.hoisted(() => ({
  api: vi.fn(),
  push: vi.fn(),
}));

vi.mock('@umijs/max', () => ({
  history: { push: mocks.push },
  useLocation: () => ({ pathname: '/engine/run/overview' }),
  useModel: () => ({
    selectedGroup: 'live-sector-6040',
    selectedRuns: {},
    setSelectedGroup: vi.fn(),
    setSelectedRuns: vi.fn(),
  }),
}));
vi.mock('../../operator/api', async (original) => ({
  ...(await original<typeof import('../../operator/api')>()),
  api: mocks.api,
}));
vi.mock('../../operator/TimeSeriesChart', () => ({
  TimeSeriesChart: ({ label }: { label: string }) => (
    <div role="img" aria-label={label} />
  ),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

it('renders the selected live engine and uses the compact overview payload', async () => {
  mocks.api.mockImplementation((path: string) => {
    if (path === 'strategy-groups')
      return Promise.resolve({
        groups: [{ id: 'live-sector-6040', name: '四板块 60/40' }],
      });
    if (path === 'strategy-groups/live-sector-6040/runtime')
      return Promise.resolve({
        available: true,
        groups: [
          {
            groupId: 'live-sector-6040',
            name: '四板块 60/40',
            runId: 'live-run-1',
            runs: [
              {
                runId: 'live-run-1',
                status: 'running',
                engine: {
                  marketReady: true,
                  executionReady: true,
                  ordersEnabled: true,
                  uptimeSeconds: 3661,
                  ordersAccepted: 12,
                  fillsTotal: 3,
                  execution: {
                    cancel: 9,
                    openOrders: 2,
                    decisionCycles: 88,
                    quoteEvents: 144,
                    ackP50Ms: 31.5,
                    ackP95Ms: 54.25,
                    history: [
                      { time: 1788700000, orders: 1, cancels: 0, fills: 0 },
                      { time: 1788700001, orders: 2, cancels: 1, fills: 1 },
                    ],
                    instruments: [],
                  },
                },
              },
            ],
          },
        ],
      });
    if (path === 'strategy-groups/live-sector-6040?view=summary')
      return Promise.resolve({
        id: 'live-sector-6040',
        runId: 'live-run-1',
        status: 'running',
      });
    return Promise.reject(new Error(`Unexpected API call: ${path}`));
  });

  render(<Engine />);

  expect(await screen.findByText('live-run-1')).toBeInTheDocument();
  expect(screen.getByText('12')).toBeInTheDocument();
  expect(screen.getByText('31.5 ms')).toBeInTheDocument();
  expect(screen.getByText('行情 已连接')).toBeInTheDocument();
  expect(screen.getAllByRole('img', { name: /\/ 秒/ })).toHaveLength(3);
  expect(screen.queryByText(/本地模拟|模拟盘|影子/)).not.toBeInTheDocument();
  await waitFor(() =>
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/live-sector-6040?view=summary',
    ),
  );
});
