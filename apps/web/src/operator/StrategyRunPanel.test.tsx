import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { type ReactNode, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { StrategyRunPanel } from './StrategyRunPanel';

type Ticket = {
  id: string;
  operation?: {
    name?: string;
    arguments?: { groupId?: string; runId?: string };
  };
};

const mocks = vi.hoisted(() => ({
  api: vi.fn(),
  role: 'admin',
  start: true,
  available: true,
  baselineRunning: false,
}));

vi.mock('@umijs/max', () => ({
  useModel: function useTestModel(key: string) {
    const [selectedGroup, setSelectedGroup] = useState('baseline');
    const [selectedRuns, setSelectedRuns] = useState<Record<string, string>>(
      {},
    );
    if (key === '@@initialState')
      return { initialState: { currentUser: { access: mocks.role } } };
    return { selectedGroup, setSelectedGroup, selectedRuns, setSelectedRuns };
  },
  Link: ({ to, children }: { to: string; children: ReactNode }) => (
    <a href={to}>{children}</a>
  ),
}));
vi.mock('./api', () => ({
  api: mocks.api,
  number: (value: unknown) => String(value),
  timeOf: (value: unknown) => String(value),
}));
vi.mock('./ConfirmOperation', () => ({
  ConfirmOperation: ({
    ticket,
    onClose,
    onComplete,
  }: {
    ticket?: Ticket;
    onClose: () => void;
    onComplete: (result: Record<string, unknown>) => void;
  }) =>
    ticket ? (
      <div role="dialog">
        确认启动 {ticket.operation?.arguments?.groupId}
        <span>{ticket.operation?.arguments?.runId}</span>
        <button type="button" onClick={onClose}>
          取消确认
        </button>
        <button
          type="button"
          onClick={() =>
            onComplete({
              id: ticket.id,
              status: ticket.id === 'ticket-unknown' ? 'unknown' : 'completed',
              result: {
                runId: ticket.operation?.arguments?.runId,
                status: ticket.id === 'ticket-unknown' ? 'unknown' : 'stopping',
                ...(ticket.id === 'ticket-unknown'
                  ? { receiptStatus: 'unknown', error: '响应超时' }
                  : {}),
              },
            })
          }
        >
          提交测试回执
        </button>
      </div>
    ) : null,
}));

function runtime(groupId: string) {
  return {
    available: mocks.available,
    groups: [
      {
        groupId,
        runId:
          groupId === 'baseline' && mocks.baselineRunning
            ? 'baseline-run-old'
            : null,
        name: `${groupId} signal`,
        defaultBudgetUsdt: '2000',
        maxBudgetUsdt: '2000',
        maxDurationSeconds: 86400,
        capabilities: {
          start: mocks.start,
          halt: mocks.baselineRunning,
          reduce: mocks.baselineRunning,
          resume: false,
          stop: mocks.baselineRunning,
        },
        runs:
          groupId === 'baseline' && mocks.baselineRunning
            ? [
                {
                  runId: 'another-managed-run',
                  status: 'completed',
                  startedAt: 1230,
                },
                {
                  runId: 'baseline-run-old',
                  status: 'running',
                  startedAt: 1234,
                },
              ]
            : [],
      },
    ],
  };
}

function defaultAPI(path: string, input?: unknown) {
  if (path === 'strategy-groups')
    return Promise.resolve({
      groups: [
        { id: 'baseline', name: '基础组', runId: 'legacy-history' },
        { id: 'enhanced', name: '增强组' },
      ],
    });
  if (path.endsWith('/runtime'))
    return Promise.resolve(runtime(path.split('/')[1]));
  if (path.endsWith('/preflight')) return Promise.resolve({ ok: true });
  if (path === 'prepare')
    return Promise.resolve({ id: 'ticket-local', operation: input });
  return Promise.reject(new Error(`Unexpected API call: ${path}`));
}

async function ready() {
  await waitFor(() =>
    expect(screen.getByRole('button', { name: /检查配置/ })).toBeEnabled(),
  );
}

async function preflight() {
  fireEvent.click(screen.getByRole('button', { name: /检查配置/ }));
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '开始运行' })).toBeEnabled(),
  );
}

async function selectEnhanced() {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: '运行策略组' }));
  fireEvent.click(await screen.findByText('增强组'));
  await waitFor(() =>
    expect(mocks.api).toHaveBeenCalledWith('strategy-groups/enhanced/runtime'),
  );
}

async function selectRun(label: string) {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: '控制运行实例' }));
  fireEvent.click(await screen.findByText(label));
}

describe('strategy group run controls', () => {
  it('offers a restart action and the recorded cause after a failed live run', async () => {
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path === 'strategy-groups')
        return Promise.resolve({
          groups: [
            {
              id: 'baseline',
              name: '四板块 60/40 · 实盘',
              managed: true,
              runId: 'failed-run',
            },
          ],
        });
      if (path === 'strategy-groups/baseline/runtime')
        return Promise.resolve({
          available: true,
          groups: [
            {
              groupId: 'baseline',
              kind: 'live',
              mode: 'live',
              name: '四板块 60/40 · 实盘',
              runId: null,
              maxDurationSeconds: null,
              durationPolicy: { continuous: true, maxSeconds: null },
              capabilities: { start: true, stop: false },
              runs: [
                {
                  runId: 'failed-run',
                  status: 'failed',
                  error: 'exec-events receiver closed',
                },
              ],
            },
          ],
        });
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    expect(await screen.findByText('策略运行已停止')).toBeInTheDocument();
    expect(screen.getByText('exec-events receiver closed')).toBeInTheDocument();
    const restart = screen.getByRole('button', { name: '开始运行' });
    expect(restart).toHaveTextContent('重新启动实盘策略');
    expect(restart).toBeEnabled();
  });
  it('offers continuous operation and a chosen duration beyond one day without changing trading mode', async () => {
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path.endsWith('/runtime')) {
        const result = runtime(path.split('/')[1]);
        return Promise.resolve({
          ...result,
          groups: result.groups.map((group) => ({
            ...group,
            kind: 'live',
            mode: 'live',
            maxDurationSeconds: null,
            durationPolicy: { continuous: true, maxSeconds: null },
            executionPolicy: {
              kind: 'initial_allocation',
              label: '初始调仓',
              description: '按已发布信号配置目标仓位，完成后跟踪持仓与收益。',
            },
          })),
        });
      }
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    await ready();
    expect(screen.getByLabelText('策略执行方式')).toHaveTextContent('初始调仓');
    expect(screen.queryByLabelText('运行时长 分钟')).not.toBeInTheDocument();
    await preflight();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline/preflight',
      { durationSeconds: 0 },
    );
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '运行方式' }));
    fireEvent.click(await screen.findByText('定时结束'));
    const duration = screen.getByLabelText('运行时长 分钟');
    expect(duration).not.toHaveAttribute('aria-valuemax');
    fireEvent.change(duration, { target: { value: '10080' } });
    await preflight();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline/preflight',
      { durationSeconds: 604800 },
    );
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });

  it('keeps run settings editable while an existing instance prevents another start', async () => {
    mocks.baselineRunning = true;
    mocks.start = false;
    render(<StrategyRunPanel />);
    await screen.findByText('baseline signal');
    expect(screen.getByLabelText('运行时长 分钟')).toBeEnabled();
    expect(screen.getByRole('button', { name: '开始运行' })).toBeDisabled();
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });

  it('shows a fixed live exposure cap and omits virtual budget', async () => {
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path === 'strategy-groups')
        return Promise.resolve({
          groups: [
            { id: 'baseline', name: '基础组' },
            { id: 'live-hft-inventory', name: '实盘高频' },
          ],
        });
      if (path === 'strategy-groups/live-hft-inventory/runtime')
        return Promise.resolve({
          available: true,
          groups: [
            {
              groupId: 'live-hft-inventory',
              kind: 'live',
              mode: 'live',
              name: '实盘高频',
              exposureCapUsdt: '1',
              maxDurationSeconds: 86400,
              capabilities: { start: true, stop: false },
              runs: [],
            },
          ],
        });
      if (path === 'strategy-groups/live-hft-inventory/preflight')
        return Promise.resolve({
          ok: true,
          liveInventory: { totalEqUsd: '417.5', pairs: [{}, {}, {}, {}] },
        });
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    await ready();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '运行策略组' }));
    fireEvent.click(await screen.findByText('实盘高频'));
    await waitFor(() =>
      expect(screen.getByLabelText('策略执行环境')).toHaveTextContent(
        'OKX 实盘',
      ),
    );
    expect(screen.queryByLabelText('运行预算 USDT')).not.toBeInTheDocument();
    await preflight();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/live-hft-inventory/preflight',
      { durationSeconds: 3600 },
    );
    expect(screen.getAllByText(/新增净敞口 ≤ 1 USDT/).length).toBeGreaterThan(
      0,
    );
    expect(screen.getByText(/4 个交易对 · 60 分钟/)).toBeInTheDocument();
  });
  it('reads actual model telemetry nested in engine and identifies shadow mode', async () => {
    mocks.baselineRunning = true;
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path.endsWith('/runtime')) {
        const value = runtime('baseline');
        return Promise.resolve({
          ...value,
          groups: value.groups.map((group) => ({
            ...group,
            mode: 'shadow',
            runs: group.runs.map((run) => ({
              ...run,
              model: { modelVersion: 'legacy-stale' },
              engine: {
                model: { modelVersion: 'actual-browser-v1', device: 'cpu' },
              },
            })),
          })),
        });
      }
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    await ready();
    await selectRun('baseline-run-old · 运行中');
    expect(screen.getAllByText('研究运行').length).toBeGreaterThan(0);
    expect(screen.getByText('actual-browser-v1')).toBeInTheDocument();
    expect(screen.queryByText('legacy-stale')).not.toBeInTheDocument();
  });
  it('blocks new runs for a mismatched node without blocking stop of an existing run', async () => {
    mocks.baselineRunning = true;
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path.endsWith('/runtime')) {
        const value = runtime('baseline');
        return Promise.resolve({
          ...value,
          groups: value.groups.map((group) => ({
            ...group,
            nodeCompatibility: {
              ready: false,
              reason: '当前节点需要 CUDA 资源',
            },
          })),
        });
      }
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    await selectRun('baseline-run-old · 运行中');
    expect(screen.getByRole('button', { name: '开始运行' })).toBeDisabled();
    expect(
      screen.getByRole('button', { name: '检查配置' }),
    ).toHaveAccessibleDescription('当前节点需要 CUDA 资源');
    expect(screen.getByRole('button', { name: '停止运行' })).toBeEnabled();
  });
  it('queries an uncertain control receipt read-only and reports failure by the requested action', async () => {
    mocks.baselineRunning = true;
    let resolveReceipt: ((value: unknown) => void) | undefined;
    mocks.api.mockImplementation((path: string, body?: unknown) =>
      path === 'prepare'
        ? Promise.resolve({ id: 'ticket-unknown', operation: body })
        : path === 'operations/ticket-unknown'
          ? new Promise((resolve) => {
              resolveReceipt = resolve;
            })
          : defaultAPI(path, body),
    );
    render(<StrategyRunPanel />);
    await ready();
    await selectRun('baseline-run-old · 运行中');
    fireEvent.click(screen.getByRole('button', { name: '暂停开仓' }));
    await screen.findByRole('dialog');
    fireEvent.click(screen.getByRole('button', { name: '提交测试回执' }));
    expect(
      await screen.findByText('暂停开仓 · 正在核对结果'),
    ).toBeInTheDocument();
    expect(screen.queryByText('暂停开仓失败')).not.toBeInTheDocument();
    await waitFor(() =>
      expect(mocks.api).toHaveBeenCalledWith('operations/ticket-unknown'),
    );
    await act(async () =>
      resolveReceipt?.({
        id: 'ticket-unknown',
        status: 'error',
        result: {
          runId: 'baseline-run-old',
          receiptStatus: 'failed',
          status: 'failed',
          error: '命令被拒绝',
        },
      }),
    );
    expect(await screen.findByText('暂停开仓失败')).toBeInTheDocument();
    expect(
      mocks.api.mock.calls.filter(([path]) => path === 'prepare'),
    ).toHaveLength(1);
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });
  it('separates an unselected instance from an empty instance list', async () => {
    mocks.baselineRunning = true;
    mocks.api.mockImplementation((path: string, body?: unknown) =>
      path === 'strategy-groups'
        ? Promise.resolve({ groups: [{ id: 'baseline', name: '基础组' }] })
        : defaultAPI(path, body),
    );
    render(<StrategyRunPanel />);
    expect(
      await screen.findByText('选择记录 · 共 2 个实例'),
    ).toBeInTheDocument();
    expect(screen.queryByText('尚无运行实例')).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: '暂停开仓' }),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '查看当前运行' }));
    expect(screen.getByRole('button', { name: '暂停开仓' })).toBeEnabled();
  });

  it('shows the selected run budget independently from new-run inputs and updates a stop receipt from final runtime status', async () => {
    mocks.baselineRunning = true;
    let stopped = false;
    mocks.api.mockImplementation((path: string, body?: unknown) => {
      if (path.endsWith('/runtime')) {
        const value = runtime('baseline');
        const managed = value.groups[0].runs[1];
        return Promise.resolve({
          ...value,
          groups: [
            {
              ...value.groups[0],
              runs: [
                {
                  ...managed,
                  budgetUsdt: '1234',
                  durationSeconds: 1800,
                  status: stopped ? 'stopped' : 'running',
                },
              ],
            },
          ],
        });
      }
      return defaultAPI(path, body);
    });
    render(<StrategyRunPanel />);
    await ready();
    await selectRun('baseline-run-old · 运行中');
    expect(screen.getByText('1234 USDT')).toBeInTheDocument();
    expect(
      screen.getByRole('spinbutton', { name: '运行预算 USDT' }),
    ).toHaveValue('2000.00');
    fireEvent.click(screen.getByRole('button', { name: '停止运行' }));
    await screen.findByRole('dialog');
    stopped = true;
    fireEvent.click(screen.getByRole('button', { name: '提交测试回执' }));
    expect(
      await screen.findByText('停止请求已受理 · 已停止'),
    ).toBeInTheDocument();
    expect(screen.queryByText('启动失败')).not.toBeInTheDocument();
  });
  it('stops the explicitly displayed legacy run through its existing native confirmation only', async () => {
    mocks.api.mockImplementation((path: string, body?: unknown) =>
      path === 'strategy-groups'
        ? Promise.resolve({
            groups: [
              {
                id: 'baseline',
                name: '四板块',
                runId: 'run-24h-02',
                status: 'running',
              },
            ],
          })
        : defaultAPI(path, body),
    );
    render(<StrategyRunPanel />);
    const stop = await screen.findByRole('button', { name: '停止运行' });
    await waitFor(() => expect(stop).toBeEnabled());
    fireEvent.click(stop);
    await waitFor(() =>
      expect(mocks.api).toHaveBeenCalledWith('prepare', {
        kind: 'native',
        name: 'stop',
        arguments: { runId: 'run-24h-02' },
      }),
    );
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.role = 'admin';
    mocks.start = true;
    mocks.available = true;
    mocks.baselineRunning = false;
    mocks.api.mockImplementation(defaultAPI);
  });
  afterEach(cleanup);

  it('checks the exact budget and duration without preparing or executing a run', async () => {
    render(<StrategyRunPanel instrument="BTC-USDT" />);
    await ready();
    expect(screen.getByRole('button', { name: '开始运行' })).toBeEnabled();
    await preflight();
    expect(mocks.api).toHaveBeenCalledWith(
      'strategy-groups/baseline/preflight',
      {
        budgetUsdt: 2000,
        durationSeconds: 3600,
      },
    );
    expect(mocks.api.mock.calls.some(([path]) => path === 'prepare')).toBe(
      false,
    );
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('only prepares start and opens confirmation after successful preflight', async () => {
    render(<StrategyRunPanel />);
    await ready();
    await preflight();
    fireEvent.click(screen.getByRole('button', { name: '开始运行' }));
    expect(await screen.findByRole('dialog')).toHaveTextContent(
      '确认启动 baseline',
    );
    expect(mocks.api).toHaveBeenCalledWith('prepare', {
      kind: 'group',
      name: 'start',
      arguments: {
        groupId: 'baseline',
        budgetUsdt: 2000,
        durationSeconds: 3600,
      },
    });
    expect(
      mocks.api.mock.calls.filter(([path]) => path === 'prepare'),
    ).toHaveLength(1);
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
    fireEvent.click(screen.getByRole('button', { name: '取消确认' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('invalidates preflight when either budget or duration changes', async () => {
    render(<StrategyRunPanel />);
    await ready();
    await preflight();
    fireEvent.change(
      screen.getByRole('spinbutton', { name: '运行预算 USDT' }),
      {
        target: { value: '1500' },
      },
    );
    expect(screen.queryByText(/配置已核验/)).not.toBeInTheDocument();
    await preflight();
    expect(mocks.api).toHaveBeenLastCalledWith(
      'strategy-groups/baseline/preflight',
      {
        budgetUsdt: 1500,
        durationSeconds: 3600,
      },
    );
    fireEvent.change(
      screen.getByRole('spinbutton', { name: '运行时长 分钟' }),
      {
        target: { value: '30' },
      },
    );
    expect(screen.queryByText(/配置已核验/)).not.toBeInTheDocument();
    expect(mocks.api.mock.calls.some(([path]) => path === 'prepare')).toBe(
      false,
    );
  });

  it.each([
    'viewer',
    'missing capability',
    'unavailable runtime',
  ])('disables start and configuration for %s', async (condition) => {
    if (condition === 'viewer') mocks.role = 'viewer';
    if (condition === 'missing capability') mocks.start = false;
    if (condition === 'unavailable runtime') mocks.available = false;
    render(<StrategyRunPanel />);
    await screen.findByText('baseline signal');
    expect(screen.getByRole('button', { name: '检查配置' })).toBeDisabled();
    expect(screen.getByRole('button', { name: '开始运行' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '开始运行' }));
    expect(mocks.api.mock.calls.some(([path]) => path === 'prepare')).toBe(
      false,
    );
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });

  it('clears old status, preflight, budget and confirmation when the group changes', async () => {
    mocks.baselineRunning = true;
    render(<StrategyRunPanel />);
    await ready();
    await selectRun('baseline-run-old · 运行中');
    expect(screen.getByText('baseline-run-old')).toBeInTheDocument();
    fireEvent.change(
      screen.getByRole('spinbutton', { name: '运行预算 USDT' }),
      {
        target: { value: '1500' },
      },
    );
    await preflight();
    fireEvent.click(screen.getByRole('button', { name: '开始运行' }));
    await screen.findByRole('dialog');
    await selectEnhanced();
    await ready();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.queryByText('baseline-run-old')).not.toBeInTheDocument();
    expect(
      screen.getByRole('spinbutton', { name: '运行预算 USDT' }),
    ).toHaveValue('2000.00');
    expect(screen.getByRole('button', { name: '开始运行' })).toBeEnabled();
    expect(screen.queryByText(/配置已核验/)).not.toBeInTheDocument();
  });

  it('ignores a late preflight response for the previous group', async () => {
    let complete: ((value: unknown) => void) | undefined;
    mocks.api.mockImplementation((path: string, input?: unknown) =>
      path === 'strategy-groups/baseline/preflight'
        ? new Promise((resolve) => {
            complete = resolve;
          })
        : defaultAPI(path, input),
    );
    render(<StrategyRunPanel />);
    await ready();
    fireEvent.click(screen.getByRole('button', { name: '检查配置' }));
    await selectEnhanced();
    await act(async () => complete?.({ ok: true }));
    expect(screen.getByRole('button', { name: '开始运行' })).toBeEnabled();
    expect(screen.queryByText(/配置已核验/)).not.toBeInTheDocument();
    expect(mocks.api.mock.calls.some(([path]) => path === 'prepare')).toBe(
      false,
    );
  });

  it('does not prepare or execute when the automatic configuration check fails', async () => {
    mocks.api.mockImplementation((path: string, input?: unknown) =>
      path.endsWith('/preflight') || path === 'prepare'
        ? Promise.reject(new Error('信号文件校验失败'))
        : defaultAPI(path, input),
    );
    render(<StrategyRunPanel />);
    await ready();
    fireEvent.click(screen.getByRole('button', { name: '开始运行' }));
    await screen.findByText(/信号文件校验失败/);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });

  it('opens confirmation from one start action using the server configuration preview once', async () => {
    render(<StrategyRunPanel />);
    await ready();
    fireEvent.click(screen.getByRole('button', { name: '开始运行' }));
    await screen.findByRole('dialog');
    const calls = mocks.api.mock.calls.map(([path]) => path);
    expect(calls.filter((path) => path === 'prepare')).toHaveLength(1);
    expect(calls).not.toContain('strategy-groups/baseline/preflight');
    expect(calls).not.toContain('execute');
  });

  it('opens the current run for viewing without sending a lifecycle command', async () => {
    mocks.baselineRunning = true;
    render(<StrategyRunPanel />);
    await screen.findByRole('button', { name: '查看当前运行' });
    fireEvent.click(screen.getByRole('button', { name: '查看当前运行' }));
    expect(screen.getByRole('button', { name: '停止运行' })).toBeEnabled();
    expect(
      mocks.api.mock.calls.some(
        ([path]) => path === 'prepare' || path === 'execute',
      ),
    ).toBe(false);
  });

  it('never stops history or another managed run and binds an explicit current-run stop', async () => {
    mocks.baselineRunning = true;
    mocks.start = false;
    render(<StrategyRunPanel />);
    await screen.findByText('baseline signal');
    expect(screen.getByRole('button', { name: '停止运行' })).toBeDisabled();
    expect(screen.queryByText('baseline-run-old')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '停止运行' }));
    expect(mocks.api.mock.calls.some(([path]) => path === 'prepare')).toBe(
      false,
    );
    await selectRun('another-managed-run · 已结束');
    expect(screen.getByRole('button', { name: '停止运行' })).toBeDisabled();
    await selectRun('baseline-run-old · 运行中');
    expect(screen.getByRole('button', { name: '停止运行' })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: '停止运行' }));
    expect(await screen.findByRole('dialog')).toHaveTextContent(
      'baseline-run-old',
    );
    expect(mocks.api).toHaveBeenCalledWith('prepare', {
      kind: 'group',
      name: 'stop',
      arguments: { groupId: 'baseline', runId: 'baseline-run-old' },
    });
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
    await selectRun('历史 · legacy-history');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '停止运行' })).toBeDisabled();
  });

  it('does not reopen an old run confirmation when its prepare resolves after switching runs', async () => {
    let complete: ((value: unknown) => void) | undefined;
    mocks.baselineRunning = true;
    mocks.start = false;
    mocks.api.mockImplementation((path: string, input?: unknown) =>
      path === 'prepare'
        ? new Promise((resolve) => {
            complete = resolve;
          })
        : defaultAPI(path, input),
    );
    render(<StrategyRunPanel />);
    await screen.findByText('baseline signal');
    await selectRun('baseline-run-old · 运行中');
    fireEvent.click(screen.getByRole('button', { name: '停止运行' }));
    await selectRun('历史 · legacy-history');
    await act(async () =>
      complete?.({
        id: 'old-run-ticket',
        operation: {
          kind: 'group',
          name: 'stop',
          arguments: { groupId: 'baseline', runId: 'baseline-run-old' },
        },
      }),
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '停止运行' })).toBeDisabled();
  });
});
