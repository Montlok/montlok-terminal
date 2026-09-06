import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Models from './index';

const mocks = vi.hoisted(() => ({ api: vi.fn(), role: 'admin' }));
vi.mock('@umijs/max', () => ({
  useModel: (key: string) =>
    key === '@@initialState'
      ? { initialState: { currentUser: { access: mocks.role } } }
      : { selectedGroup: 'baseline', selectedRuns: {} },
  Link: ({ to, children }: { to: string; children: ReactNode }) => (
    <a href={to}>{children}</a>
  ),
}));
vi.mock('../../operator/api', async (original) => ({
  ...(await original<typeof import('../../operator/api')>()),
  api: mocks.api,
}));
vi.mock('../../operator/ConfirmOperation', () => ({
  ConfirmOperation: ({
    ticket,
    onComplete,
    onClose,
  }: {
    ticket?: Record<string, unknown>;
    onComplete: (result: Record<string, unknown>) => void;
    onClose: () => void;
  }) =>
    ticket ? (
      <div role="dialog">
        等待用户确认
        <button
          type="button"
          onClick={() => {
            onComplete({
              status: 'completed',
              result: {
                ...release,
                validation: 'artifact_valid',
                deployReady: false,
                deployReadiness: 'requires_target_node_preflight',
              },
            });
            onClose();
          }}
        >
          返回校验结果
        </button>
      </div>
    ) : null,
}));
const release = {
  artifactId: 'artifact-a',
  releaseId: 'release-a',
  modelVersion: 'v1',
  runnerId: 'rdt',
  family: 'ONNX',
  status: 'validated',
  manifestSha256: 'abc123',
  validation: { ok: true },
  featureContract: {
    names: ['return'],
    requiredMarkets: ['BTC-USDT'],
    sequenceBars: 60,
    barSeconds: 60,
  },
  runtime: { device: 'cpu' },
  policy: { allowedModes: ['sandbox'] },
};

describe('model publication workspace', () => {
  it('renders the final nested BFF manifest, integer RDT head and explicit node rejection', async () => {
    const original = mocks.api.getMockImplementation();
    const current = {
      releaseId: 'rdt-current',
      modelVersion: 'rdt/v1',
      runnerId: 'rdt4quant_v1',
      artifactId: 'artifact-a',
      manifestSha256: 'abc123',
      status: 'published',
      validation: 'artifact_valid',
      deployReady: false,
      deployReadiness: 'requires_target_node_preflight',
      nodeCompatibility: {
        ready: false,
        reason: '目标节点没有可用 CUDA BF16',
        device: 'cuda',
        nodeId: 'local',
      },
      manifest: {
        family: 'rdt4quant_multiasset',
        runtime: { device: 'cuda' },
        domainContracts: {
          'crypto:BTC-USDT': {
            domain: 'crypto',
            instrument: 'BTC-USDT',
            names: ['return'],
            sequenceBars: 60,
            barSeconds: 60,
          },
        },
        outputContract: {
          selectedHeadByDomain: { crypto: 1 },
          horizons: { crypto: [15, 60] },
          horizonUnit: { crypto: 'minutes' },
        },
        policy: { allowedModes: ['shadow'] },
      },
    };
    mocks.api.mockImplementation((path: string, body?: unknown) =>
      path === 'model-releases'
        ? Promise.resolve({ releases: [current] })
        : original?.(path, body),
    );
    render(<Models />);
    fireEvent.click(await screen.findByRole('button', { name: '版本详情' }));
    expect(screen.getByText('索引 1 · 60 分钟')).toBeInTheDocument();
    expect(
      screen.getAllByText('目标节点没有可用 CUDA BF16').length,
    ).toBeGreaterThan(0);
    expect(
      screen.queryByText('请检查目标节点运行环境'),
    ).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '发布此版本' })).toBeDisabled();
    expect(screen.getAllByText('影子运行').length).toBeGreaterThan(0);
    expect(
      screen.queryByText(/不代表|不等于|不会自动|不下单|不启动/),
    ).not.toBeInTheDocument();
  });
  it('keeps the just-validated artifact available for publication when the server list contains published versions only', async () => {
    const original = mocks.api.getMockImplementation();
    mocks.api.mockImplementation((path: string, body?: unknown) =>
      path === 'model-releases'
        ? Promise.resolve({ releases: [] })
        : original?.(path, body),
    );
    render(<Models />);
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '校验模型' })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole('button', { name: '校验模型' }));
    fireEvent.click(
      await screen.findByRole('button', { name: '返回校验结果' }),
    );
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '发布此版本' })).toBeEnabled(),
    );
    expect(
      screen.getAllByText('请检查目标节点运行环境').length,
    ).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole('button', { name: '发布此版本' }));
    await waitFor(() =>
      expect(mocks.api).toHaveBeenCalledWith('prepare', {
        kind: 'model_release',
        name: 'publish',
        arguments: { artifactId: 'artifact-a', manifestSha256: 'abc123' },
      }),
    );
  });
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.role = 'admin';
    window.history.replaceState(
      {},
      '',
      '/strategies/resources/models?artifactId=artifact-a',
    );
    mocks.api.mockImplementation(async (path: string, body?: unknown) =>
      path === 'model-releases'
        ? { releases: [release] }
        : path === 'artifacts'
          ? {
              artifacts: [
                {
                  id: 'artifact-a',
                  kind: 'model',
                  name: 'RDT',
                  version: 'v1',
                  filename: 'rdt.zip',
                },
              ],
            }
          : path === 'prepare'
            ? { id: 'ticket', operation: body }
            : { available: true, groups: [] },
    );
  });
  afterEach(cleanup);
  it('prepares validation of a verified registered artifact without publishing or starting', async () => {
    render(<Models />);
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '校验模型' })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole('button', { name: '校验模型' }));
    await screen.findByRole('dialog');
    expect(mocks.api).toHaveBeenCalledWith('prepare', {
      kind: 'model_release',
      name: 'validate',
      arguments: { artifactId: 'artifact-a' },
    });
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });
  it('publishes only through an explicit confirmation bound to version and manifest hash', async () => {
    render(<Models />);
    fireEvent.click(await screen.findByRole('button', { name: '版本详情' }));
    fireEvent.click(screen.getByRole('button', { name: '发布此版本' }));
    await screen.findByRole('dialog');
    expect(mocks.api).toHaveBeenCalledWith('prepare', {
      kind: 'model_release',
      name: 'publish',
      arguments: { artifactId: 'artifact-a', manifestSha256: 'abc123' },
    });
    expect(screen.getByText('模型发布流程')).toBeInTheDocument();
    expect(mocks.api.mock.calls.some(([path]) => path === 'execute')).toBe(
      false,
    );
  });
  it('keeps viewer validation and publication disabled with reasons', async () => {
    mocks.role = 'viewer';
    render(<Models />);
    fireEvent.click(await screen.findByRole('button', { name: '版本详情' }));
    expect(
      screen.getByRole('button', { name: '校验模型' }),
    ).toHaveAccessibleDescription('当前账户为只读');
    expect(screen.getByRole('button', { name: '发布此版本' })).toBeDisabled();
  });
});
