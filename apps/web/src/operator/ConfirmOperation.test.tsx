import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfirmOperation } from './ConfirmOperation';

const { api } = vi.hoisted(() => ({ api: vi.fn() }));
vi.mock('./api', async (original) => ({
  ...(await original<typeof import('./api')>()),
  api,
}));
vi.mock('./ResultView', () => ({ RecordDetails: () => <span>技术参数</span> }));

describe('confirmation expiry', () => {
  it('describes continuous execution as manual stop, not zero minutes', () => {
    render(
      <ConfirmOperation
        ticket={{
          id: 'continuous-ticket',
          expiresAt: Date.now() / 1000 + 60,
          operation: {
            kind: 'group',
            name: 'start',
            arguments: { durationSeconds: 0 },
          },
          groupPreview: {
            mode: 'live',
            executionPolicy: { label: '连续报价' },
          },
        }}
        onClose={vi.fn()}
        onComplete={vi.fn()}
      />,
    );
    expect(screen.getByText('持续运行 · 手动停止')).toBeInTheDocument();
    expect(screen.getByText('连续报价')).toBeInTheDocument();
    expect(screen.queryByText('0 分钟')).not.toBeInTheDocument();
    expect(api).not.toHaveBeenCalled();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.clearAllMocks();
  });
  it('shows remaining time and cannot execute an expired confirmation', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T00:00:00Z'));
    render(
      <ConfirmOperation
        ticket={{
          id: 'ticket',
          expiresAt: Date.now() / 1000 + 2,
          operation: {
            kind: 'group',
            name: 'stop',
            arguments: { runId: 'run-a' },
          },
        }}
        onClose={vi.fn()}
        onComplete={vi.fn()}
      />,
    );
    expect(screen.getByText('确认有效期剩余 2 秒')).toBeInTheDocument();
    act(() => vi.advanceTimersByTime(2000));
    expect(
      screen.getByText('确认已过期，请取消并重新发起操作'),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '确 认' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '确 认' }));
    expect(api).not.toHaveBeenCalled();
  });

  it('shows the live account scope without a virtual budget row', () => {
    render(
      <ConfirmOperation
        ticket={{
          id: 'live-ticket',
          expiresAt: Date.now() / 1000 + 60,
          operation: {
            kind: 'group',
            name: 'start',
            arguments: { durationSeconds: 3600 },
          },
          groupPreview: {
            mode: 'live',
            groupName: '实盘高频',
            effect: '使用账户库存启动实盘策略',
            exposureCapUsdt: '1',
            liveInventory: {
              totalEqUsd: '417.5',
              pairs: [{ instrument: 'BTC-USDT' }, { instrument: 'XNVDA-USDT' }],
            },
          },
        }}
        onClose={vi.fn()}
        onComplete={vi.fn()}
      />,
    );
    expect(screen.getByText('OKX 实盘 · 订单启用')).toBeInTheDocument();
    expect(screen.getByText('1 USDT')).toBeInTheDocument();
    expect(screen.getByText('417.5 USD')).toBeInTheDocument();
    expect(screen.getByText('BTC-USDT、XNVDA-USDT')).toBeInTheDocument();
    expect(screen.queryByText('独立虚拟预算')).not.toBeInTheDocument();
  });
  it('cannot confirm a dismissed ticket during its closing animation', () => {
    const close = vi.fn();
    render(
      <ConfirmOperation
        ticket={{
          id: 'closing',
          operation: {
            kind: 'group',
            name: 'flatten',
            arguments: { runId: 'run-a' },
          },
        }}
        onClose={close}
        onComplete={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: '取 消' }));
    expect(close).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole('button', { name: '确 认' }));
    expect(api).not.toHaveBeenCalled();
  });
});
