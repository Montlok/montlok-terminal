import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applicationAccess,
  connectionPermissions,
  HeaderStatus,
} from './HeaderStatus';

const mocks = vi.hoisted(() => ({
  api: vi.fn(),
  refresh: vi.fn(),
  role: 'admin',
  profiles: [
    { id: 'tokyo-demo', name: 'Demo', mode: 'demo', active: true },
    {
      id: 'tokyoreal',
      name: 'tokyoreal',
      mode: 'live_readonly',
      active: false,
    },
  ],
}));
vi.mock('@umijs/max', () => ({
  useModel: (key: string) =>
    key === '@@initialState'
      ? { initialState: { currentUser: { access: mocks.role } } }
      : {
          profiles: mocks.profiles,
          account: { privateConnected: true },
          refresh: mocks.refresh,
        },
}));
vi.mock('./api', () => ({ api: mocks.api }));
vi.mock('./ConfirmOperation', () => ({
  ConfirmOperation: ({
    ticket,
  }: {
    ticket?: { operation?: { arguments?: { id?: string } } };
  }) =>
    ticket ? (
      <div role="dialog">确认切换 {ticket.operation?.arguments?.id}</div>
    ) : null,
}));

describe('account and environment selector', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.role = 'admin';
  });
  afterEach(cleanup);

  it('preserves the active account on page load', () => {
    render(<HeaderStatus />);
    expect(
      screen.getByRole('combobox', { name: '账户与交易环境' }),
    ).toBeEnabled();
    expect(
      screen.getByRole('combobox', { name: '账户与交易环境' }).parentElement,
    ).toHaveAttribute('title', 'Demo · 模拟盘');
    expect(mocks.api).not.toHaveBeenCalled();
  });

  it('prepares a user-selected switch and never executes it automatically', async () => {
    mocks.api.mockResolvedValue({
      operation: { arguments: { id: 'tokyoreal' } },
    });
    render(<HeaderStatus />);
    fireEvent.mouseDown(
      screen.getByRole('combobox', { name: '账户与交易环境' }),
    );
    fireEvent.click(await screen.findByText('tokyoreal · 实盘'));
    await waitFor(() =>
      expect(screen.getByRole('dialog')).toHaveTextContent(
        '确认切换 tokyoreal',
      ),
    );
    expect(mocks.api).toHaveBeenCalledExactlyOnceWith('prepare', {
      kind: 'profile',
      name: 'select',
      arguments: { id: 'tokyoreal' },
    });
    expect(
      screen.getByRole('combobox', { name: '账户与交易环境' }).parentElement,
    ).toHaveAttribute('title', 'Demo · 模拟盘');
  });

  it('does not give a viewer connection-management controls', () => {
    mocks.role = 'viewer';
    render(<HeaderStatus />);
    expect(
      screen.getByRole('combobox', { name: '账户与交易环境' }),
    ).toBeDisabled();
    expect(mocks.api).not.toHaveBeenCalled();
  });

  it('shows feedback when logout requires renewed confirmation', async () => {
    mocks.api.mockRejectedValueOnce(new Error('会话已更新，请重新确认操作'));
    render(<HeaderStatus />);
    fireEvent.click(screen.getByRole('button', { name: /退出/ }));
    await waitFor(() =>
      expect(screen.getByText('账户操作未完成')).toBeInTheDocument(),
    );
    expect(mocks.api).toHaveBeenCalledExactlyOnceWith('logout', {});
  });

  it('distinguishes exchange API permission from the application policy', () => {
    const profile = {
      mode: 'live_readonly',
      verification: { permissions: 'read_only,trade' },
    };
    expect(connectionPermissions(profile)).toBe('读取、交易');
    expect(applicationAccess(profile, 'admin')).toBe('应用：策略控制');
    expect(applicationAccess({ mode: 'demo' }, 'admin')).toBe('应用：可操作');
    expect(applicationAccess({ mode: 'demo' }, 'viewer')).toBe('应用：查看者');
    expect(connectionPermissions({ mode: 'demo' })).toBe('未核验');
  });

  it('searches saved connections by account name and environment without switching them', async () => {
    render(<HeaderStatus />);
    const select = screen.getByRole('combobox', { name: '账户与交易环境' });
    fireEvent.mouseDown(select);
    fireEvent.change(select, { target: { value: '实盘' } });
    expect(await screen.findByText('tokyoreal · 实盘')).toBeInTheDocument();
    expect(
      screen.queryByRole('option', { name: 'Demo · 模拟盘' }),
    ).not.toBeInTheDocument();
    expect(mocks.api).not.toHaveBeenCalled();
  });
});
