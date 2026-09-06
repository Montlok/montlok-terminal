import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SessionInfo } from './operator/api';

const ensureSession = vi.fn<() => Promise<SessionInfo>>();

vi.mock('@umijs/max', () => ({
  Link: ({ children }: { children: unknown }) => children,
}));
vi.mock('./operator/api', () => ({ ensureSession }));
vi.mock('./operator/HeaderStatus', () => ({ HeaderStatus: () => null }));
vi.mock('./operator/theme.css', () => ({}));
vi.mock('../config/defaultSettings', () => ({
  default: { navTheme: 'realDark', title: '交易操作台' },
}));

function visit(pathname: string) {
  window.history.replaceState({}, '', pathname);
}

describe('app getInitialState', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    visit('/terminal');
  });

  it('returns the operator user once the session is established', async () => {
    ensureSession.mockResolvedValue({
      csrf: 'test',
      operator: 'gabira',
      role: 'admin',
      mode: 'demo',
    });
    const { getInitialState } = await import('./app');

    const state = await getInitialState();

    expect(ensureSession).toHaveBeenCalledTimes(1);
    expect(state.currentUser).toEqual({
      name: 'gabira',
      userid: 'gabira',
      access: 'admin',
    });
    expect(state.settings).toEqual({
      navTheme: 'realDark',
      title: '交易操作台',
    });
    expect(state.settingDrawerOpen).toBe(false);
    await expect(state.fetchUserInfo?.()).resolves.toEqual(state.currentUser);
  });

  it('returns an empty state when the session cannot be established', async () => {
    ensureSession.mockRejectedValue(new Error('请登录交易操作台'));
    const { getInitialState } = await import('./app');

    await expect(getInitialState()).resolves.toEqual({});
  });

  it('keeps a viewer identity read-only in the application layout', async () => {
    ensureSession.mockResolvedValue({
      csrf: 'test',
      operator: 'audit',
      role: 'viewer',
      mode: 'live_readonly',
    });
    const { getInitialState } = await import('./app');
    expect((await getInitialState()).currentUser?.access).toBe('viewer');
  });

  it('skips the session probe on the login page', async () => {
    visit('/login');
    const { getInitialState } = await import('./app');

    await expect(getInitialState()).resolves.toEqual({});
    expect(ensureSession).not.toHaveBeenCalled();
  });
});
