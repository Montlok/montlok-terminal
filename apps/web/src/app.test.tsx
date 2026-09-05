import { beforeEach, describe, expect, it, vi } from 'vitest';

const ensureSession = vi.fn<() => Promise<void>>();

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
    ensureSession.mockResolvedValue();
    const { getInitialState } = await import('./app');

    const state = await getInitialState();

    expect(ensureSession).toHaveBeenCalledTimes(1);
    expect(state.currentUser).toEqual({
      name: '交易操作员',
      userid: 'operator',
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

  it('skips the session probe on the login page', async () => {
    visit('/login');
    const { getInitialState } = await import('./app');

    await expect(getInitialState()).resolves.toEqual({});
    expect(ensureSession).not.toHaveBeenCalled();
  });
});
