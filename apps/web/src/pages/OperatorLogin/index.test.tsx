import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import OperatorLogin from './index';

const webauthn = vi.hoisted(() => ({
  browserSupportsWebAuthn: vi.fn(() => true),
  startRegistration: vi.fn(),
  startAuthentication: vi.fn(),
}));
vi.mock('@simplewebauthn/browser', () => webauthn);

function reply(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status });
}

describe('Passkey login', () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal('fetch', fetchMock);
    window.history.replaceState({}, '', '/login');
    webauthn.browserSupportsWebAuthn.mockReturnValue(true);
  });
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it('shows one passkey action without password fields or automatic prompts', () => {
    render(<OperatorLogin />);
    expect(
      screen.getByRole('button', { name: /使用通行密钥登录/ }),
    ).toBeEnabled();
    expect(document.querySelector('input')).toBeNull();
    expect(webauthn.startAuthentication).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('removes the enrollment fragment and requires a separate login after registration', async () => {
    window.history.replaceState({}, '', '/login#enroll=one-use-token');
    const options = { challenge: 'local-test-only' };
    const credential = { id: 'local-test-only' };
    fetchMock
      .mockResolvedValueOnce(reply(options))
      .mockResolvedValueOnce(reply({ registered: true }));
    webauthn.startRegistration.mockResolvedValueOnce(credential);
    render(<OperatorLogin />);
    expect(window.location.hash).toBe('');
    fireEvent.click(screen.getByRole('button', { name: /创建通行密钥/ }));
    await waitFor(() =>
      expect(screen.getByRole('status')).toHaveTextContent('通行密钥已保存'),
    );
    expect(webauthn.startRegistration).toHaveBeenCalledWith({
      optionsJSON: options,
    });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      token: 'one-use-token',
    });
    expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({ credential });
    expect(
      screen.getByRole('button', { name: /使用通行密钥登录/ }),
    ).toBeEnabled();
    expect(webauthn.startAuthentication).not.toHaveBeenCalled();
  });

  it('surfaces cancellation and permits retry', async () => {
    fetchMock.mockResolvedValueOnce(reply({ challenge: 'local-test-only' }));
    webauthn.startAuthentication.mockRejectedValueOnce(
      new DOMException('Cancelled', 'NotAllowedError'),
    );
    render(<OperatorLogin />);
    fireEvent.click(screen.getByRole('button', { name: /使用通行密钥登录/ }));
    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent(
        '验证已取消或超时，请重试',
      ),
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('button', { name: /使用通行密钥登录/ }),
    ).toBeEnabled();
  });

  it('opens the portfolio workspace after successful passkey login', async () => {
    const replace = vi
      .spyOn(window.location, 'replace')
      .mockImplementation(() => {});
    fetchMock
      .mockResolvedValueOnce(reply({ challenge: 'local-test-only' }))
      .mockResolvedValueOnce(reply({ authenticated: true }));
    webauthn.startAuthentication.mockResolvedValueOnce({
      id: 'local-test-only',
    });
    render(<OperatorLogin />);
    fireEvent.click(screen.getByRole('button', { name: /使用通行密钥登录/ }));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledExactlyOnceWith(
        '/workspace/portfolio/overview',
      ),
    );
    replace.mockRestore();
  });

  it('rejects an expired enrollment link before opening an authenticator', async () => {
    window.history.replaceState({}, '', '/login#enroll=expired-token');
    fetchMock.mockResolvedValueOnce(
      reply({ error: '设置链接已失效，请重新获取' }, 400),
    );
    render(<OperatorLogin />);
    fireEvent.click(screen.getByRole('button', { name: /创建通行密钥/ }));
    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent('设置链接已失效'),
    );
    expect(webauthn.startRegistration).not.toHaveBeenCalled();
  });

  it('disables the action when the browser does not support WebAuthn', () => {
    webauthn.browserSupportsWebAuthn.mockReturnValue(false);
    render(<OperatorLogin />);
    expect(
      screen.getByRole('button', { name: /使用通行密钥登录/ }),
    ).toBeDisabled();
    expect(screen.getByRole('alert')).toHaveTextContent(
      '请使用支持通行密钥的浏览器',
    );
  });
});
