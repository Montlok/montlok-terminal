import { KeyOutlined } from '@ant-design/icons';
import {
  browserSupportsWebAuthn,
  startAuthentication,
  startRegistration,
} from '@simplewebauthn/browser';
import { Alert, Button, ConfigProvider, theme } from 'antd';
import { useEffect, useState } from 'react';

async function passkeyRequest<T>(
  action: string,
  body: unknown = {},
): Promise<T> {
  const response = await fetch(`/api/passkeys/${action}`, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    let message = text;
    try {
      message = JSON.parse(text).error || text;
    } catch {
      // HTTP errors can be plain text.
    }
    throw new Error(message || '暂时无法登录，请重试');
  }
  return JSON.parse(text) as T;
}

export default function OperatorLogin() {
  const [token, setToken] = useState(
    () =>
      new URLSearchParams(window.location.hash.slice(1)).get('enroll') || '',
  );
  const [error, setError] = useState('');
  const [registered, setRegistered] = useState(false);
  const [busy, setBusy] = useState(false);
  const supported = browserSupportsWebAuthn();
  useEffect(() => {
    if (window.location.hash.startsWith('#enroll=')) {
      window.history.replaceState(null, '', window.location.pathname);
    }
  }, []);

  async function submit() {
    setBusy(true);
    setError('');
    try {
      if (token) {
        const optionsJSON = await passkeyRequest<
          Parameters<typeof startRegistration>[0]['optionsJSON']
        >('register-options', { token });
        const credential = await startRegistration({ optionsJSON });
        await passkeyRequest('register', { credential });
        setToken('');
        setRegistered(true);
      } else {
        const optionsJSON =
          await passkeyRequest<
            Parameters<typeof startAuthentication>[0]['optionsJSON']
          >('login-options');
        const credential = await startAuthentication({ optionsJSON });
        await passkeyRequest('login', { credential });
        const next = new URLSearchParams(window.location.search).get('next');
        let destination = '/workspace/portfolio/overview';
        if (next) {
          const url = new URL(next, window.location.origin);
          if (url.origin === window.location.origin && /^\/(workspace|strategies|trade|market|assets|engine|settings)(\/|$)/.test(url.pathname)) {
            destination = url.pathname + url.search;
          }
        }
        window.location.replace(destination);
      }
    } catch (reason) {
      setError(
        reason instanceof Error && reason.name === 'NotAllowedError'
          ? '验证已取消或超时，请重试'
          : reason instanceof Error
            ? reason.message
            : '暂时无法登录，请重试',
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <ConfigProvider
      theme={{
        algorithm: theme.darkAlgorithm,
        token: { colorPrimary: '#1677ff', borderRadius: 4 },
      }}
    >
      <main className="operator-login">
        <section className="passkey-login" aria-labelledby="login-title">
          <h1 id="login-title">Montlok</h1>
          {registered && <p role="status">通行密钥已保存，请验证登录</p>}
          {error && <Alert title={error} type="error" showIcon />}
          {!supported && (
            <Alert title="请使用支持通行密钥的浏览器" type="warning" showIcon />
          )}
          <Button
            type="primary"
            size="large"
            block
            icon={<KeyOutlined />}
            loading={busy}
            disabled={!supported}
            onClick={submit}
          >
            {token ? '创建通行密钥' : '使用通行密钥登录'}
          </Button>
        </section>
      </main>
    </ConfigProvider>
  );
}
