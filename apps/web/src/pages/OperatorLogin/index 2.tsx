import { LockOutlined, UserOutlined } from '@ant-design/icons';
import { LoginForm, ProFormText } from '@ant-design/pro-components';
import { Alert, ConfigProvider, theme } from 'antd';
import { useState } from 'react';

export default function OperatorLogin() {
  const [error, setError] = useState('');
  return (
    <ConfigProvider
      theme={{
        algorithm: theme.darkAlgorithm,
        token: { colorPrimary: '#1677ff', borderRadius: 4 },
      }}
    >
      <main className="operator-login">
        <section style={{ width: 'min(400px, 100vw)' }}>
          <LoginForm
            title={<span style={{ color: '#e6e8eb' }}>Montlok</span>}
            submitter={{ searchConfig: { submitText: '登录' } }}
            onFinish={async (values) => {
              setError('');
              try {
                const response = await fetch('/api/login', {
                  method: 'POST',
                  credentials: 'same-origin',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(values),
                });
                if (!response.ok) throw new Error(await response.text());
                window.location.replace('/trade');
                return true;
              } catch (reason) {
                setError(String(reason));
                return false;
              }
            }}
          >
            {error && (
              <Alert
                title={error}
                type="error"
                showIcon
                style={{ marginBottom: 20 }}
              />
            )}
            <ProFormText
              name="username"
              label="账户"
              fieldProps={{
                prefix: <UserOutlined />,
                autoComplete: 'username',
              }}
              rules={[{ required: true, message: '输入账户' }]}
            />
            <ProFormText.Password
              name="password"
              label="密码"
              fieldProps={{
                prefix: <LockOutlined />,
                autoComplete: 'current-password',
              }}
              rules={[{ required: true, message: '输入密码' }]}
            />
          </LoginForm>
        </section>
      </main>
    </ConfigProvider>
  );
}
