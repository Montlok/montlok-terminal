import { Link, useModel } from '@umijs/max';
import { Button } from 'antd';
import { useEffect, useState } from 'react';
import { api } from './api';
export function HeaderStatus() {
  const { profiles, account } = useModel('operator');
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);
  const active = profiles.find((profile) => profile.active);
  return (
    <div className="header-status">
      <span
        className={`connection-dot ${account.privateConnected ? 'online' : ''}`}
      />
      <Link to="/settings/accounts/connections">
        {active?.id === 'tokyo-demo' && active.name === '东京 · OKX Demo'
          ? 'OKX'
          : active?.name || '连接中'}
      </Link>
      <span className="environment">
        {active?.mode === 'demo' ? '模拟盘' : active ? '实盘 · 只读' : '—'}
      </span>
      <span className="header-clock">
        {now.toLocaleTimeString('zh-CN', { hour12: false })} UTC+8
      </span>
      <Button
        type="text"
        onClick={() =>
          void api('logout', {}).then(() => window.location.replace('/login'))
        }
      >
        退出
      </Button>
    </div>
  );
}
