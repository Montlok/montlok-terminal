import { useModel } from '@umijs/max';
import { Button, Select, Tooltip } from 'antd';
import { useEffect, useState } from 'react';
import { api, type Row } from './api';
import { ConfirmOperation } from './ConfirmOperation';

export function connectionEnvironment(profile?: Row): string {
  if (profile?.mode === 'demo') return '模拟盘';
  if (profile?.mode === 'live_readonly') return '实盘';
  return '未选择';
}

export function connectionPermissions(profile?: Row): string {
  const raw = profile?.verification?.permissions;
  if (typeof raw !== 'string' || !raw.trim()) return '未核验';
  const labels: Record<string, string> = {
    read_only: '读取',
    trade: '交易',
    withdraw: '提现',
    transfer: '划转',
    earn: '赚币',
  };
  return raw
    .split(/[,;\s]+/)
    .filter(Boolean)
    .map((permission) => labels[permission] || permission)
    .join('、');
}

export function applicationAccess(
  profile: Row | undefined,
  role?: string,
): string {
  if (!profile) return '未选择连接';
  if (role !== 'admin') return '应用：查看者';
  return profile.mode === 'demo' ? '应用：可操作' : '应用：策略控制';
}

export function HeaderStatus() {
  const { profiles, account, refresh } = useModel('operator');
  const { initialState } = useModel('@@initialState');
  const [ticket, setTicket] = useState<Row>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);
  const active = profiles.find((profile) => profile.active);
  const access = applicationAccess(active, initialState?.currentUser?.access);
  async function select(id: string) {
    if (id === active?.id) return;
    setBusy(true);
    setError('');
    try {
      setTicket(
        await api('prepare', {
          kind: 'profile',
          name: 'select',
          arguments: { id },
        }),
      );
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '无法切换账户');
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="header-status">
      <span
        className={`connection-dot ${account.privateConnected ? 'online' : ''}`}
      />
      <Select
        aria-label="账户与交易环境"
        className="header-account-select"
        value={active?.id}
        loading={busy}
        disabled={busy || initialState?.currentUser?.access !== 'admin'}
        placeholder="选择账户"
        showSearch={{ optionFilterProp: 'searchText' }}
        popupMatchSelectWidth={false}
        onChange={(id) => void select(id)}
        options={profiles.map((profile) => ({
          value: profile.id,
          label: `${profile.name} · ${connectionEnvironment(profile)}`,
          searchText: [
            profile.name,
            profile.id,
            profile.keyMask,
            profile.verification?.uid,
            connectionEnvironment(profile),
            connectionPermissions(profile),
          ].join(' '),
        }))}
      />
      {error && (
        <Tooltip title={error}>
          <span className="negative">账户操作未完成</span>
        </Tooltip>
      )}
      <Tooltip
        trigger={['hover', 'focus']}
        title={
          <>
            <div>API 权限：{connectionPermissions(active)}</div>
            <div>{access}</div>
            <div>账户：{active?.verification?.uid || '未核验'}</div>
          </>
        }
      >
        <Button
          type="text"
          className="environment"
          aria-label={`连接权限；API：${connectionPermissions(active)}；${access}`}
        >
          {access}
        </Button>
      </Tooltip>
      <span className="header-clock">
        {now.toLocaleTimeString('zh-CN', {
          hour12: false,
          timeZone: 'Asia/Shanghai',
        })}{' '}
        UTC+8
      </span>
      <Button
        type="text"
        onClick={() =>
          void api('logout', {})
            .then(() => window.location.replace('/login'))
            .catch((reason) =>
              setError(reason instanceof Error ? reason.message : '退出失败'),
            )
        }
      >
        退出
      </Button>
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(result) => {
          setError(result.status === 'completed' ? '' : '账户切换未完成');
          void refresh();
        }}
      />
    </div>
  );
}
