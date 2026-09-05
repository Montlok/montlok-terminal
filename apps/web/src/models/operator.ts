import { useCallback, useEffect, useState } from 'react';
import { api, ensureSession, type Row } from '../operator/api';

export default function useOperator() {
  const [account, setAccount] = useState<Row>({
    balances: [],
    orders: [],
    fills: [],
    available: false,
  });
  const [paper, setPaper] = useState<Row>();
  const [profiles, setProfiles] = useState<Row[]>([]);
  const [catalog, setCatalog] = useState<Row>({
    tools: [],
    routes: [],
    nativeMethods: [],
  });
  const [error, setError] = useState('');
  const [epoch, setEpoch] = useState(-1);
  const refresh = useCallback(async () => {
    try {
      await Promise.all([
        api('account').then(setAccount),
        api('paper').then(setPaper),
        api('profiles').then((connections) => {
          setProfiles(connections.profiles);
          setEpoch(connections.epoch);
        }),
        api('catalog').then(setCatalog),
      ]);
      setError('');
    } catch (reason) {
      setError(String(reason));
    }
  }, []);
  useEffect(() => {
    if (window.location.pathname === '/login') return;
    let events: EventSource | undefined;
    let disposed = false;
    void refresh();
    void ensureSession()
      .then(() => {
        if (disposed) return;
        events = new EventSource('/api/events');
        events.onmessage = (event) => {
          const next = JSON.parse(event.data);
          setAccount(next.account);
          setEpoch((previous) => {
            if (previous !== -1 && previous !== next.epoch) void refresh();
            return next.epoch;
          });
        };
        events.onerror = () => setError('账户推送连接中断，正在重新连接');
        events.onopen = () => setError('');
      })
      .catch(() => {});
    const timer = window.setInterval(() => {
      void api('paper')
        .then(setPaper)
        .catch(() => {});
    }, 10000);
    return () => {
      disposed = true;
      events?.close();
      window.clearInterval(timer);
    };
  }, [refresh]);
  return { account, paper, profiles, catalog, error, epoch, refresh };
}
