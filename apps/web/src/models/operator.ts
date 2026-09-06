import {
  type SetStateAction,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import { api, ensureSession, type Row } from '../operator/api';
import {
  type RunSelection,
  readSelection,
  saveSelection,
  validateSelection,
} from '../operator/selectionStorage';

export default function useOperator() {
  const [selection, setSelection] = useState<RunSelection>({
    selectedGroup: 'live-yesterday-alpha-test-1u',
    selectedRuns: {},
  });
  const [selectionOwner, setSelectionOwner] = useState('');
  const selectionEdits = useRef(0);
  const { selectedGroup, selectedRuns } = selection;
  const setSelectedGroup = useCallback((value: SetStateAction<string>) => {
    selectionEdits.current += 1;
    setSelection((current) => ({
      ...current,
      selectedGroup:
        typeof value === 'function' ? value(current.selectedGroup) : value,
    }));
  }, []);
  const setSelectedRuns = useCallback(
    (value: SetStateAction<Record<string, string>>) => {
      selectionEdits.current += 1;
      setSelection((current) => ({
        ...current,
        selectedRuns:
          typeof value === 'function' ? value(current.selectedRuns) : value,
      }));
    },
    [],
  );
  useEffect(() => {
    if (window.location.pathname === '/login') return;
    let disposed = false;
    void ensureSession()
      .then(async ({ operator }) => {
        const saved = readSelection(operator);
        const restored = saved ? await validateSelection(saved) : undefined;
        if (disposed) return;
        if (restored && selectionEdits.current === 0) setSelection(restored);
        setSelectionOwner(operator);
      })
      .catch(() => {});
    return () => {
      disposed = true;
    };
  }, []);
  useEffect(() => {
    if (selectionOwner) saveSelection(selectionOwner, selection);
  }, [selectionOwner, selection]);
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
  return {
    account,
    paper,
    profiles,
    catalog,
    error,
    epoch,
    refresh,
    selectedGroup,
    setSelectedGroup,
    selectedRuns,
    setSelectedRuns,
  };
}
