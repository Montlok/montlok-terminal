import { useModel } from '@umijs/max';
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { TerminalClient } from '@montlok/sdk';
import { EventEnvelope, EventType } from '@montlok/sdk/protocol';
import { ensureSession, api, type Row } from './api';
import { DataGrid } from './DataGrid';
import { Input, Select } from 'antd';
import { EventInspector } from './EventInspector';
import './eventDetail.css';

const timeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  hour12: false,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});
function eventTime(ns: bigint) {
  if (ns <= 0n) return '—';
  const seconds = ns / 1_000_000_000n;
  const fraction = (ns % 1_000_000_000n).toString().padStart(9, '0');
  const time = timeFormatter.format(Number(seconds) * 1000);
  return `${time}.${fraction}`;
}
const EVENT_LABELS: Partial<Record<EventType, string>> = {
  [EventType.ORDER_SUBMITTED]: '委托提交',
  [EventType.ORDER_ACCEPTED]: '委托确认',
  [EventType.ORDER_REJECTED]: '委托拒绝',
  [EventType.ORDER_CANCEL_REQUESTED]: '撤单提交',
  [EventType.ORDER_CANCELED]: '撤单确认',
  [EventType.FILL_RECEIVED]: '成交',
  [EventType.POSITION_UPDATED]: '持仓更新',
  [EventType.PNL_UPDATED]: '收益更新',
  [EventType.RUN_STATE_CHANGED]: '运行状态',
  [EventType.MODEL_INFERENCE_COMPLETED]: '模型推理',
  [EventType.SIGNAL_GENERATED]: '信号',
  [EventType.RISK_DECISION]: '风控',
  [EventType.TARGET_POSITION_CHANGED]: '目标更新',
  [EventType.ROUTE_UPDATED]: '路由更新',
  [EventType.ALERT_RAISED]: '告警',
  [EventType.ALERT_CLEARED]: '告警恢复',
  [EventType.OPERATION_RECEIPT_UPDATED]: '操作回执',
};
const PHASE = {
  connecting: '连接中',
  snapshot: '同步快照',
  live: '已同步',
  recovering: '恢复事件',
  disconnected: '连接中断',
};
type BlotterRow = {
  time: string;
  instrument: string;
  type: string;
  sequence: string;
  correlation: string;
  source: string;
  event: EventEnvelope;
};

export default function EventBlotter() {
  const { selectedGroup, selectedRuns } = useModel('operator');
  const requestedRun = selectedRuns?.[selectedGroup] || '';
  const selection = `${selectedGroup}/${requestedRun}`;
  const [resolved, setResolved] = useState({
    selection: '',
    run: '',
    account: '',
  });
  const runId = resolved.selection === selection ? resolved.run : '';
  const accountId = resolved.selection === selection ? resolved.account : '';
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true;
    void api<Row>(
      `strategy-groups/${encodeURIComponent(selectedGroup)}?view=market${requestedRun ? `&runId=${encodeURIComponent(requestedRun)}` : ''}`,
    )
      .then((value) => {
        if (requestedRun && value.runId !== requestedRun)
          throw new Error('事件流的运行实例与所选记录不一致');
        if (active)
          setResolved({
            selection,
            run: typeof value.runId === 'string' ? value.runId : '',
            account: typeof value.accountId === 'string' ? value.accountId : '',
          });
      })
      .catch((reason) => {
        if (active)
          setError(reason instanceof Error ? reason.message : String(reason));
      });
    return () => {
      active = false;
    };
  }, [selectedGroup, requestedRun, selection]);
  const [selected, setSelected] = useState<EventEnvelope>();
  const [query, setQuery] = useState(''),
    [kind, setKind] = useState('all');
  const grid = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState(300);
  useEffect(() => {
    const observer = new ResizeObserver((entries) => {
      const next = Math.floor(entries[0]?.contentRect.height || 0);
      if (next > 0) setHeight(next);
    });
    if (grid.current) observer.observe(grid.current);
    return () => observer.disconnect();
  }, []);
  const [client] = useState(() => {
    const url = new URL('/api/v2/stream', window.location.href);
    url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return new TerminalClient(url.toString(), setError);
  });
  const state = useSyncExternalStore(
    client.store.subscribe,
    client.store.getSnapshot,
    client.store.getSnapshot,
  );
  const rendered = useRef(new Map<string, BlotterRow>());
  const observed = useRef(new Map<string, EventEnvelope>());
  useEffect(
    () =>
      client.store.onCritical((event) => {
        observed.current.set(event.eventId, event);
        while (observed.current.size > 20_000) {
          const first = observed.current.keys().next().value;
          if (first === undefined) break;
          observed.current.delete(first);
        }
      }),
    [client],
  );
  useEffect(() => {
    let active = true;
    setSelected(undefined);
    observed.current.clear();
    setError('');
    if (runId && accountId)
      void ensureSession()
        .then(() => {
          if (active)
            client.start({ accountId, strategyGroupId: selectedGroup, runId }, [
              `run.${runId}`,
            ]);
        })
        .catch((reason) => setError(String(reason)));
    return () => {
      active = false;
      client.stop();
    };
  }, [client, selectedGroup, runId, accountId]);
  const rows = useMemo(() => {
    const next = new Map<string, BlotterRow>();
    const history = new Map(
      [...state.events.values()].map((event) => [event.eventId, event]),
    );
    for (const [id, event] of observed.current) history.set(id, event);
    for (const [key, event] of history) {
      if (
        event.runId !== runId ||
        event.accountId !== accountId ||
        event.strategyGroupId !== selectedGroup
      )
        continue;
      const prior = rendered.current.get(key);
      next.set(
        key,
        prior?.event === event
          ? prior
          : {
              time: eventTime(event.occurredAtNs),
              instrument: event.instrumentId,
              type: EVENT_LABELS[event.eventType] || EventType[event.eventType],
              sequence: event.streamSeq.toString(),
              correlation: event.correlationId || '—',
              source: event.source,
              event,
            },
      );
    }
    rendered.current = next;
    return [...next.values()]
      .sort((a, b) =>
        a.event.streamSeq > b.event.streamSeq
          ? -1
          : a.event.streamSeq < b.event.streamSeq
            ? 1
            : 0,
      )
      .slice(0, 20_000);
  }, [state.version, runId, accountId, selectedGroup]);
  const filtered = useMemo(
    () =>
      rows.filter(
        (row) =>
          (kind === 'all' || row.type === kind) &&
          (!query ||
            `${row.instrument} ${row.type} ${row.correlation} ${row.source}`
              .toLowerCase()
              .includes(query.toLowerCase())),
      ),
    [rows, query, kind],
  );
  return (
    <section className="event-blotter">
      <div className="event-blotter-main">
        <div className="panel-heading">
          <strong>事件与订单</strong>
          <span>
            {PHASE[state.phase]} · {filtered.length} 条 · 合并 {state.conflated}{' '}
            · 缺口 {state.gaps}
          </span>
        </div>
        <div className="event-filter-bar">
          <Select
            aria-label="事件类型"
            value={kind}
            onChange={setKind}
            options={[
              { value: 'all', label: '全部事件' },
              ...Object.values(EVENT_LABELS).map((value) => ({
                value,
                label: value,
              })),
            ]}
          />
          <Input
            aria-label="筛选运行事件"
            allowClear
            placeholder="品种、事件、关联编号或来源"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <div className="event-blotter-grid" ref={grid}>
          <DataGrid
            rows={filtered}
            height={height}
            emptyLabel={
              error || (runId ? '等待运行事件' : '选择运行实例后读取事件')
            }
            onSelectRow={(row) => setSelected(row.event)}
            columns={[
              { key: 'time', title: '时间 / UTC+8', width: 180 },
              { key: 'instrument', title: '品种', width: 160 },
              { key: 'type', title: '事件', width: 100 },
              { key: 'sequence', title: '序号', width: 95 },
              { key: 'correlation', title: '关联编号', width: 245 },
              { key: 'source', title: '来源', width: 160 },
            ]}
          />
        </div>
        <div className="event-source-footer">
          {runId || '选择运行实例'}
          {error ? ` · ${error}` : ''}
        </div>
      </div>
      <EventInspector event={selected} />
    </section>
  );
}
