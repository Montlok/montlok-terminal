import { ReloadOutlined } from '@ant-design/icons';
import { history, useLocation, useModel } from '@umijs/max';
import { Alert, Button, Select, Tabs } from 'antd';
import { useCallback, useMemo, useRef, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';
import { DataGrid, type GridColumn } from '../../operator/DataGrid';
import { ModelRuntime } from '../../operator/ModelRuntime';
import { TimeSeriesChart } from '../../operator/TimeSeriesChart';
import { usePoll } from '../../operator/usePoll';
import { statusLabel, strategyDisplayName } from '../StrategyGroups/groupModel';

const POSITION_COLUMNS: GridColumn[] = [
  { key: 'instrument', title: '品种', width: 220 },
  { key: 'side', title: '方向', width: 90 },
  { key: 'quantity', title: '数量', render: (value) => number(value, 8) },
  {
    key: 'markPrice',
    title: '标记价',
    render: (value) => number(value, 6),
  },
  {
    key: 'notional',
    title: '名义金额 / USDT',
    render: (value) => number(value, 3),
  },
  {
    key: 'unrealizedPnl',
    title: '浮动收益 / USDT',
    render: (value) => number(value, 3),
  },
];

function eventTime(value: unknown): string {
  const numeric = Number(value);
  return timeOf(
    Number.isFinite(numeric) && numeric > 1e15 ? numeric / 1e6 : value,
  );
}

const ORDER_COLUMNS: GridColumn[] = [
  { key: 'time', title: '时间', width: 105, render: eventTime },
  { key: 'instrument', title: '品种', width: 190 },
  { key: 'side', title: '方向', width: 80 },
  { key: 'price', title: '价格', render: (value) => number(value, 6) },
  { key: 'quantity', title: '数量', render: (value) => number(value, 8) },
  {
    key: 'filledQuantity',
    title: '已成交',
    render: (value) => number(value, 8),
  },
  { key: 'status', title: '状态', width: 120 },
  { key: 'venueOrderId', title: 'OKX 订单号', width: 210 },
];

const FILL_COLUMNS: GridColumn[] = [
  { key: 'time', title: '时间', width: 105, render: eventTime },
  { key: 'instrument', title: '品种', width: 190 },
  { key: 'side', title: '方向', width: 80 },
  { key: 'price', title: '成交价', render: (value) => number(value, 6) },
  { key: 'quantity', title: '成交量', render: (value) => number(value, 8) },
  { key: 'fee', title: '手续费', render: (value) => number(value, 8) },
  { key: 'feeCurrency', title: '费用币种', width: 110 },
  { key: 'venueOrderId', title: 'OKX 订单号', width: 210 },
];

const STRATEGY_COLUMNS: GridColumn[] = [
  { key: 'id', title: '策略组', width: 220 },
  { key: 'artifact', title: '策略 / 模型', width: 260 },
  { key: 'mode', title: '状态', width: 120 },
  { key: 'fills', title: '成交', width: 100 },
  { key: 'runId', title: '运行实例', width: 260 },
];

const INSTRUMENT_COLUMNS: GridColumn[] = [
  { key: 'instrument', title: '品种', width: 190 },
  {
    key: 'position',
    title: '当前数量',
    render: (value) => number(value, 8),
  },
  {
    key: 'target',
    title: '目标数量',
    render: (value) => number(value, 8),
  },
  {
    key: 'reason',
    title: '执行状态',
    width: 210,
    render: (value) =>
      ({
        quoting: '连续报价',
        target_within_minimum: '位于目标区间',
        waiting_quote: '等待行情',
        retry_backoff: '订单重试',
        request_budget: '报单频率调度',
        available_cash: '可用 USDT 调度',
        minimum_size: '低于最小委托量',
      })[String(value)] || String(value || '跟踪中'),
  },
];

function elapsed(value: unknown): string {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return '—';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return `${hours}h ${minutes}m`;
}

export default function Engine() {
  const { selectedGroup, setSelectedGroup, selectedRuns, setSelectedRuns } =
    useModel('operator');
  const selectedRun = selectedRuns[selectedGroup] || '';
  const selection = `${selectedGroup}/${selectedRun}`;
  const currentSelection = useRef(selection);
  currentSelection.current = selection;
  const { pathname } = useLocation();
  const routeView = pathname.split('/').at(-1) || 'overview';
  const view = routeView === 'run' ? 'overview' : routeView;
  const dataKey = `${selection}/${view === 'overview' ? 'summary' : 'detail'}`;
  const currentDataKey = useRef(dataKey);
  currentDataKey.current = dataKey;
  const [groups, setGroups] = useState<Row[]>([]);
  const [runtime, setRuntime] = useState<Row>();
  const [detail, setDetail] = useState<Row>();
  const [loadedDataKey, setLoadedDataKey] = useState('');
  const [error, setError] = useState('');

  const loadGroups = useCallback(async (signal?: AbortSignal) => {
    try {
      const listing = await api('strategy-groups');
      if (!signal?.aborted) setGroups(listing.groups || []);
    } catch (reason) {
      if (!signal?.aborted)
        setError(reason instanceof Error ? reason.message : String(reason));
    }
  }, []);
  const loadRuntime = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const nextRuntime = await api(
          `strategy-groups/${encodeURIComponent(selectedGroup)}/runtime`,
        );
        if (signal?.aborted || currentSelection.current !== selection) return;
        setRuntime(nextRuntime);
      } catch (reason) {
        if (!signal?.aborted && currentSelection.current === selection) {
          const message =
            reason instanceof Error ? reason.message : String(reason);
          setRuntime({ available: false, reason: message, groups: [] });
        }
      }
    },
    [selectedGroup, selection],
  );
  const loadDetail = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const query = new URLSearchParams();
        if (selectedRun) query.set('runId', selectedRun);
        if (view === 'overview') query.set('view', 'summary');
        const nextDetail = await api(
          `strategy-groups/${encodeURIComponent(selectedGroup)}${query.size ? `?${query}` : ''}`,
        );
        if (signal?.aborted || currentDataKey.current !== dataKey) return;
        setDetail(nextDetail);
        setLoadedDataKey(dataKey);
        setError('');
      } catch (reason) {
        if (!signal?.aborted && currentDataKey.current === dataKey)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
    },
    [dataKey, selectedGroup, selectedRun, view],
  );
  usePoll(loadGroups, 30000);
  usePoll(loadRuntime, 1000);
  usePoll(loadDetail, view === 'overview' ? 5000 : 3000);

  const data = loadedDataKey === dataKey ? detail : undefined;
  const control = runtime?.groups?.find(
    (item: Row) => item.groupId === selectedGroup,
  );
  const runs: Row[] = control?.runs || [];
  const displayedRun = selectedRun
    ? runs.find((item) => item.runId === selectedRun)
    : runs.find((item) => item.runId === control?.runId) || runs[0];
  const engine = displayedRun?.engine;
  const execution = engine?.execution || {};
  const state = displayedRun?.status || data?.status || 'ready';
  const activeState = [
    'starting',
    'running',
    'halted',
    'reducing',
    'recovering',
    'stopping',
  ].includes(state);
  const original = groups.find((item) => item.id === selectedGroup);
  const historyRows: Row[] = execution.history || [];
  const charts = useMemo(
    () =>
      [
        ['报单 / 秒', 'orders', '#5b8ff9'],
        ['撤单 / 秒', 'cancels', '#f6bd16'],
        ['成交 / 秒', 'fills', '#30bf78'],
      ].map(([label, key, color]) => ({
        label,
        color,
        points: historyRows
          .map((row) => ({ time: Number(row.time), value: Number(row[key]) }))
          .filter(
            (point) =>
              Number.isFinite(point.time) && Number.isFinite(point.value),
          ),
      })),
    [historyRows],
  );
  const configRows = [
    ...(data?.runtimeConfig || []),
    ...Object.entries(execution.parameters || {}).map(([key, value]) => ({
      key: `执行.${key}`,
      value,
    })),
  ];

  return (
    <div className="management-page engine-page">
      <div className="page-heading engine-heading">
        <div>
          <h1>执行引擎</h1>
          <p>{strategyDisplayName(control || original)} · OKX 实盘</p>
        </div>
        <div className="native-actions engine-selectors">
          <Select
            aria-label="引擎策略组"
            value={selectedGroup}
            showSearch={{ optionFilterProp: 'label' }}
            onChange={setSelectedGroup}
            options={groups.map((item) => ({
              value: item.id,
              label: strategyDisplayName(item),
            }))}
          />
          <Select
            aria-label="引擎运行实例"
            value={selectedRun}
            showSearch={{ optionFilterProp: 'label' }}
            onChange={(value) =>
              setSelectedRuns((previous) => ({
                ...previous,
                [selectedGroup]: value,
              }))
            }
            options={[
              {
                value: '',
                label: control?.runId
                  ? `当前运行 · ${control.runId}`
                  : '最近运行',
              },
              ...runs.map((item) => ({
                value: item.runId,
                label: `${item.runId} · ${statusLabel(item.status)}`,
              })),
            ]}
          />
          <Button
            icon={<ReloadOutlined />}
            onClick={() =>
              void Promise.all([loadGroups(), loadRuntime(), loadDetail()])
            }
          >
            刷新
          </Button>
        </div>
      </div>
      {error && <Alert type="error" title={error} showIcon />}
      {runtime?.available === false && (
        <Alert
          type="warning"
          title={runtime.reason || '运行服务正在重新连接'}
          showIcon
        />
      )}
      <section className="engine-run-context panel" aria-label="引擎运行上下文">
        <div>
          <strong>
            {data?.runId || displayedRun?.runId || '选择运行实例'}
          </strong>
          <span className={state === 'running' ? 'positive' : ''}>
            {statusLabel(state)}
          </span>
        </div>
        {engine ? (
          <>
            <span>行情 {engine.marketReady ? '已连接' : '连接中'}</span>
            <span>交易 {engine.executionReady ? '已连接' : '连接中'}</span>
            <span>报单 {engine.ordersEnabled ? '已启用' : '准备中'}</span>
            <span>运行 {elapsed(engine.uptimeSeconds)}</span>
          </>
        ) : (
          <span>{activeState ? '正在连接运行引擎' : '历史运行记录'}</span>
        )}
      </section>
      <section className="engine-summary panel" aria-label="执行引擎指标">
        {[
          ['已接受委托', engine?.ordersAccepted ?? data?.ordersTotal],
          ['撤单', execution.cancel],
          ['成交', engine?.fillsTotal ?? data?.fillsTotal],
          ['工作委托', execution.openOrders ?? engine?.ordersOpen],
          ['决策周期', execution.decisionCycles],
          ['行情事件', execution.quoteEvents],
          [
            '确认 P50',
            execution.ackP50Ms == null
              ? '—'
              : `${number(execution.ackP50Ms, 2)} ms`,
          ],
          [
            '确认 P95',
            execution.ackP95Ms == null
              ? '—'
              : `${number(execution.ackP95Ms, 2)} ms`,
          ],
        ].map(([label, value]) => (
          <div key={String(label)}>
            <span>{label}</span>
            <strong>{value ?? '—'}</strong>
          </div>
        ))}
      </section>
      <Tabs
        activeKey={view}
        onChange={(key) => history.push(`/engine/run/${key}`)}
        destroyOnHidden
        items={[
          {
            key: 'overview',
            label: '总览',
            children: (
              <div className="engine-overview">
                {historyRows.length > 1 ? (
                  <div className="engine-rate-charts">
                    {charts.map((chart) => (
                      <section className="panel" key={chart.label}>
                        <div className="panel-heading">
                          <strong>{chart.label}</strong>
                          <span className="muted">
                            最近 {chart.points.length} 秒
                          </span>
                        </div>
                        <TimeSeriesChart
                          label={chart.label}
                          points={chart.points}
                          height={170}
                          color={chart.color}
                        />
                      </section>
                    ))}
                  </div>
                ) : (
                  <div className="engine-live-waiting panel">
                    {activeState
                      ? '实时速率将随下一个执行采样点更新'
                      : '选择当前运行查看实时执行速率'}
                  </div>
                )}
                <section className="panel engine-instrument-state">
                  <div className="panel-heading">
                    <strong>品种执行状态</strong>
                    <span className="muted">
                      {execution.instruments?.length || 0} 个品种
                    </span>
                  </div>
                  <DataGrid
                    rows={execution.instruments || []}
                    columns={INSTRUMENT_COLUMNS}
                    height={300}
                    emptyLabel="品种状态随执行周期更新"
                  />
                </section>
                {engine?.model && (
                  <section className="panel engine-model-state">
                    <div className="panel-heading">
                      <strong>模型推理</strong>
                    </div>
                    <ModelRuntime model={engine.model} />
                  </section>
                )}
              </div>
            ),
          },
          {
            key: 'positions',
            label: '持仓',
            children: (
              <DataGrid
                height={450}
                rows={data?.positions || []}
                columns={POSITION_COLUMNS}
                emptyLabel="持仓记录将随成交更新"
              />
            ),
          },
          {
            key: 'orders',
            label: '委托',
            children: (
              <DataGrid
                height={450}
                rows={data?.orders || []}
                columns={ORDER_COLUMNS}
                emptyLabel="委托记录将随报单更新"
              />
            ),
          },
          {
            key: 'fills',
            label: '成交',
            children: (
              <DataGrid
                height={450}
                rows={data?.fills || []}
                columns={FILL_COLUMNS}
                emptyLabel="成交记录将随 OKX 回报更新"
              />
            ),
          },
          {
            key: 'strategies',
            label: '策略',
            children: (
              <DataGrid
                rows={data?.strategies || []}
                columns={STRATEGY_COLUMNS}
                emptyLabel="策略状态随运行更新"
              />
            ),
          },
          {
            key: 'config',
            label: '参数',
            children: (
              <DataGrid
                rows={configRows}
                height={470}
                columns={[
                  { key: 'key', title: '配置项', width: 280 },
                  {
                    key: 'value',
                    title: '值',
                    width: 740,
                    render: (value) =>
                      typeof value === 'object'
                        ? JSON.stringify(value)
                        : String(value ?? '—'),
                  },
                ]}
                emptyLabel="运行参数随实例加载"
              />
            ),
          },
          {
            key: 'health',
            label: '监控',
            children: (
              <div className="engine-health">
                <DataGrid
                  rows={data?.systems || []}
                  columns={[
                    { key: 'label', title: '模块', width: 180 },
                    { key: 'level', title: '状态', width: 140 },
                    { key: 'detail', title: '详情', width: 650 },
                  ]}
                  emptyLabel="连接后将更新运行健康"
                />
                {!!data?.exceptions?.length && (
                  <DataGrid
                    rows={data.exceptions}
                    columns={[
                      { key: 'severity', title: '级别', width: 120 },
                      { key: 'title', title: '事件', width: 300 },
                      { key: 'detail', title: '详情', width: 600 },
                    ]}
                  />
                )}
              </div>
            ),
          },
        ]}
      />
    </div>
  );
}
