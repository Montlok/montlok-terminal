import { useModel } from '@umijs/max';
import { Alert, Select, Table } from 'antd';
import { useCallback, useState } from 'react';
import { api, number, type Row } from '../../operator/api';
import { TimeSeriesChart } from '../../operator/TimeSeriesChart';
import { usePoll } from '../../operator/usePoll';
import {
  executionModeLabel,
  runDateTime,
  statusLabel,
  strategyDisplayName,
} from '../StrategyGroups/groupModel';
import '../Portfolio/portfolio.css';

export default function ResearchHistory() {
  const [groups, setGroups] = useState<Row[]>([]);
  const {
    selectedGroup: selected,
    setSelectedGroup: setSelected,
    selectedRuns,
    setSelectedRuns,
  } = useModel('operator');
  const runId = selectedRuns[selected] || '';
  const [runs, setRuns] = useState<Row[]>([]);
  const [data, setData] = useState<{ id: string; detail: Row; curve: Row }>();
  const [error, setError] = useState('');
  const load = useCallback(
    async (signal: AbortSignal) => {
      try {
        const suffix = runId ? `?runId=${encodeURIComponent(runId)}` : '';
        const [listing, detail, curve, runtime] = await Promise.all([
          api('strategy-groups'),
          api(`strategy-groups/${encodeURIComponent(selected)}${suffix}`),
          api(
            `strategy-groups/${encodeURIComponent(selected)}/equity${suffix}`,
          ),
          api(`strategy-groups/${encodeURIComponent(selected)}/runtime`),
        ]);
        if (signal.aborted) return;
        setGroups(listing.groups || []);
        const records =
          runtime.groups?.find((g: Row) => g.groupId === selected)?.runs || [];
        setRuns(records);
        setData({ id: `${selected}/${runId}`, detail, curve });
        setError('');
      } catch (reason) {
        if (!signal.aborted)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
    },
    [selected, runId],
  );
  usePoll(load, 10000);
  const detail = data?.id === `${selected}/${runId}` ? data.detail : undefined;
  const curve = data?.id === `${selected}/${runId}` ? data.curve : undefined;
  return (
    <div className="management-page">
      <div className="page-heading">
        <h1>运行历史</h1>
        <Select
          aria-label="历史策略组"
          value={selected}
          style={{ minWidth: 260 }}
          onChange={(value) => {
            setSelected(value);
            setSelectedRuns((current) => ({ ...current, [value]: '' }));
          }}
          options={groups.map((row) => ({
            value: row.id,
            label: `${strategyDisplayName(row)} · ${executionModeLabel(row)}`,
          }))}
        />
        <Select
          aria-label="历史运行记录"
          value={runId}
          style={{ minWidth: 240 }}
          onChange={(value) =>
            setSelectedRuns((current) => ({
              ...current,
              [selected]: value,
            }))
          }
          options={[
            { value: '', label: '最近一次运行' },
            ...runs.map((row) => ({
              value: row.runId,
              label: `${row.runId} · ${statusLabel(row.status)}`,
            })),
          ]}
        />
      </div>
      {error && <Alert type="error" title={error} />}
      <p>
        {detail ? executionModeLabel(detail) : '读取执行环境'} ·{' '}
        {statusLabel(detail?.status || '')} · {runDateTime(detail?.observedAt)}{' '}
        UTC+8
      </p>
      <div className="portfolio-metrics panel">
        {[
          ['净值 / USDT', 'nav'],
          ['收益 / USDT', 'pnl'],
          ['最大回撤 / %', 'maxDrawdownPct'],
          ['手续费 / USDT', 'fees'],
        ].map(([label, key]) => (
          <div key={key}>
            <span>{label}</span>
            <strong>{number(detail?.metrics?.[key])}</strong>
          </div>
        ))}
      </div>
      <div className="portfolio-charts">
        <section className="panel">
          <h3>运行净值 / USDT</h3>
          <TimeSeriesChart label="运行净值" points={curve?.points || []} />
        </section>
        <section className="panel">
          <h3>运行回撤 / %</h3>
          <TimeSeriesChart
            label="运行回撤"
            points={curve?.drawdown || []}
            color="#ee5464"
          />
        </section>
      </div>
      <Table
        size="small"
        rowKey={(row: Row) => row.instrument || row.id}
        pagination={{ pageSize: 12 }}
        dataSource={detail?.positions || []}
        columns={[
          { title: '品种', dataIndex: 'instrument' },
          { title: '数量', dataIndex: 'quantity' },
          {
            title: '市值 / USDT',
            dataIndex: 'notional',
            render: (v) => number(v),
          },
          {
            title: '未实现收益 / USDT',
            dataIndex: 'unrealizedPnl',
            render: (v) => number(v),
          },
        ]}
      />
    </div>
  );
}
