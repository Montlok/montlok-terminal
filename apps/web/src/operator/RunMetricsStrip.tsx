import { Drawer } from 'antd';
import { useState } from 'react';
import { number, timeOf } from './api';
import { useRunObservation } from './runObservation';
import './runMetrics.css';

/** Every headline number can expose the actual value, scope and source metadata. */
export function RunMetricsStrip({
  group,
  run,
}: {
  group: string;
  run: string;
}) {
  const observation = useRunObservation(group, run);
  const data = observation.data;
  const [selected, setSelected] = useState<string>();
  const metrics = [
    {
      id: 'portfolio.nav',
      label: '运行净值',
      unit: 'USDT',
      value: data?.metrics.nav,
    },
    {
      id: 'portfolio.pnl.total',
      label: '累计收益',
      unit: 'USDT',
      value: data?.metrics.pnl,
    },
    {
      id: 'portfolio.drawdown',
      label: '最大回撤',
      unit: '%',
      value: data?.metrics.maxDrawdownPct,
    },
    {
      id: 'execution.free_cash',
      label: '可用资金',
      unit: 'USDT',
      value: data?.execution?.freeCashUsdt,
    },
    {
      id: 'order.working_count',
      label: '工作委托',
      unit: '笔',
      value: data?.execution?.openOrders,
    },
    {
      id: 'latency.p95',
      label: '确认 P95',
      unit: 'ms',
      value: data?.execution?.ackP95Ms,
    },
  ];
  const field = metrics.find((value) => value.id === selected);
  const unavailable = data?.sources?.accounting === false;
  return (
    <section className="run-metrics-panel" aria-label="运行指标与来源">
      <div className="run-metrics-grid">
        {metrics.map((field) => {
          const value =
            unavailable && field.id.startsWith('portfolio.')
              ? null
              : field.value;
          const color =
            field.id === 'portfolio.pnl.total' &&
            value !== null &&
            value !== undefined
              ? Number(value) > 0
                ? 'positive'
                : Number(value) < 0
                  ? 'negative'
                  : ''
              : field.id === 'portfolio.drawdown' && Number(value) < 0
                ? 'negative'
                : '';
          return (
            <button
              type="button"
              key={field.id}
              aria-label={`查看${field.label}口径`}
              onClick={() => setSelected(field.id)}
            >
              <span>
                {field.label}
                <small>{field.unit}</small>
              </span>
              <strong className={color}>
                {number(value, field.unit === '笔' ? 0 : 3)}
              </strong>
            </button>
          );
        })}
      </div>
      <div className="run-metrics-source" role="status">
        {observation.error
          ? '运行指标更新延迟'
          : data?.fresh === false
            ? '等待最新运行快照'
            : data
              ? `更新 ${timeOf(data.observedAt)} · ${data.provenance?.source === 'nautilus.run_files' ? '东京 Rust 数据服务' : '运行服务'}`
              : '读取运行指标'}
      </div>
      <Drawer
        open={!!field}
        onClose={() => setSelected(undefined)}
        title={field ? `${field.label} · 数据口径` : ''}
        width={420}
      >
        {field && (
          <dl className="run-metric-definition">
            <dt>原始值</dt>
            <dd>
              {field.value === null || field.value === undefined
                ? '—'
                : String(field.value)}
            </dd>
            <dt>单位</dt>
            <dd>{field.unit}</dd>
            <dt>字段</dt>
            <dd>{field.id}</dd>
            <dt>执行账户</dt>
            <dd>{data?.accountId || '—'}</dd>
            <dt>策略组</dt>
            <dd>{group}</dd>
            <dt>运行实例</dt>
            <dd>{data?.runId || '—'}</dd>
            <dt>估值 / 观测时间</dt>
            <dd>
              {data?.observedAt
                ? new Date(data.observedAt * 1000).toLocaleString('zh-CN', {
                    timeZone: 'Asia/Shanghai',
                    hour12: false,
                  }) + ' UTC+8'
                : '—'}
            </dd>
            <dt>数据来源</dt>
            <dd>{data?.provenance?.source || '—'}</dd>
            <dt>计算口径</dt>
            <dd>
              {data?.provenance?.accounting_basis ===
              'reported_run_mark_to_market'
                ? '本次运行记录的盯市结果'
                : data?.provenance?.accounting_basis || '—'}
            </dd>
            <dt>计算版本</dt>
            <dd>{data?.provenance?.calculation_version || '—'}</dd>
          </dl>
        )}
      </Drawer>
    </section>
  );
}
