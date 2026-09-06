import { ReloadOutlined } from '@ant-design/icons';
import { useModel } from '@umijs/max';
import { Alert, Button } from 'antd';
import { useCallback, useMemo, useRef, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';
import { DataGrid } from '../../operator/DataGrid';
import { TimeSeriesChart } from '../../operator/TimeSeriesChart';
import { usePoll } from '../../operator/usePoll';
import {
  type Distribution,
  type ExecutionDetail,
  executionAnalytics,
  finite,
} from './executionModel';
import './executionAnalytics.css';

function DistributionChart({
  rows,
  label,
  unit,
  empty,
}: {
  rows: Distribution[];
  label: string;
  unit: string;
  empty: string;
}) {
  const peak = Math.max(0, ...rows.map((row) => Math.abs(row.value)));
  return rows.length ? (
    <ul className="execution-bars" aria-label={label}>
      {rows.map((row) => (
        <li key={row.label}>
          <div>
            <span title={row.label}>{row.label.replace('.OKX', '')}</span>
            <b>
              {number(row.value, 6)} {unit}
            </b>
          </div>
          <div className="execution-bar-track">
            <span
              className={`execution-bar execution-bar-${row.tone}`}
              style={{
                width: `${peak ? (Math.abs(row.value) / peak) * 100 : 0}%`,
              }}
            />
          </div>
        </li>
      ))}
    </ul>
  ) : (
    <div className="execution-empty">{empty}</div>
  );
}

export default function ExecutionAnalytics() {
  const { selectedGroup, selectedRuns } = useModel('operator');
  const runId = selectedRuns?.[selectedGroup] || '';
  const selection = `${selectedGroup}/${runId}`;
  const current = useRef(selection);
  current.current = selection;
  const generation = useRef(0);
  const [loaded, setLoaded] = useState<{
    selection: string;
    detail: ExecutionDetail;
  }>();
  const [failure, setFailure] = useState<{
    selection: string;
    message: string;
  }>();
  const [loading, setLoading] = useState(false);
  const load = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++generation.current;
      setLoading(true);
      try {
        const suffix = runId ? `?runId=${encodeURIComponent(runId)}` : '';
        const detail = await api<ExecutionDetail>(
          `strategy-groups/${encodeURIComponent(selectedGroup)}${suffix}`,
        );
        if (
          signal?.aborted ||
          current.current !== selection ||
          requestId !== generation.current
        )
          return;
        if (detail.id !== selectedGroup || (runId && detail.runId !== runId))
          throw new Error('返回的运行实例与当前选择不一致');
        setLoaded({ selection, detail });
        setFailure(undefined);
      } catch (reason) {
        if (
          !signal?.aborted &&
          current.current === selection &&
          requestId === generation.current
        )
          setFailure({
            selection,
            message: reason instanceof Error ? reason.message : String(reason),
          });
      } finally {
        if (
          !signal?.aborted &&
          current.current === selection &&
          requestId === generation.current
        )
          setLoading(false);
      }
    },
    [selectedGroup, runId, selection],
  );
  usePoll(load, 5000);
  const detail = loaded?.selection === selection ? loaded.detail : undefined;
  const error = failure?.selection === selection ? failure.message : '';
  const analysis = useMemo(
    () => (detail ? executionAnalytics(detail) : undefined),
    [detail],
  );
  const count = (value: number | null | undefined) =>
    value === null || value === undefined ? '未知' : number(value, 0);
  const age = finite(detail?.health?.snapshotAgeSeconds);
  const reportFees = finite(detail?.metrics?.fees);
  const amount = (value: unknown) => number(value, 6);
  return (
    <div className="management-page execution-analytics">
      <div className="page-heading">
        <div>
          <h1>执行质量</h1>
          <p>成交、成本与订单状态 · 按策略组和运行实例统计</p>
        </div>
        <Button
          icon={<ReloadOutlined />}
          loading={loading}
          onClick={() => void load()}
        >
          刷新
        </Button>
      </div>
      <section className="execution-context" aria-label="执行分析范围">
        <strong>{detail?.name || selectedGroup}</strong>
        <span>{detail?.modeLabel || '环境待读取'}</span>
        <span>实例 {runId || detail?.runId || '尚未选择实例'}</span>
        <span>快照 {timeOf(detail?.observedAt)} UTC+8</span>
      </section>
      {error && (
        <Alert
          type="error"
          title={error}
          description={detail ? '当前保留上次成功读取的快照。' : undefined}
        />
      )}
      {analysis &&
        (detail?.sources?.orders === false ||
          detail?.sources?.fills === false ||
          (!analysis.ordersSource && !analysis.orders.length) ||
          (!analysis.fillsSource && !analysis.fills.length)) && (
          <Alert
            type="warning"
            title="数据来源未确认"
            description={`委托：${analysis.ordersSource ? '已同步' : '未知'} · 成交：${analysis.fillsSource ? '已同步' : '未知'}。下方仅呈现已读取记录。`}
          />
        )}
      {analysis &&
        !analysis.ordersSource &&
        analysis.orders.length > 0 &&
        detail?.sources?.orders !== false && (
          <p className="execution-note">
            已载入 {analysis.orders.length} 笔委托、{analysis.fills.length}{' '}
            笔成交 · 历史格式未标记完整性
          </p>
        )}
      <div className="execution-metrics panel">
        {[
          [
            analysis?.ordersTotal == null && analysis?.orders.length
              ? '已读取委托'
              : '委托总数',
            analysis?.ordersTotal == null
              ? count(analysis?.orders.length || null)
              : count(analysis.ordersTotal),
          ],
          [
            analysis?.fillsTotal == null && analysis?.fills.length
              ? '已读取成交'
              : '成交总数',
            analysis?.fillsTotal == null
              ? count(analysis?.fills.length || null)
              : count(analysis.fillsTotal),
          ],
          [
            '记录内成交额 / USDT',
            analysis?.notional === null || !analysis
              ? '未知'
              : number(analysis.notional, 2),
          ],
          [
            reportFees !== undefined
              ? '运行报表手续费 / USDT'
              : '记录内手续费 / USDT',
            reportFees !== undefined
              ? number(reportFees, 6)
              : analysis?.usdtFee === null || !analysis
                ? '未知'
                : number(analysis.usdtFee, 6),
          ],
          ['记录内拒单', count(analysis?.rejected)],
          ['快照年龄 / 秒', age === undefined ? '未知' : number(age, 1)],
        ].map(([label, value]) => (
          <div key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </div>
      <div className="execution-chart-grid">
        <section className="panel execution-volume">
          <div className="panel-heading">
            <strong>成交额 / USDT</strong>
            <span>5 分钟分桶 · UTC+8</span>
          </div>
          <TimeSeriesChart
            label="执行成交额"
            points={analysis?.volume || []}
            height={245}
          />
          <p className="execution-note">
            仅统计已记录成交，不补齐无数据时段。
            {analysis?.amountWithoutTime
              ? `${analysis.amountWithoutTime} 笔缺少成交时间，未进入图表。`
              : ''}
          </p>
        </section>
        <section className="panel">
          <div className="panel-heading">
            <strong>手续费分布 / USDT</strong>
            <span>按品种</span>
          </div>
          <DistributionChart
            label="品种手续费"
            rows={analysis?.feesByInstrument || []}
            unit="USDT"
            empty="暂无可核验的 USDT 手续费记录"
          />
        </section>
        <section className="panel">
          <div className="panel-heading">
            <strong>订单状态分布</strong>
            <span>当前记录</span>
          </div>
          <DistributionChart
            label="订单状态"
            rows={analysis?.statuses || []}
            unit="笔"
            empty={
              analysis?.ordersTotal === 0 ? '本实例尚无委托' : '订单状态未知'
            }
          />
        </section>
      </div>
      <section className="execution-quality-strip panel" aria-label="采集覆盖">
        <div>
          <span>成交记录覆盖</span>
          <strong>
            {analysis
              ? `${analysis.fills.length} / ${count(analysis.fillsTotal)}`
              : '未知'}
          </strong>
        </div>
        <div>
          <span>委托记录覆盖</span>
          <strong>
            {analysis
              ? `${analysis.orders.length} / ${count(analysis.ordersTotal)}`
              : '未知'}
          </strong>
        </div>
        <div>
          <span>实现滑点</span>
          <strong>未采集</strong>
        </div>
        <div>
          <span>决策 → 成交延迟</span>
          <strong>未采集</strong>
        </div>
        <div>
          <span>Maker / Taker</span>
          <strong>未采集</strong>
        </div>
      </section>
      {analysis &&
        (analysis.fillsPartial ||
          analysis.ordersPartial ||
          analysis.missingNotional > 0 ||
          analysis.feesWithoutCurrency > 0 ||
          analysis.feesOtherCurrency > 0 ||
          analysis.unknownFee > 0) && (
          <p className="execution-note" role="status">
            {analysis.fillsPartial || analysis.ordersPartial
              ? '图表基于已读取记录，不代表完整运行。 '
              : ''}
            {analysis.missingNotional
              ? `${analysis.missingNotional} 笔缺少可核验成交额；合约数量不直接视为基础币数量。 `
              : ''}
            {analysis.feesWithoutCurrency
              ? `${analysis.feesWithoutCurrency} 笔手续费币种未记录。 `
              : ''}
            {analysis.feesOtherCurrency
              ? `${analysis.feesOtherCurrency} 笔非 USDT 手续费未换算。 `
              : ''}
            {analysis.unknownFee
              ? `${analysis.unknownFee} 笔手续费未采集。`
              : ''}
          </p>
        )}
      <div className="execution-bottom-grid">
        <section className="panel">
          <div className="panel-heading">
            <strong>成交明细</strong>
            <span>
              {analysis ? `${analysis.fills.length} 条已读取` : '等待数据'}
            </span>
          </div>
          <DataGrid
            rows={analysis?.fills || []}
            height={260}
            columns={[
              { key: 'time', title: '成交时间', render: timeOf, width: 115 },
              { key: 'instrument', title: '品种', width: 190 },
              { key: 'side', title: '方向', width: 80 },
              {
                key: 'quantity',
                title: '成交数量',
                render: amount,
                width: 120,
              },
              { key: 'price', title: '成交价', render: amount, width: 115 },
              { key: 'fee', title: '手续费原值', render: amount, width: 120 },
              {
                key: 'feeCurrency',
                title: '币种',
                render: (value: unknown) => String(value || '未记录'),
                width: 90,
              },
            ]}
          />
        </section>
        <section className="panel">
          <div className="panel-heading">
            <strong>执行健康</strong>
            <span>{detail?.health?.tradingState || '状态未知'}</span>
          </div>
          <ul className="execution-health">
            {(detail?.systems || []).map((row: Row, index) => (
              <li key={String(row.id || index)}>
                <div>
                  <strong>{row.label || row.id}</strong>
                  <span>{row.level || '未知'}</span>
                </div>
                <p>{row.detail || '未采集'}</p>
              </li>
            ))}
            {!detail?.systems?.length && <li>尚未读取到健康快照</li>}
          </ul>
          {(detail?.exceptions || []).slice(0, 5).map((row: Row, index) => (
            <p className="execution-exception" key={String(row.id || index)}>
              <strong>{row.title || row.severity || '运行异常'}</strong>{' '}
              {row.detail || ''}
            </p>
          ))}
        </section>
      </div>
    </div>
  );
}
