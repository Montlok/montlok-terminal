import { ReloadOutlined } from '@ant-design/icons';
import { history, useModel } from '@umijs/max';
import { Alert, Button, Select, Tabs } from 'antd';
import { useCallback, useRef, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';
import { DataGrid } from '../../operator/DataGrid';
import { valueLabel } from '../../operator/labels';
import { TimeSeriesChart } from '../../operator/TimeSeriesChart';
import { usePoll } from '../../operator/usePoll';
import {
  executionModeLabel,
  type GroupEquity,
  matchingGroup,
  runDateTime,
  runElapsed,
  runRecordCount,
  type StrategyGroup,
  stateWarning,
  statusLabel,
  strategyDisplayName,
} from './groupModel';
import './strategyGroups.css';

const formatNumber = (value: unknown) => number(value);

export default function StrategyGroups() {
  const [groups, setGroups] = useState<StrategyGroup[]>([]);
  const {
    selectedGroup: selected,
    setSelectedGroup: setSelected,
    selectedRuns: runSelections,
    setSelectedRuns: setRunSelections,
  } = useModel('operator');
  const [detail, setDetail] = useState<StrategyGroup>();
  const [curve, setCurve] = useState<GroupEquity>();
  const [runtimeRuns, setRuntimeRuns] = useState<Row[]>([]);
  const [loadedSelection, setLoadedSelection] = useState('');
  const [error, setError] = useState('');
  const runId = runSelections[selected] || '';
  const selection = `${selected}/${runId}`;
  const currentSelection = useRef(selection);
  currentSelection.current = selection;
  const load = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const suffix = runId ? `?runId=${encodeURIComponent(runId)}` : '';
        const [listing, result, equity, runtime] = await Promise.all([
          api<{ groups: StrategyGroup[] }>('strategy-groups'),
          api<StrategyGroup>(`strategy-groups/${selected}${suffix}`),
          api<GroupEquity>(`strategy-groups/${selected}/equity${suffix}`),
          api(`strategy-groups/${selected}/runtime`).catch(() => undefined),
        ]);
        if (signal?.aborted || currentSelection.current !== selection) return;
        setGroups(listing.groups);
        setDetail(result);
        setCurve(equity);
        setRuntimeRuns(
          runtime?.groups?.find((item: Row) => item.groupId === selected)
            ?.runs || [],
        );
        setLoadedSelection(selection);
        setError('');
      } catch (reason) {
        if (!signal?.aborted && currentSelection.current === selection)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
    },
    [selected, runId, selection],
  );
  usePoll(load, 5000);
  const group =
    loadedSelection === selection && (!runId || detail?.runId === runId)
      ? matchingGroup(detail, selected)
      : undefined;
  const equity =
    loadedSelection === selection && (!runId || curve?.runId === runId)
      ? matchingGroup(curve, selected)
      : undefined;
  const originalRun = groups.find((item) => item.id === selected)?.runId;
  const metrics = group?.metrics;
  const warning = stateWarning(group?.status || '');
  const historyUnavailable =
    !!group?.runId && equity?.sourceAvailable === false;
  return (
    <div className="management-page strategy-groups-page">
      <div className="page-heading">
        <div>
          <h1>策略组</h1>
          <p>运行实例历史 · 资金、持仓与收益独立统计</p>
        </div>
        <div className="group-selector">
          <Select
            aria-label="策略组"
            showSearch={{ optionFilterProp: 'label' }}
            value={selected}
            onChange={setSelected}
            loading={!groups.length}
            options={groups.map((item) => ({
              value: item.id,
              label: `${strategyDisplayName(item)} · ${statusLabel(item.status)}`,
            }))}
          />
          <Select
            aria-label="运行实例"
            value={runId}
            showSearch={{ optionFilterProp: 'label' }}
            onChange={(value) =>
              setRunSelections((current) => ({ ...current, [selected]: value }))
            }
            options={[
              {
                value: '',
                label: originalRun
                  ? `原始运行 · ${originalRun}`
                  : runtimeRuns.length
                    ? `选择记录 · 共 ${runtimeRuns.length} 个实例`
                    : '等待首次运行',
              },
              ...runtimeRuns
                .filter(
                  (item) =>
                    item.groupId === selected && item.runId !== originalRun,
                )
                .map((item) => ({
                  value: item.runId,
                  label: `${item.runId} · ${statusLabel(item.status)}`,
                })),
            ]}
          />
          <Button
            onClick={() => history.push('/strategies/resources/artifacts')}
          >
            模型与因子
          </Button>
          <Button icon={<ReloadOutlined />} onClick={() => void load()}>
            刷新
          </Button>
        </div>
      </div>
      {error && <Alert type="error" title={error} showIcon />}
      {group ? (
        <>
          <div className="group-run-heading">
            <div>
              <strong>{strategyDisplayName(group)}</strong>
              <span className="group-mode">{executionModeLabel(group)}</span>
              <span>{statusLabel(group.status)}</span>
            </div>
            <span>
              {group.runId ||
                (runtimeRuns.length
                  ? `选择运行记录 · 共 ${runtimeRuns.length} 个`
                  : '等待首次运行')}
            </span>
          </div>
          <section className="group-run-metadata" aria-label="信号与运行版本">
            <span>
              信号截至 <b>{group.signalAsOf || '读取中'}</b>
            </span>
            <span title={group.signalVersion || undefined}>
              信号版本 <b>{group.signalVersion?.slice(0, 12) || '读取中'}</b>
            </span>
            <span>
              启动 <b>{runDateTime(group.startedAt)}</b>
            </span>
            <span>
              {group.completedAt
                ? '结束'
                : group.scheduledStopAt
                  ? '计划结束'
                  : '运行方式'}{' '}
              <b>
                {group.completedAt || group.scheduledStopAt
                  ? runDateTime(group.completedAt ?? group.scheduledStopAt)
                  : '持续运行'}
              </b>
            </span>
            <span>
              已观测 <b>{runElapsed(group.elapsedSeconds)}</b>
            </span>
            <span>
              快照 <b>{runDateTime(group.observedAt)}</b> UTC+8
            </span>
          </section>
          {group.status === 'pending_validation' && (
            <Alert
              type="info"
              title="组合配置检查中"
              description={group.capabilities.reason}
            />
          )}
          {group.status === 'stale' && (
            <Alert type="warning" title="运行快照已超过 120 秒未更新" />
          )}
          {group.status === 'unavailable' && (
            <Alert type="warning" title="本组运行数据正在连接" />
          )}
          {warning && (
            <Alert type={warning.type} title={warning.title} showIcon />
          )}
          {(group.sources?.orders === false ||
            group.sources?.fills === false) && (
            <Alert
              type="warning"
              title="订单与成交视图未同步"
              description="当前记录数量未知；请查看运行健康中的数据来源异常。"
              showIcon
            />
          )}
          {historyUnavailable && (
            <Alert
              type="warning"
              title="历史曲线读取异常"
              description={`${equity?.sourceIssue || '历史文件读取失败'}${equity?.points.length ? '；下方保留已读取的历史曲线' : ''}`}
              showIcon
            />
          )}
          <div className="group-metrics panel">
            {[
              ['初始资金 / USDT', metrics?.capital],
              ['净值 / USDT', metrics?.nav],
              ['收益 / USDT', metrics?.pnl],
              ['收益率 / %', metrics?.returnPct],
              ['最大回撤 / %', metrics?.maxDrawdownPct],
              ['手续费 / USDT', metrics?.fees],
              ['成交笔数', metrics?.fills],
            ].map(([label, value]) => (
              <div key={String(label)}>
                <span>{label}</span>
                <strong>{number(value, 3)}</strong>
              </div>
            ))}
          </div>
          <div className="group-charts">
            <section className="panel">
              <div className="panel-heading">
                <strong>净值 / USDT</strong>
                <span>{equity?.sampleCount ?? 0} 条观测</span>
              </div>
              {historyUnavailable && !equity?.points.length ? (
                <div className="group-source-empty">请刷新或查看运行健康</div>
              ) : (
                <TimeSeriesChart
                  label={`${strategyDisplayName(group)}净值`}
                  points={equity?.points || []}
                  height={280}
                />
              )}
            </section>
            <section className="panel">
              <div className="panel-heading">
                <strong>最大回撤 / %</strong>
                <span>{executionModeLabel(group)}</span>
              </div>
              {historyUnavailable && !equity?.drawdown.length ? (
                <div className="group-source-empty">请刷新或查看运行健康</div>
              ) : (
                <TimeSeriesChart
                  label={`${strategyDisplayName(group)}最大回撤`}
                  points={equity?.drawdown || []}
                  color="#f05b65"
                  height={280}
                />
              )}
            </section>
          </div>
          {(equity?.partial || !!equity?.invalidLines) && (
            <Alert
              type="warning"
              title={
                equity.partial
                  ? '历史曲线正在读取，下一轮刷新继续加载'
                  : '历史曲线存在无效记录'
              }
              description={
                equity.invalidLines
                  ? `已跳过 ${equity.invalidLines} 条无效记录`
                  : undefined
              }
            />
          )}
          <section className="panel group-detail-tabs">
            <Tabs
              key={selected}
              destroyOnHidden
              items={[
                {
                  key: 'positions',
                  label: `持仓 (${runRecordCount(group, 'positions')})`,
                  children:
                    group.sources?.view === false ? (
                      <div className="group-source-empty">持仓数据未同步</div>
                    ) : (
                      <DataGrid
                        rows={group.positions || []}
                        columns={[
                          { key: 'instrument', title: '品种', width: 200 },
                          {
                            key: 'quantity',
                            title: '数量',
                            render: (value) => number(value, 8),
                          },
                          {
                            key: 'averagePrice',
                            title: '均价',
                            render: formatNumber,
                          },
                          {
                            key: 'markPrice',
                            title: '估值',
                            render: formatNumber,
                          },
                          {
                            key: 'unrealizedPnl',
                            title: '未实现盈亏 / USDT',
                            render: formatNumber,
                            width: 180,
                          },
                          {
                            key: 'notional',
                            title: '名义金额 / USDT',
                            render: formatNumber,
                            width: 180,
                          },
                        ]}
                      />
                    ),
                },
                {
                  key: 'orders',
                  label: `委托 (${runRecordCount(group, 'orders')})`,
                  children:
                    group.sources?.orders === false ? (
                      <div className="group-source-empty">委托记录未同步</div>
                    ) : (
                      <DataGrid
                        rows={group.orders || []}
                        columns={[
                          { key: 'instrument', title: '品种', width: 200 },
                          { key: 'side', title: '方向', render: valueLabel },
                          { key: 'quantity', title: '数量' },
                          {
                            key: 'price',
                            title: '委托价',
                            render: formatNumber,
                          },
                          { key: 'status', title: '状态', render: valueLabel },
                          { key: 'id', title: '订单号', width: 250 },
                        ]}
                      />
                    ),
                },
                {
                  key: 'fills',
                  label: `成交 (${runRecordCount(group, 'fills')})`,
                  children:
                    group.sources?.fills === false ? (
                      <div className="group-source-empty">成交记录未同步</div>
                    ) : (
                      <DataGrid
                        rows={group.fills || []}
                        columns={[
                          { key: 'time', title: '时间', render: timeOf },
                          { key: 'instrument', title: '品种', width: 200 },
                          { key: 'side', title: '方向', render: valueLabel },
                          { key: 'quantity', title: '数量' },
                          {
                            key: 'price',
                            title: '成交价',
                            render: formatNumber,
                          },
                          {
                            key: 'fee',
                            title: '手续费 / USDT',
                            render: (value) => number(value, 6),
                            width: 180,
                          },
                        ]}
                      />
                    ),
                },
                {
                  key: 'alpha',
                  label: 'Alpha 与板块',
                  children: (
                    <>
                      <DataGrid
                        rows={group.alpha}
                        height={180}
                        columns={[
                          { key: 'name', title: '组成', width: 180 },
                          { key: 'rule', title: '规则', width: 320 },
                          { key: 'source', title: '来源' },
                          { key: 'asOf', title: '信号日期', width: 180 },
                        ]}
                      />
                      <DataGrid
                        rows={group.strategies || []}
                        height={200}
                        columns={[
                          { key: 'id', title: '板块', width: 240 },
                          { key: 'instrument', title: '覆盖', width: 260 },
                          {
                            key: 'pnl',
                            title: '收益 / USDT',
                            render: formatNumber,
                          },
                          { key: 'mode', title: '状态', render: valueLabel },
                        ]}
                      />
                    </>
                  ),
                },
                {
                  key: 'version',
                  label: '版本与参数',
                  children: (
                    <>
                      <DataGrid
                        rows={group.version}
                        columns={[
                          { key: 'key', title: '版本项', width: 200 },
                          { key: 'value', title: '值', width: 700 },
                        ]}
                      />
                      <DataGrid
                        rows={group.runtimeConfig || []}
                        columns={[
                          { key: 'key', title: '参数', width: 240 },
                          { key: 'value', title: '值', width: 650 },
                        ]}
                      />
                    </>
                  ),
                },
                {
                  key: 'health',
                  label: '运行健康',
                  children: (
                    <>
                      <DataGrid
                        rows={group.systems || []}
                        height={180}
                        columns={[
                          { key: 'label', title: '模块' },
                          { key: 'level', title: '状态', render: valueLabel },
                          { key: 'detail', title: '详情', width: 600 },
                        ]}
                      />
                      <DataGrid
                        rows={group.exceptions || []}
                        height={200}
                        columns={[
                          { key: 'severity', title: '级别' },
                          { key: 'title', title: '异常', width: 240 },
                          { key: 'detail', title: '详情', width: 500 },
                        ]}
                      />
                    </>
                  ),
                },
              ]}
            />
          </section>
          <p className="group-source-note">
            {group.accountId || '读取执行账户'} · {executionModeLabel(group)} ·{' '}
            {group.description}
          </p>
        </>
      ) : (
        <div className="empty-state">正在读取策略组</div>
      )}
    </div>
  );
}
