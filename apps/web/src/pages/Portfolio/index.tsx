import { ReloadOutlined } from '@ant-design/icons';
import { history, useModel } from '@umijs/max';
import { Alert, Button, Select } from 'antd';
import { useCallback, useMemo, useRef, useState } from 'react';
import { api, number } from '../../operator/api';
import { TimeSeriesChart } from '../../operator/TimeSeriesChart';
import { usePoll } from '../../operator/usePoll';
import {
  executionModeLabel,
  type GroupEquity,
  runDateTime,
  type StrategyGroup,
  stateWarning,
  statusLabel,
  strategyDisplayName,
} from '../StrategyGroups/groupModel';
import { PortfolioBars } from './PortfolioBars';
import {
  matchingPortfolio,
  positionAnalytics,
  type RuntimeGroups,
  type RuntimeRun,
} from './portfolioModel';
import './portfolio.css';

type Snapshot = {
  selection: string;
  group: StrategyGroup;
  equity: GroupEquity;
};

export default function Portfolio() {
  const { selectedGroup, setSelectedGroup, selectedRuns, setSelectedRuns } =
    useModel('operator', (model) => ({
      selectedGroup: model.selectedGroup,
      setSelectedGroup: model.setSelectedGroup,
      selectedRuns: model.selectedRuns,
      setSelectedRuns: model.setSelectedRuns,
    }));
  const selectedRun = selectedRuns[selectedGroup] || '';
  const selection = `${selectedGroup}/${selectedRun}`;
  const current = useRef(selection);
  current.current = selection;
  const [groups, setGroups] = useState<StrategyGroup[]>([]);
  const [runs, setRuns] = useState<RuntimeRun[]>([]);
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [error, setError] = useState('');
  const load = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const runQuery = selectedRun
          ? `runId=${encodeURIComponent(selectedRun)}`
          : '';
        const detailQuery = `?${[runQuery, 'view=summary'].filter(Boolean).join('&')}`;
        const runSuffix = runQuery ? `?${runQuery}` : '';
        const [listing, group, equity, runtime] = await Promise.all([
          api<{ groups: StrategyGroup[] }>('strategy-groups'),
          api<StrategyGroup>(`strategy-groups/${selectedGroup}${detailQuery}`),
          api<GroupEquity>(
            `strategy-groups/${selectedGroup}/equity${runSuffix}`,
          ),
          api<RuntimeGroups>(`strategy-groups/${selectedGroup}/runtime`).catch(
            () => undefined,
          ),
        ]);
        if (signal?.aborted || current.current !== selection) return;
        if (!matchingPortfolio(group, equity, selectedGroup, selectedRun))
          throw new Error('运行实例数据不一致，请刷新');
        setGroups(listing.groups);
        setRuns(
          runtime?.groups?.find((item) => item.groupId === selectedGroup)
            ?.runs || [],
        );
        setSnapshot({ selection, group, equity });
        setError('');
      } catch (reason) {
        if (!signal?.aborted && current.current === selection)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
    },
    [selectedGroup, selectedRun, selection],
  );
  usePoll(load, 5000);
  const data = snapshot?.selection === selection ? snapshot : undefined;
  const group = data?.group;
  const equity = data?.equity;
  const original = groups.find((item) => item.id === selectedGroup);
  const analytics = useMemo(
    () =>
      positionAnalytics(
        group?.positions || [],
        !!group?.runId &&
          group.sources?.view !== false &&
          group.health.detailAvailable !== false,
      ),
    [group],
  );
  const warning = stateWarning(group?.status || '');
  const basis = analytics.basis === 'sector' ? '按板块' : '按品种';
  const orderedGroups = useMemo(
    () =>
      [...groups].sort(
        (left, right) =>
          Number(right.mode === 'live') - Number(left.mode === 'live'),
      ),
    [groups],
  );

  return (
    <div className="portfolio-page">
      <header className="portfolio-heading">
        <div>
          <h1>{group?.mode === 'live' ? '实盘组合' : '组合总览'}</h1>
          <span className="muted">
            {group
              ? `${group.accountId || '读取执行账户'} · ${executionModeLabel(group)}`
              : '读取运行实例'}
          </span>
        </div>
        <div className="portfolio-selectors">
          <Select
            aria-label="组合策略组"
            value={selectedGroup}
            onChange={setSelectedGroup}
            showSearch={{ optionFilterProp: 'label' }}
            options={orderedGroups.map((item) => ({
              value: item.id,
              label: strategyDisplayName(item),
            }))}
          />
          <Select
            aria-label="组合运行实例"
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
                label: original?.runId
                  ? `${original.managed ? '当前 / 最近一次' : '原始运行'} · ${original.runId}`
                  : '等待首次运行',
              },
              ...runs
                .filter(
                  (run) =>
                    run.groupId === selectedGroup &&
                    run.runId !== original?.runId,
                )
                .map((run) => ({
                  value: run.runId,
                  label: `${run.runId} · ${statusLabel(run.status)}`,
                })),
            ]}
          />
          <Button icon={<ReloadOutlined />} onClick={() => void load()}>
            刷新
          </Button>
        </div>
      </header>
      {error && <Alert type="error" title={error} showIcon />}
      <div className="portfolio-layout">
        <aside className="portfolio-group-list panel" aria-label="策略组列表">
          <div className="panel-heading">
            <strong>策略组</strong>
            <span className="muted">{groups.length}</span>
          </div>
          {orderedGroups.map((item) => {
            const displayed = item.id === selectedGroup && group ? group : item;
            return (
              <button
                type="button"
                className={`portfolio-group-item ${item.id === selectedGroup ? 'selected' : ''}`}
                aria-pressed={item.id === selectedGroup}
                key={item.id}
                onClick={() => setSelectedGroup(item.id)}
              >
                <strong>{strategyDisplayName(item)}</strong>
                <span>
                  {statusLabel(displayed.status)} ·{' '}
                  {executionModeLabel(displayed)}
                </span>
                <small>{displayed.runId || '等待首次运行'}</small>
              </button>
            );
          })}
          <Button
            type="text"
            block
            onClick={() => history.push('/strategies/resources/artifacts')}
          >
            模型与因子
          </Button>
        </aside>
        <section className="portfolio-main" aria-label="组合分析">
          {group ? (
            <>
              <section
                className="portfolio-run-context"
                aria-label="组合运行来源"
              >
                <strong>{strategyDisplayName(group)}</strong>
                <span>{statusLabel(group.status)}</span>
                <span>{group.runId || '等待首次运行'}</span>
                <span>信号 {group.signalAsOf || '读取中'}</span>
                <span>快照 {runDateTime(group.observedAt)} UTC+8</span>
              </section>
              {(warning ||
                group.status === 'stale' ||
                group.status === 'unavailable') && (
                <Alert
                  type={warning?.type || 'warning'}
                  title={
                    warning?.title ||
                    (group.status === 'stale'
                      ? '运行快照已超过 120 秒未更新'
                      : '运行数据连接中')
                  }
                  showIcon
                />
              )}
              {group.status === 'pending_validation' && (
                <Alert
                  type="info"
                  title="组合配置检查中"
                  description={group.capabilities.reason}
                />
              )}
              {group.sources?.accounting === false && (
                <Alert
                  type="warning"
                  title="收益数据待核对"
                  description="成交回报与持仓正在对齐，完成后更新净值与回撤。"
                  showIcon
                />
              )}
              <section
                className="portfolio-metrics panel"
                aria-label="组合指标"
              >
                {[
                  ['净值 / USDT', group.metrics.nav],
                  ['累计收益 / USDT', group.metrics.pnl],
                  ['收益率 / %', group.metrics.returnPct],
                  ['最大回撤 / %', group.metrics.maxDrawdownPct],
                  ['名义敞口 / USDT', analytics.grossExposure],
                  ['手续费 / USDT', group.metrics.fees],
                ].map(([label, value]) => (
                  <div key={String(label)}>
                    <span>{label}</span>
                    <strong>{number(value, 3)}</strong>
                  </div>
                ))}
              </section>
              {equity?.sourceAvailable === false && group.runId && (
                <Alert
                  type="warning"
                  title="历史曲线读取异常"
                  description={equity.sourceIssue || undefined}
                  showIcon
                />
              )}
              <div className="portfolio-series">
                <section className="panel">
                  <div className="panel-heading">
                    <strong>组合净值 / USDT</strong>
                    <span className="muted">
                      {equity?.sampleCount ?? 0} 条观测
                    </span>
                  </div>
                  <TimeSeriesChart
                    key={`${selection}/nav`}
                    label="组合净值"
                    points={equity?.points || []}
                    height={240}
                  />
                </section>
                <section className="panel">
                  <div className="panel-heading">
                    <strong>最大回撤 / %</strong>
                    <span className="muted">{executionModeLabel(group)}</span>
                  </div>
                  <TimeSeriesChart
                    key={`${selection}/drawdown`}
                    label="组合最大回撤"
                    points={equity?.drawdown || []}
                    height={240}
                    color="#f05b65"
                  />
                </section>
              </div>
              <div className="portfolio-distributions">
                <section className="panel">
                  <div className="panel-heading">
                    <strong>持仓名义敞口 / USDT</strong>
                    <span className="muted">{basis}</span>
                  </div>
                  <PortfolioBars
                    label="持仓名义敞口分布"
                    rows={analytics.exposure}
                    available={analytics.available}
                    missing={analytics.missingExposure}
                  />
                </section>
                <section className="panel">
                  <div className="panel-heading">
                    <strong>未实现 PnL 贡献 / USDT</strong>
                    <span className="muted">{basis}</span>
                  </div>
                  <PortfolioBars
                    label="未实现盈亏贡献"
                    rows={analytics.contribution}
                    signed
                    available={analytics.available}
                    missing={analytics.missingPnl}
                  />
                </section>
              </div>
              <footer className="portfolio-footer">
                <span>
                  {group.marketSource || executionModeLabel(group)} ·{' '}
                  {group.accountId}
                </span>
                <Button
                  type="text"
                  onClick={() => history.push('/strategies/groups/overview')}
                >
                  持仓、成交与运行详情
                </Button>
              </footer>
            </>
          ) : (
            <div className="portfolio-empty">正在读取所选运行实例</div>
          )}
        </section>
      </div>
    </div>
  );
}
