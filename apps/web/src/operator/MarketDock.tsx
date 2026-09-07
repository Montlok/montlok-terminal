import { LineChartOutlined } from '@ant-design/icons';
import { useModel } from '@umijs/max';
import { Button, Drawer, Modal, Select, Space } from 'antd';
import {
  lazy,
  memo,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from 'react';
import {
  executionModeLabel,
  statusLabel,
  strategyDisplayName,
} from '../pages/StrategyGroups/groupModel';
import { api, number, type Row, timeOf } from './api';
import { instrumentUnits, marketNumber } from './instrumentFormat';
import { MarketWorkspace } from './MarketWorkspace';
import { OrderBook, RecentTrades } from './OrderBook';
import {
  primaryStrategyInstrument,
  strategyInstruments,
} from './strategyMarketModel';
import { useMarket } from './useMarket';
import { usePoll } from './usePoll';
import { Watchlist } from './Watchlist';

const BARS = ['1m', '5m', '15m', '1H', '4H', '1D'];
const OrderTicket = lazy(() =>
  import('./OrderTicket').then((module) => ({ default: module.OrderTicket })),
);

export const MarketDock = memo(function MarketDock({
  compact = false,
  onToggle,
}: {
  compact?: boolean;
  onToggle?: () => void;
}) {
  const { error, selectedGroup, selectedRuns } = useModel(
    'operator',
    (model) => ({
      error: model.error,
      selectedGroup: model.selectedGroup,
      selectedRuns: model.selectedRuns,
    }),
  );
  const selectedRun = selectedRuns[selectedGroup] || '';
  const selection = `${selectedGroup}/${selectedRun}`;
  const [strategySnapshot, setStrategy] = useState<{
    selection: string;
    value: Row;
  }>();
  const strategy =
    strategySnapshot?.selection === selection
      ? strategySnapshot.value
      : undefined;
  const loadStrategy = useCallback(
    async (signal: AbortSignal) => {
      const suffix = selectedRun
        ? `?runId=${encodeURIComponent(selectedRun)}&view=market`
        : '?view=market';
      try {
        const value = await api(
          `strategy-groups/${encodeURIComponent(selectedGroup)}${suffix}`,
        );
        if (!signal.aborted) setStrategy({ selection, value });
      } catch {
        if (!signal.aborted) setStrategy(undefined);
      }
    },
    [selectedGroup, selectedRun, selection],
  );
  usePoll(loadStrategy, 3000);
  const universe = useMemo(() => strategyInstruments(strategy), [strategy]);
  const [choice, setChoice] = useState({ group: '', symbol: '' });
  const instrument =
    choice.group === selectedGroup
      ? choice.symbol
      : primaryStrategyInstrument(universe);
  const setInstrument = (value: string) =>
    setChoice({ group: selectedGroup, symbol: value });
  useEffect(() => {
    if (universe.length && choice.group !== selectedGroup)
      setChoice({
        group: selectedGroup,
        symbol: primaryStrategyInstrument(universe),
      });
  }, [selectedGroup, universe, choice.group]);
  const [bar, setBar] = useState('5m');
  const mode = 'live';
  const [manualOpen, setManualOpen] = useState(false);
  const [universeOpen, setUniverseOpen] = useState(false);
  const market = useMarket(instrument, bar, mode);
  const symbols = [
    ...new Set([
      ...universe.map((row) => row.instrument),
      ...(instrument ? [instrument] : []),
    ]),
  ];
  const ticker = market.ticker || {};
  const units = instrumentUnits(market.instrumentDefinition);
  const change =
    ticker.last && ticker.open24h
      ? (Number(ticker.last) / Number(ticker.open24h) - 1) * 100
      : undefined;

  return (
    <section
      className={`market-dock ${compact ? 'market-dock-compact' : ''}`}
      aria-label="常驻行情"
    >
      <div className="instrument-strip">
        <Select
          aria-label="交易品种"
          showSearch
          className="dock-instrument-select"
          value={instrument}
          placeholder="选择观察标的"
          onChange={setInstrument}
          options={symbols.map((value) => ({
            label: value.replace('-', '/'),
            value,
          }))}
        />
        <strong
          className={`last-price ${change !== undefined && change < 0 ? 'negative' : 'positive'}`}
        >
          {marketNumber(ticker.last, market.instrumentDefinition?.tickSz)}
        </strong>
        <span
          className={
            change !== undefined && change < 0 ? 'negative' : 'positive'
          }
        >
          {change === undefined
            ? '—'
            : `${change > 0 ? '+' : ''}${change.toFixed(2)}%`}
        </span>
        <div className="market-stat">
          <span>24h 最高</span>
          <b>
            {marketNumber(ticker.high24h, market.instrumentDefinition?.tickSz)}
          </b>
        </div>
        <div className="market-stat">
          <span>24h 最低</span>
          <b>
            {marketNumber(ticker.low24h, market.instrumentDefinition?.tickSz)}
          </b>
        </div>
        <div className="market-stat">
          <span>
            {units.volumeLabel} / {units.volumeCurrency}
          </span>
          <b>{number(ticker.volCcy24h, 0)}</b>
        </div>
        <section className="market-strategy-strip" aria-label="策略收益与回撤">
          <span>
            {strategyDisplayName(strategy)} ·{' '}
            {strategy ? executionModeLabel(strategy) : '读取中'}
          </span>
          <b
            className={
              Number(strategy?.metrics?.pnl) < 0 ? 'negative' : 'positive'
            }
          >
            {strategy?.sources?.accounting === false
              ? '收益待核对'
              : `PnL ${number(strategy?.metrics?.pnl, 3)} USDT`}
          </b>
          <small>
            收益率 {number(strategy?.metrics?.returnPct, 3)}% · 回撤{' '}
            {number(strategy?.metrics?.maxDrawdownPct, 3)}%
          </small>
        </section>
        <Button
          className="universe-toggle"
          type="text"
          onClick={() => setUniverseOpen(true)}
        >
          策略标的 · {universe.length}
        </Button>
        <Button
          type="text"
          disabled={!instrument}
          onClick={() => setManualOpen(true)}
        >
          手动委托
        </Button>
        <Button type="text" onClick={onToggle} aria-expanded={!compact}>
          {compact ? '展开行情' : '收起行情'}
        </Button>
      </div>
      <section
        className="strategy-market-context"
        aria-label="当前策略交易范围"
      >
        <strong>
          {strategy?.composition?.universeLabel || '策略交易范围'}
        </strong>
        <span>
          {strategy?.composition?.description ||
            strategy?.description ||
            '读取策略配置'}
        </span>
        <span>
          {universe.length} 个标的 ·{' '}
          {universe.filter((row) => Math.abs(Number(row.quantity)) > 0).length}{' '}
          项持仓
        </span>
        <span>成交 {strategy?.fillsTotal ?? '—'} 笔</span>
        <span>
          {strategy?.status ? statusLabel(strategy.status) : '读取运行状态'}
        </span>
        {strategy?.allocation?.phase === 'ALLOCATED' && (
          <span className="positive">初始调仓完成</span>
        )}
      </section>
      {(error || market.error) && (
        <div className="status-message">{error || market.error}</div>
      )}
      <div className="trading-grid">
        <div className="dock-watchlist">
          <Watchlist
            selected={instrument}
            onSelect={setInstrument}
            universe={universe}
            scopeKey={selectedGroup}
            groupName={strategyDisplayName(strategy)}
          />
        </div>
        <section className="chart-panel panel">
          <div className="panel-heading">
            <div className="workspace-tabs">
              <strong>图表</strong>
              <span>{instrument}</span>
            </div>
            <span className="muted">TradingView</span>
          </div>
          <div className="chart-toolbar">
            <Space size={0}>
              {BARS.map((value) => (
                <Button
                  key={value}
                  type={bar === value ? 'primary' : 'text'}
                  onClick={() => setBar(value)}
                >
                  {value}
                </Button>
              ))}
            </Space>
            <span className="muted dock-chart-hint">
              <LineChartOutlined /> K 线 · 成交量
            </span>
          </div>
          <div className="dock-chart-content">
            <MarketWorkspace
              market={market}
              instrument={instrument}
              mode={mode}
            />
          </div>
          <div className="chart-status">
            <span>
              <span
                className={`connection-dot ${market.connected ? 'online' : ''}`}
              />
              {market.connected ? '实时' : '连接中'}
            </span>
            <span>行情接收 {timeOf(market.receivedAt)}</span>
          </div>
        </section>
        <section className="book-panel panel">
          <div className="panel-heading">
            <strong>订单簿</strong>
            <span className="muted">5 档</span>
          </div>
          <div className="dock-book-ladder">
            <OrderBook
              book={market.book}
              last={ticker.last}
              definition={market.instrumentDefinition}
            />
          </div>
          <div className="panel-heading latest-heading">
            <strong>最新成交</strong>
            <span className="muted">市场</span>
          </div>
          <RecentTrades
            trades={market.trades}
            definition={market.instrumentDefinition}
          />
        </section>
      </div>
      <Drawer
        rootClassName="operator-dialog bp6-dark strategy-market-drawer"
        title="策略标的与持仓"
        open={universeOpen}
        onClose={() => setUniverseOpen(false)}
        size={440}
        destroyOnHidden
      >
        <Watchlist
          selected={instrument}
          onSelect={(value) => {
            setInstrument(value);
            setUniverseOpen(false);
          }}
          universe={universe}
          scopeKey={selectedGroup}
          groupName={strategyDisplayName(strategy)}
        />
      </Drawer>
      <Modal
        title="手动委托"
        open={manualOpen}
        onCancel={() => setManualOpen(false)}
        footer={null}
        width={360}
        destroyOnHidden
      >
        <Suspense
          fallback={<div className="workspace-loading">加载委托表单</div>}
        >
          <OrderTicket
            instrument={instrument}
            last={ticker.last}
            marketMode={mode}
          />
        </Suspense>
      </Modal>
    </section>
  );
});
