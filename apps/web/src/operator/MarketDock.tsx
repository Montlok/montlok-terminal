import { Icon } from '@blueprintjs/core';
import { useModel } from '@umijs/max';
import { Button, Modal, Select, Space } from 'antd';
import { memo, useCallback, useState } from 'react';
import { api, number, type Row, timeOf } from './api';
import { instrumentUnits, marketNumber } from './instrumentFormat';
import { MarketWorkspace } from './MarketWorkspace';
import { OrderBook, RecentTrades } from './OrderBook';
import { OrderTicket } from './OrderTicket';
import { useMarket } from './useMarket';
import { usePoll } from './usePoll';
import { Watchlist } from './Watchlist';

const BARS = ['1m', '5m', '15m', '1H', '4H', '1D'];

export const MarketDock = memo(function MarketDock({
  compact = false,
  onToggle,
}: {
  compact?: boolean;
  onToggle?: () => void;
}) {
  const { paper, error, selectedGroup, selectedRuns } = useModel('operator');
  const selectedRun = selectedRuns[selectedGroup] || '';
  const [strategy, setStrategy] = useState<Row>();
  const loadStrategy = useCallback(async (signal: AbortSignal) => {
    const suffix = selectedRun ? `?runId=${encodeURIComponent(selectedRun)}` : '';
    try {
      const value = await api(`strategy-groups/${encodeURIComponent(selectedGroup)}${suffix}`);
      if (!signal.aborted) setStrategy(value);
    } catch {
      if (!signal.aborted) setStrategy(undefined);
    }
  }, [selectedGroup, selectedRun]);
  usePoll(loadStrategy, 3000);
  const [instrument, setInstrument] = useState('BTC-USDT');
  const [bar, setBar] = useState('5m');
  const mode = 'live';
  const [manualOpen, setManualOpen] = useState(false);
  const market = useMarket(instrument, bar, mode);
  const symbols = [
    ...new Set([
      'BTC-USDT',
      'ETH-USDT',
      ...(paper?.positions || []).map((row: Row) =>
        row.instrument.replace('.OKX', ''),
      ),
      instrument,
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
        <div className="market-strategy-strip" aria-label="策略收益与回撤">
          <span>{strategy?.name || '策略'}</span>
          <b className={Number(strategy?.metrics?.pnl) < 0 ? 'negative' : 'positive'}>
            PnL {number(strategy?.metrics?.pnl, 3)} USDT
          </b>
          <small>收益率 {number(strategy?.metrics?.returnPct, 3)}% · 回撤 {number(strategy?.metrics?.maxDrawdownPct, 3)}%</small>
        </div>
        <span className="dock-market-select muted">公开行情</span>
        <Button type="text" onClick={() => setManualOpen(true)}>
          手动委托
        </Button>
        <Button type="text" onClick={onToggle} aria-expanded={!compact}>
          {compact ? '展开行情' : '收起行情'}
        </Button>
      </div>
      {(error || market.error) && (
        <div className="status-message">{error || market.error}</div>
      )}
      <div className="trading-grid">
        <div className="dock-watchlist">
          <Watchlist selected={instrument} onSelect={setInstrument} />
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
              <Icon icon="chart" size={13} /> K 线 · 成交量
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
      <Modal
        title="手动委托"
        open={manualOpen}
        onCancel={() => setManualOpen(false)}
        footer={null}
        width={360}
        destroyOnHidden
      >
        <OrderTicket
          instrument={instrument}
          last={ticker.last}
          marketMode={mode}
        />
      </Modal>
    </section>
  );
});
