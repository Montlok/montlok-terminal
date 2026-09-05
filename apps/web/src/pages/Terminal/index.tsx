import { Icon } from '@blueprintjs/core';
import { useModel } from '@umijs/max';
import { Button, Select, Space, Tabs } from 'antd';
import { useMemo, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';
import { DataGrid, type GridColumn } from '../../operator/DataGrid';
import { valueLabel } from '../../operator/labels';
import { MarketWorkspace } from '../../operator/MarketWorkspace';
import { OrderBook, RecentTrades } from '../../operator/OrderBook';
import { OrderTicket } from '../../operator/OrderTicket';
import { useMarket } from '../../operator/useMarket';
import { Watchlist } from '../../operator/Watchlist';

const BARS = ['1m', '5m', '15m', '1H', '4H', '1D'];

// Static column sets live at module scope so the memoized grids never see new props per tick.
const FILL_COLUMNS: GridColumn[] = [
  { key: 'fillTime', title: '时间', width: 105, render: timeOf },
  { key: 'instId', title: '交易品种' },
  { key: 'side', title: '方向', width: 90 },
  { key: 'fillPx', title: '成交价', render: (value) => number(value, 6) },
  { key: 'fillSz', title: '成交数量' },
  { key: 'fee', title: '手续费' },
  { key: 'feeCcy', title: '费用币种' },
  { key: 'ordId', title: '订单号', width: 215 },
];
const ASSET_COLUMNS: GridColumn[] = [
  { key: 'ccy', title: '币种' },
  {
    key: 'cashBal',
    title: '余额',
    width: 210,
    render: (value) => number(value, 10),
  },
  {
    key: 'availBal',
    title: '可用',
    width: 210,
    render: (value) => number(value, 10),
  },
  { key: 'frozenBal', title: '冻结', render: (value) => number(value, 8) },
  { key: 'upl', title: '浮动收益' },
];
const PAPER_COLUMNS: GridColumn[] = [
  { key: 'instrument', title: '品种', width: 220 },
  { key: 'quantity', title: '数量', render: (value) => number(value, 6) },
  { key: 'averagePrice', title: '均价', render: (value) => number(value) },
  { key: 'markPrice', title: '估值价格', render: (value) => number(value) },
  {
    key: 'unrealizedPnl',
    title: '未实现 PnL',
    render: (value) => (
      <span className={value < 0 ? 'negative' : 'positive'}>
        {number(value)}
      </span>
    ),
  },
];

export default function Terminal() {
  const { account, paper, error } = useModel('operator');
  const [ticket, setTicket] = useState<Row>();
  const [actionError, setActionError] = useState('');
  const [instrument, setInstrument] = useState('BTC-USDT');
  const [bar, setBar] = useState('5m');
  const [mode, setMode] = useState('live');
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
  const change =
    ticker.last && ticker.open24h
      ? (Number(ticker.last) / Number(ticker.open24h) - 1) * 100
      : undefined;
  const cancelable = account.mode === 'demo';
  const orderColumns = useMemo<GridColumn[]>(
    () => [
      { key: 'instId', title: '交易品种' },
      { key: 'side', title: '方向', width: 90 },
      { key: 'ordType', title: '类型', width: 100, render: valueLabel },
      { key: 'px', title: '价格', render: (value) => number(value, 6) },
      { key: 'sz', title: '委托数量' },
      { key: 'accFillSz', title: '已成交' },
      { key: 'state', title: '状态', render: valueLabel },
      { key: 'ordId', title: '订单号', width: 215 },
      {
        key: 'action',
        title: '操作',
        width: 85,
        render: (_, row) => (
          <Button
            type="text"
            disabled={!cancelable}
            onClick={() =>
              void api('prepare', {
                kind: 'mcp',
                name: 'spot_cancel_order',
                arguments: { instId: row.instId, ordId: row.ordId },
              })
                .then(setTicket)
                .catch((reason) => setActionError(String(reason)))
            }
          >
            撤单
          </Button>
        ),
      },
    ],
    [cancelable],
  );
  return (
    <div className="terminal-page">
      <section className="instrument-strip">
        <Select
          aria-label="交易品种"
          showSearch
          value={instrument}
          onChange={setInstrument}
          options={symbols.map((value) => ({
            label: value.replace('-', '/'),
            value,
          }))}
          style={{ width: 190 }}
        />
        <strong
          className={`last-price ${change !== undefined && change < 0 ? 'negative' : 'positive'}`}
        >
          {number(ticker.last, 4)}
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
          <b>{number(ticker.high24h, 4)}</b>
        </div>
        <div className="market-stat">
          <span>24h 最低</span>
          <b>{number(ticker.low24h, 4)}</b>
        </div>
        <div className="market-stat">
          <span>24h 成交额 / USDT</span>
          <b>{number(ticker.volCcy24h, 0)}</b>
        </div>
        <Select
          aria-label="行情环境"
          value={mode}
          onChange={setMode}
          options={[
            { label: '公开行情', value: 'live' },
            { label: '模拟盘行情', value: 'demo' },
          ]}
          style={{ width: 150, marginLeft: 'auto' }}
        />
      </section>
      {(error || market.error || actionError) && (
        <div className="status-message">
          {error || market.error || actionError}
        </div>
      )}
      <div className="trading-grid">
        <Watchlist selected={instrument} onSelect={setInstrument} />
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
            <span className="muted">
              <Icon icon="chart" size={13} /> K 线 · 成交量
            </span>
          </div>
          <MarketWorkspace
            market={market}
            instrument={instrument}
            mode={mode}
          />
          <div className="chart-status">
            <span>
              <span
                className={`connection-dot ${market.connected ? 'online' : ''}`}
              />{' '}
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
          <OrderBook book={market.book} last={ticker.last} />
          <div className="panel-heading latest-heading">
            <strong>最新成交</strong>
            <span className="muted">市场</span>
          </div>
          <RecentTrades trades={market.trades} />
        </section>
        <OrderTicket
          instrument={instrument}
          last={ticker.last}
          marketMode={mode}
        />
      </div>
      <section className="blotter panel">
        <Tabs
          size="small"
          items={[
            {
              key: 'orders',
              label: `当前委托 (${account.orders.length})`,
              children: (
                <DataGrid rows={account.orders} columns={orderColumns} />
              ),
            },
            {
              key: 'fills',
              label: `成交 (${account.fills.length})`,
              children: (
                <DataGrid rows={account.fills} columns={FILL_COLUMNS} />
              ),
            },
            {
              key: 'assets',
              label: '资产',
              children: (
                <DataGrid rows={account.balances} columns={ASSET_COLUMNS} />
              ),
            },
            {
              key: 'paper',
              label: '本地模拟',
              children: (
                <>
                  <div className="paper-context">
                    {paper?.runId} · 净值 {number(paper?.risk?.nav)} USDT
                  </div>
                  <DataGrid
                    rows={paper?.positions || []}
                    columns={PAPER_COLUMNS}
                  />
                </>
              ),
            },
          ]}
        />
      </section>
      <footer className="terminal-status">
        <span>{market.connected ? '行情已连接' : '行情连接中'}</span>
        <span>
          账户更新 {timeOf(account.updatedAt)} ·{' '}
          {account.mode === 'demo' ? '模拟盘' : '实盘 · 只读'}
        </span>
      </footer>
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={() => setTicket(undefined)}
      />
    </div>
  );
}
