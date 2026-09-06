import { useLocation } from '@umijs/max';
import { Alert, Button, Select } from 'antd';
import { lazy, Suspense, useEffect, useState } from 'react';
import { number, timeOf } from '../../operator/api';
import DepthChart from '../../operator/DepthChart';
import { instrumentUnits, marketNumber } from '../../operator/instrumentFormat';
import { MarketWorkspace } from '../../operator/MarketWorkspace';
import { OrderBook, RecentTrades } from '../../operator/OrderBook';
import { useMarket } from '../../operator/useMarket';
import { Watchlist } from '../../operator/Watchlist';
import './marketData.css';

const ApiWorkbench = lazy(() => import('../ApiWorkbench'));
const BARS = ['1m', '5m', '15m', '1H', '4H', '1D'];
const SYMBOLS = [
  'BTC-USDT',
  'ETH-USDT',
  'SOL-USDT',
  'OKB-USDT',
  'XNVDA-USDT',
  'BTC-USDT-SWAP',
];

export default function MarketData() {
  const { pathname, search: query } = useLocation();
  const [advanced, setAdvanced] = useState(() =>
    new URLSearchParams(query).has('operation'),
  );
  useEffect(() => {
    setAdvanced(new URLSearchParams(query).has('operation'));
  }, [pathname, query]);
  const depth = pathname.endsWith('/depth');
  const quotes = pathname.endsWith('/quotes');
  const [instrument, setInstrument] = useState('BTC-USDT');
  const [search, setSearch] = useState('');
  const [bar, setBar] = useState('5m');
  const market = useMarket(instrument, bar, 'live');
  const ticker = market.ticker || {};
  const units = instrumentUnits(market.instrumentDefinition);
  const change =
    Number(ticker.open24h) > 0 && Number(ticker.last) > 0
      ? (Number(ticker.last) / Number(ticker.open24h) - 1) * 100
      : undefined;
  const candidate = search.toUpperCase().replace('/', '-').trim();
  const symbols = [
    ...new Set([
      ...SYMBOLS,
      instrument,
      ...(/^[A-Z0-9]+-[A-Z0-9]+(?:-SWAP)?$/.test(candidate) ? [candidate] : []),
    ]),
  ];
  return (
    <div className="management-page market-data-page">
      <div className="page-heading">
        <h1>{depth ? '订单簿' : quotes ? '行情' : 'K 线'}</h1>
        <span className="market-data-source">
          OKX 公开行情 · {market.connected ? '已连接' : '连接中'} ·{' '}
          {timeOf(market.receivedAt)}
        </span>
      </div>
      <section className="market-data-toolbar">
        <Select
          aria-label="行情品种"
          value={instrument}
          onChange={setInstrument}
          showSearch={{ onSearch: setSearch }}
          options={symbols.map((value) => ({
            value,
            label: value.replace('-', '/'),
          }))}
        />
        <strong className="market-data-price">
          {marketNumber(ticker.last, market.instrumentDefinition?.tickSz)}
        </strong>
        <span
          className={
            change === undefined ? '' : change < 0 ? 'negative' : 'positive'
          }
        >
          {change === undefined
            ? '—'
            : `${change >= 0 ? '+' : ''}${number(change)}%`}
        </span>
        <span>
          24h 高{' '}
          <b>
            {marketNumber(ticker.high24h, market.instrumentDefinition?.tickSz)}
          </b>
        </span>
        <span>
          24h 低{' '}
          <b>
            {marketNumber(ticker.low24h, market.instrumentDefinition?.tickSz)}
          </b>
        </span>
        <span>
          {units.volumeLabel}{' '}
          <b>
            {number(ticker.volCcy24h)} {units.volumeCurrency}
          </b>
        </span>
      </section>
      {market.error && (
        <Alert type="error" title={String(market.error)} showIcon />
      )}
      <details
        className="market-data-api"
        open={advanced}
        onToggle={(event) => setAdvanced(event.currentTarget.open)}
      >
        <summary>接口查询</summary>
        {advanced && (
          <Suspense fallback={<div className="empty-state">加载接口</div>}>
            <ApiWorkbench />
          </Suspense>
        )}
      </details>
      <div className={`market-data-grid${quotes ? ' market-data-quotes' : ''}`}>
        {quotes && (
          <aside className="panel">
            <Watchlist selected={instrument} onSelect={setInstrument} />
          </aside>
        )}
        <section className="panel market-data-primary">
          <div className="panel-heading">
            <strong>
              {instrument} · {depth ? '累计深度' : '价格与成交量'}
            </strong>
            <span>
              {depth
                ? `${market.book?.bids?.length || 0} 档买盘 / ${market.book?.asks?.length || 0} 档卖盘`
                : 'TradingView'}
            </span>
          </div>
          {depth ? (
            <DepthChart book={market.book || {}} height={500} />
          ) : (
            <>
              <div className="market-data-bars">
                {BARS.map((value) => (
                  <Button
                    key={value}
                    type={bar === value ? 'primary' : 'text'}
                    size="small"
                    onClick={() => setBar(value)}
                  >
                    {value}
                  </Button>
                ))}
              </div>
              <MarketWorkspace
                market={market}
                instrument={instrument}
                mode="live"
              />
            </>
          )}
        </section>
        <aside className="panel market-data-book">
          <div className="panel-heading">
            <strong>订单簿</strong>
            <span>{timeOf(Number(market.book?.ts))}</span>
          </div>
          <OrderBook
            book={market.book}
            last={ticker.last}
            definition={market.instrumentDefinition}
          />
          <div className="panel-heading">
            <strong>最新成交</strong>
          </div>
          <RecentTrades
            trades={market.trades}
            limit={12}
            definition={market.instrumentDefinition}
          />
        </aside>
      </div>
    </div>
  );
}
