import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useMarket } from '../../operator/useMarket';
import MarketData from './index';

const route = vi.hoisted(() => ({
  pathname: '/market/prices/candles',
  search: '',
}));
vi.mock('@umijs/max', () => ({ useLocation: () => route }));
vi.mock('../../operator/useMarket', () => ({
  useMarket: vi.fn(() => ({
    ticker: { last: '110', open24h: '100' },
    book: { asks: [['111', '2']], bids: [['109', '3']] },
    candles: [{ time: 1788600000, close: 110 }],
    trades: [],
    connected: true,
  })),
}));
vi.mock('../../operator/MarketWorkspace', () => ({
  MarketWorkspace: ({ market }: { market: { candles: unknown[] } }) => (
    <div>K线观测数 {market.candles.length}</div>
  ),
}));
vi.mock('../../operator/DepthChart', () => ({
  default: () => <div>累计深度画布</div>,
}));
vi.mock('../../operator/OrderBook', () => ({
  OrderBook: () => <div>盘口阶梯</div>,
  RecentTrades: () => <div>成交列表</div>,
}));
vi.mock('../../operator/Watchlist', () => ({
  Watchlist: ({ onSelect }: { onSelect: (symbol: string) => void }) => (
    <button type="button" onClick={() => onSelect('ETH-USDT')}>
      选择 ETH
    </button>
  ),
}));
vi.mock('../ApiWorkbench', () => ({
  default: () => (
    <form aria-label="原有接口查询">
      <span>
        {new URLSearchParams(route.search).get('operation') ||
          '可选择全部关联接口'}
      </span>
    </form>
  ),
}));

beforeEach(() => {
  route.search = '';
});
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});
describe('dedicated market pages', () => {
  it('loads public data on entry and changing a period resubscribes', () => {
    route.pathname = '/market/prices/candles';
    render(<MarketData />);
    expect(useMarket).toHaveBeenLastCalledWith('BTC-USDT', '5m', 'live');
    expect(screen.getByText('K线观测数 1')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '1H' }));
    expect(useMarket).toHaveBeenLastCalledWith('BTC-USDT', '1H', 'live');
    expect(
      screen.queryByRole('button', { name: /买入|卖出/ }),
    ).not.toBeInTheDocument();
  });
  it('renders depth directly without requiring an API form submission', () => {
    route.pathname = '/market/prices/depth';
    render(<MarketData />);
    expect(screen.getByText('累计深度画布')).toBeInTheDocument();
    expect(screen.queryByText('K线观测数 1')).not.toBeInTheDocument();
    expect(screen.getByText('1 档买盘 / 1 档卖盘')).toBeInTheDocument();
  });
  it('quotes watchlist selection updates the linked chart instrument', () => {
    route.pathname = '/market/prices/quotes';
    render(<MarketData />);
    fireEvent.click(screen.getByRole('button', { name: '选择 ETH' }));
    expect(useMarket).toHaveBeenLastCalledWith('ETH-USDT', '5m', 'live');
  });
  it('keeps original API forms behind a disclosure by default', async () => {
    const { container } = render(<MarketData />);
    expect(
      screen.queryByRole('form', { name: '原有接口查询' }),
    ).not.toBeInTheDocument();
    expect(screen.getByText('接口查询')).toBeInTheDocument();
    const disclosure = container.querySelector('details');
    if (!disclosure) throw new Error('Missing API disclosure');
    disclosure.open = true;
    fireEvent(disclosure, new Event('toggle'));
    expect(
      await screen.findByRole('form', { name: '原有接口查询' }),
    ).toBeInTheDocument();
  });
  it('opens and preserves a linked operation on load and after route changes', async () => {
    route.search = '?operation=rest%3AGET%20%2Fapi%2Fv5%2Fmarket%2Fcandles';
    const { container, rerender } = render(<MarketData />);
    expect(container.querySelector('details')).toHaveAttribute('open');
    expect(
      await screen.findByText('rest:GET /api/v5/market/candles'),
    ).toBeInTheDocument();
    route.search = '';
    rerender(<MarketData />);
    expect(container.querySelector('details')).not.toHaveAttribute('open');
    route.pathname = '/market/prices/depth';
    route.search = '?operation=mcp%3Amarket_get_orderbook';
    rerender(<MarketData />);
    expect(container.querySelector('details')).toHaveAttribute('open');
    expect(
      await screen.findByText('mcp:market_get_orderbook'),
    ).toBeInTheDocument();
  });
});
