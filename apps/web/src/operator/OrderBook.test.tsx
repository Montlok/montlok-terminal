import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import {
  incrementDigits,
  instrumentUnits,
  marketNumber,
} from './instrumentFormat';
import { OrderBook, RecentTrades } from './OrderBook';

describe('instrument-defined market precision and units', () => {
  afterEach(cleanup);
  it('keeps low-price tick and lot precision and non-USDT quote currency', () => {
    const definition = {
      instType: 'SPOT',
      baseCcy: 'PEPE',
      quoteCcy: 'USDC',
      tickSz: '0.000000001',
      lotSz: '0.00000001',
    };
    render(
      <OrderBook
        definition={definition}
        book={{ asks: [['0.000008123', '0.00000012']], bids: [] }}
      />,
    );
    expect(screen.getByText('价格 (USDC)')).toBeInTheDocument();
    expect(screen.getByText('数量 (PEPE)')).toBeInTheDocument();
    expect(screen.getByText('0.000008123')).toBeInTheDocument();
    expect(screen.getByText('0.00000012')).toBeInTheDocument();
  });
  it('labels derivative size as contracts, separate from base and quote', () => {
    const definition = {
      instType: 'SWAP',
      uly: 'BTC-USD',
      settleCcy: 'BTC',
      tickSz: '0.1',
      lotSz: '1',
      ctVal: '100',
      ctValCcy: 'USD',
    };
    render(
      <RecentTrades
        definition={definition}
        trades={[{ tradeId: '1', px: '97000.1', sz: '3', ts: '1720000000000' }]}
      />,
    );
    expect(screen.getByText('价格 (USD)')).toBeInTheDocument();
    expect(screen.getByText('数量 (张)')).toBeInTheDocument();
    expect(instrumentUnits(definition).contractValue).toBe('每张 100 USD');
  });
  it('preserves exchange precision and does not invent units when metadata is absent', () => {
    expect(marketNumber('10000.000000000123')).toBe('10,000.000000000123');
    expect(instrumentUnits().quote).toBe('单位待确认');
    expect(incrementDigits('1e-8')).toBe(8);
    expect(incrementDigits('0.0100')).toBe(2);
    expect(incrementDigits('0')).toBeUndefined();
    expect(
      instrumentUnits({ instType: 'OPTION', uly: 'BTC-USD', settleCcy: 'BTC' })
        .quote,
    ).toBe('BTC');
  });
});
