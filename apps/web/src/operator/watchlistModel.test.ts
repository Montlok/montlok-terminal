import { describe, expect, it } from 'vitest';
import {
  addSymbol,
  DEFAULT_WATCHLIST,
  loadWatchlist,
  normalizeSymbol,
  priceDigits,
  removeSymbol,
  saveWatchlist,
  WATCHLIST_KEY,
  WATCHLIST_LIMIT,
  watchRows,
} from './watchlistModel';

function memoryStorage(initial: Record<string, string> = {}) {
  const store = new Map(Object.entries(initial));
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
    store,
  };
}

describe('normalizeSymbol', () => {
  it('accepts spot, swap and dated instruments in any case', () => {
    expect(normalizeSymbol(' btc-usdt ')).toBe('BTC-USDT');
    expect(normalizeSymbol('eth/usdt')).toBe('ETH-USDT');
    expect(normalizeSymbol('BTC-USDT-SWAP')).toBe('BTC-USDT-SWAP');
    expect(normalizeSymbol('BTC-USD-240628')).toBe('BTC-USD-240628');
  });

  it('rejects malformed input', () => {
    expect(normalizeSymbol('')).toBeUndefined();
    expect(normalizeSymbol('BTCUSDT')).toBeUndefined();
    expect(normalizeSymbol('BTC-')).toBeUndefined();
    expect(normalizeSymbol('<script>')).toBeUndefined();
  });
});

describe('loadWatchlist / saveWatchlist', () => {
  it('falls back to defaults when storage is empty or corrupt', () => {
    expect(loadWatchlist(memoryStorage())).toEqual(DEFAULT_WATCHLIST);
    expect(loadWatchlist(memoryStorage({ [WATCHLIST_KEY]: '{oops' }))).toEqual(
      DEFAULT_WATCHLIST,
    );
    expect(loadWatchlist(memoryStorage({ [WATCHLIST_KEY]: '[1, 2]' }))).toEqual(
      DEFAULT_WATCHLIST,
    );
    expect(loadWatchlist(undefined)).toEqual(DEFAULT_WATCHLIST);
  });

  it('sanitizes, dedupes and caps a stored list', () => {
    const stored = JSON.stringify([
      'sol-usdt',
      'SOL-USDT',
      'bad symbol',
      ...Array.from({ length: 20 }, (_, i) => `T${i}-USDT`),
    ]);
    const symbols = loadWatchlist(memoryStorage({ [WATCHLIST_KEY]: stored }));
    expect(symbols[0]).toBe('SOL-USDT');
    expect(symbols).toHaveLength(WATCHLIST_LIMIT);
    expect(new Set(symbols).size).toBe(symbols.length);
  });

  it('round-trips through storage', () => {
    const storage = memoryStorage();
    saveWatchlist(['BTC-USDT', 'ETH-USDT-SWAP'], storage);
    expect(loadWatchlist(storage)).toEqual(['BTC-USDT', 'ETH-USDT-SWAP']);
  });
});

describe('addSymbol / removeSymbol', () => {
  it('adds normalized symbols once and respects the limit', () => {
    const list = addSymbol(['BTC-USDT'], 'eth-usdt');
    expect(list).toEqual(['BTC-USDT', 'ETH-USDT']);
    expect(addSymbol(list, 'ETH-USDT')).toBe(list);
    expect(addSymbol(list, 'nonsense')).toBe(list);
    const full = Array.from(
      { length: WATCHLIST_LIMIT },
      (_, i) => `T${i}-USDT`,
    );
    expect(addSymbol(full, 'BTC-USDT')).toBe(full);
  });

  it('removes a symbol without mutating the input', () => {
    const list = ['BTC-USDT', 'ETH-USDT'];
    expect(removeSymbol(list, 'BTC-USDT')).toEqual(['ETH-USDT']);
    expect(list).toHaveLength(2);
  });
});

describe('watchRows', () => {
  it('joins tickers by instId and computes the 24h change', () => {
    const rows = watchRows(
      ['BTC-USDT', 'ETH-USDT'],
      [{ instId: 'BTC-USDT', last: '110', open24h: '100' }],
    );
    expect(rows[0]).toMatchObject({ symbol: 'BTC-USDT', last: '110' });
    expect(rows[0].changePct).toBeCloseTo(10);
    expect(rows[1]).toEqual({
      symbol: 'ETH-USDT',
      last: undefined,
      changePct: undefined,
      ticker: undefined,
    });
  });

  it('leaves the change undefined when the open is missing or zero', () => {
    const [row] = watchRows(
      ['BTC-USDT'],
      [{ instId: 'BTC-USDT', last: '110', open24h: '0' }],
    );
    expect(row.changePct).toBeUndefined();
  });
});

describe('priceDigits', () => {
  it('scales precision with magnitude', () => {
    expect(priceDigits('68000.5')).toBe(2);
    expect(priceDigits('3.14159')).toBe(4);
    expect(priceDigits('0.000123')).toBe(6);
    expect(priceDigits(undefined)).toBe(2);
  });
});
