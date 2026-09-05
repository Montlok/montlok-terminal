import type { Row } from './api';

export const WATCHLIST_KEY = 'operator.watchlist';
export const WATCHLIST_LIMIT = 12;
export const DEFAULT_WATCHLIST = [
  'BTC-USDT',
  'ETH-USDT',
  'SOL-USDT',
  'OKB-USDT',
];

/** OKX instrument ids: `BTC-USDT`, `BTC-USDT-SWAP`, `BTC-USD-240628`. */
const INSTRUMENT = /^[A-Z0-9]+-[A-Z0-9]+(-[A-Z0-9]+)*$/;

export type WatchRow = {
  symbol: string;
  last?: string;
  changePct?: number;
  ticker?: Row;
};

type Storage = Pick<globalThis.Storage, 'getItem' | 'setItem'>;

export function normalizeSymbol(value: string): string | undefined {
  const symbol = value.trim().toUpperCase().replace('/', '-');
  return INSTRUMENT.test(symbol) ? symbol : undefined;
}

export function loadWatchlist(
  storage: Storage | undefined = globalThis.localStorage,
): string[] {
  try {
    const parsed: unknown = JSON.parse(storage?.getItem(WATCHLIST_KEY) || '');
    if (Array.isArray(parsed)) {
      const symbols = parsed
        .filter((item): item is string => typeof item === 'string')
        .map(normalizeSymbol)
        .filter((item): item is string => Boolean(item));
      if (symbols.length)
        return [...new Set(symbols)].slice(0, WATCHLIST_LIMIT);
    }
  } catch {
    // Corrupt or missing entry: fall through to the defaults.
  }
  return [...DEFAULT_WATCHLIST];
}

export function saveWatchlist(
  symbols: string[],
  storage: Storage | undefined = globalThis.localStorage,
): void {
  try {
    storage?.setItem(WATCHLIST_KEY, JSON.stringify(symbols));
  } catch {
    // Private mode or quota: the in-memory list still works for this session.
  }
}

export function addSymbol(symbols: string[], value: string): string[] {
  const symbol = normalizeSymbol(value);
  if (!symbol || symbols.includes(symbol) || symbols.length >= WATCHLIST_LIMIT)
    return symbols;
  return [...symbols, symbol];
}

export function removeSymbol(symbols: string[], symbol: string): string[] {
  return symbols.filter((item) => item !== symbol);
}

export function changePercent(ticker?: Row): number | undefined {
  const last = Number(ticker?.last);
  const open = Number(ticker?.open24h);
  if (!ticker || !Number.isFinite(last) || !(open > 0)) return undefined;
  return (last / open - 1) * 100;
}

/** Join the watched symbols with whatever tickers arrived; missing ones stay explicit. */
export function watchRows(symbols: string[], tickers: Row[]): WatchRow[] {
  const byId = new Map(tickers.map((ticker) => [ticker.instId, ticker]));
  return symbols.map((symbol) => {
    const ticker = byId.get(symbol);
    return {
      symbol,
      last: ticker?.last,
      changePct: changePercent(ticker),
      ticker,
    };
  });
}

/** Decimal places that keep prices from BTC to micro-caps readable in a narrow column. */
export function priceDigits(value: unknown): number {
  const price = Math.abs(Number(value));
  if (!Number.isFinite(price) || price === 0) return 2;
  if (price >= 1000) return 2;
  if (price >= 1) return 4;
  return 6;
}
