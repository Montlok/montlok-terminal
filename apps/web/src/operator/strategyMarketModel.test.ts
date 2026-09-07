import { describe, expect, it } from 'vitest';
import {
  primaryStrategyInstrument,
  strategyInstruments,
  watchedStrategySymbols,
} from './strategyMarketModel';

describe('strategy market scope', () => {
  it('uses the current stock allocation instead of hardcoded BTC', () => {
    const rows = strategyInstruments({
      universe: [
        { instrument: 'XNVDA-USDT', weight: 0.04 },
        { instrument: 'XTSLA-USDT', weight: 0.1 },
      ],
      positions: [
        { instrument: 'XNVDA-USDT.OKX', quantity: '.02', notional: 4 },
        { instrument: 'XTSLA-USDT.OKX', quantity: '.2', notional: 40 },
      ],
    });
    expect(primaryStrategyInstrument(rows)).toBe('XTSLA-USDT');
    expect(rows.some((r) => r.instrument === 'BTC-USDT')).toBe(false);
    expect(watchedStrategySymbols(rows, 'positions')).toHaveLength(2);
  });
  it('keeps targets not yet held and does not cap strategy universes at twelve', () => {
    const rows = strategyInstruments({
      universe: Array.from({ length: 26 }, (_, i) => ({
        instrument: `X${i}-USDT`,
        weight: 0.02,
      })),
      positions: [{ instrument: 'X1-USDT.OKX', quantity: '.1' }],
    });
    expect(watchedStrategySymbols(rows, 'strategy')).toHaveLength(26);
    expect(watchedStrategySymbols(rows, 'positions')).toEqual(['X1-USDT']);
  });
  it('does not invent a default asset before strategy data arrives', () => {
    expect(primaryStrategyInstrument(strategyInstruments())).toBe('');
  });
});
