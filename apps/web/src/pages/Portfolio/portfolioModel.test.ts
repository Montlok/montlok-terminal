import { describe, expect, it } from 'vitest';
import routes from '../../../config/routes';
import type { GroupEquity, StrategyGroup } from '../StrategyGroups/groupModel';
import {
  barGeometry,
  matchingPortfolio,
  positionAnalytics,
} from './portfolioModel';

describe('observed portfolio analytics', () => {
  it('opens the portfolio route by default and keeps the separate exchange terminal available', () => {
    expect(
      routes
        .find((route) => route.path === '/')
        ?.routes?.find((route) => route.path === '/'),
    ).toMatchObject({
      redirect: '/workspace/portfolio/overview',
    });
    const workspace = routes
      .find((route) => route.path === '/')
      ?.routes?.find((route) => route.path === '/workspace');
    expect(JSON.stringify(workspace)).toContain('"component":"./Portfolio"');
    expect(JSON.stringify(routes)).toContain('/trade/spot/terminal');
  });
  it('uses instrument labels when no complete explicit sector map exists', () => {
    const result = positionAnalytics([
      { instrument: 'XNVDA-USDT.OKX', notional: 100, unrealizedPnl: 3 },
      {
        instrument: 'XMU-USDT.OKX',
        notional: -50,
        unrealizedPnl: -2,
        sector: 'hardware',
      },
    ]);
    expect(result.basis).toBe('instrument');
    expect(result.exposure.map((row) => row.label)).toEqual([
      'XNVDA-USDT.OKX',
      'XMU-USDT.OKX',
    ]);
    expect(result.grossExposure).toBe(150);
    expect(result.unrealizedPnl).toBe(1);
    expect(result.contribution[1].value).toBe(-2);
  });
  it('aggregates only supplied sectors and never guesses them from ticker names', () => {
    const result = positionAnalytics([
      {
        instrument: 'AAA',
        sector: 'hardware',
        notional: '100',
        unrealizedPnl: '5',
      },
      {
        instrument: 'BBB',
        sector: 'hardware',
        notional: '50',
        unrealizedPnl: '-2',
      },
    ]);
    expect(result.basis).toBe('sector');
    expect(result.exposure).toEqual([
      { label: 'hardware', value: 150, count: 2 },
    ]);
    expect(result.contribution).toEqual([
      { label: 'hardware', value: 3, count: 2 },
    ]);
  });
  it('does not infer contract notional or turn missing values into zero', () => {
    const result = positionAnalytics([
      { instrument: 'AAA', notional: null, unrealizedPnl: '' },
      { instrument: 'BBB', notional: true, unrealizedPnl: Infinity },
      { instrument: 'CCC', notional: ' ', unrealizedPnl: [] },
    ]);
    expect(result.exposure).toEqual([]);
    expect(result.missingExposure).toBe(3);
    expect(result.grossExposure).toBeNull();
    expect(result.unrealizedPnl).toBeNull();
    expect(positionAnalytics([], false).grossExposure).toBeNull();
    expect(positionAnalytics([], true).grossExposure).toBe(0);
  });
  it('rejects run or group mismatches instead of plotting another run equity', () => {
    const group = { id: 'baseline', runId: 'run-one' } as StrategyGroup;
    const equity = { groupId: 'baseline', runId: 'run-one' } as GroupEquity;
    expect(matchingPortfolio(group, equity, 'baseline', '')).toBe(true);
    expect(matchingPortfolio(group, equity, 'baseline', 'run-two')).toBe(false);
    expect(
      matchingPortfolio(group, { ...equity, runId: 'run-two' }, 'baseline', ''),
    ).toBe(false);
    expect(matchingPortfolio(group, equity, 'enhanced', '')).toBe(false);
  });
  it('draws loss bars left of zero and bounds exposure without inventing a visual minimum', () => {
    expect(barGeometry(-5, 10, true)).toEqual({ left: 25, width: 25 });
    expect(barGeometry(10, 10, true)).toEqual({ left: 50, width: 50 });
    expect(barGeometry(0, 0, true)).toEqual({ left: 50, width: 0 });
    expect(barGeometry(5, 10, false)).toEqual({ left: 0, width: 50 });
  });
});
