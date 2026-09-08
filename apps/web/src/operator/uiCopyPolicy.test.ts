import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const surfaces = [
  'HeaderStatus.tsx',
  'OrderTicket.tsx',
  'ModelRuntime.tsx',
  'StrategyRunPanel.tsx',
  'TimeSeriesChart.tsx',
  'Watchlist.tsx',
  '../models/operator.ts',
  '../pages/Connections/index.tsx',
  '../pages/Engine/index.tsx',
  '../pages/ExecutionAnalytics/index.tsx',
  '../pages/ModelReleases/index.tsx',
  '../pages/Portfolio/index.tsx',
  '../pages/ResearchHistory/index.tsx',
  '../pages/StrategyGroups/index.tsx',
  '../pages/Terminal/index.tsx',
];

const staleProductionTerms = [
  '本地模拟',
  '模拟盘',
  '影子运行',
  '模拟撮合',
  '独立虚拟预算',
  '实盘 · 只读',
];

describe('production UI copy', () => {
  it('keeps historical execution modes out of live operator surfaces', () => {
    for (const surface of surfaces) {
      const source = readFileSync(new URL(surface, import.meta.url), 'utf8');
      for (const term of staleProductionTerms)
        expect(source, `${surface} contains ${term}`).not.toContain(term);
    }
  });
});
