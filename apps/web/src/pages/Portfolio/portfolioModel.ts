import type { GroupEquity, StrategyGroup } from '../StrategyGroups/groupModel';

export type PortfolioPosition = {
  instrument?: unknown;
  sector?: unknown;
  sectorName?: unknown;
  notional?: unknown;
  unrealizedPnl?: unknown;
};
export type PortfolioBar = { label: string; value: number; count: number };
export type PositionAnalytics = {
  basis: 'sector' | 'instrument';
  available: boolean;
  exposure: PortfolioBar[];
  contribution: PortfolioBar[];
  grossExposure: number | null;
  unrealizedPnl: number | null;
  missingExposure: number;
  missingPnl: number;
};
export type RuntimeRun = {
  groupId: string;
  runId: string;
  status: string;
  startedAt?: number;
};
export type RuntimeGroups = {
  available?: boolean;
  groups?: { groupId: string; runs?: RuntimeRun[] }[];
};

function finite(value: unknown): number | undefined {
  if (typeof value !== 'number' && typeof value !== 'string') return undefined;
  if (typeof value === 'string' && !value.trim()) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/** Only explicit per-position sectors qualify as a complete sector map. */
export function positionAnalytics(
  positions: readonly PortfolioPosition[],
  available = true,
): PositionAnalytics {
  const basis =
    positions.length > 0 &&
    positions.every((row) => text(row.sectorName) || text(row.sector))
      ? 'sector'
      : 'instrument';
  const exposure = new Map<string, PortfolioBar>();
  const contribution = new Map<string, PortfolioBar>();
  let missingExposure = 0;
  let missingPnl = 0;
  if (available)
    for (const row of positions) {
      const label =
        basis === 'sector'
          ? text(row.sectorName) || text(row.sector)
          : text(row.instrument);
      const notional = finite(row.notional);
      const pnl = finite(row.unrealizedPnl);
      if (!label || notional === undefined) missingExposure += 1;
      else {
        const previous = exposure.get(label) || { label, value: 0, count: 0 };
        exposure.set(label, {
          ...previous,
          value: previous.value + Math.abs(notional),
          count: previous.count + 1,
        });
      }
      if (!label || pnl === undefined) missingPnl += 1;
      else {
        const previous = contribution.get(label) || {
          label,
          value: 0,
          count: 0,
        };
        contribution.set(label, {
          ...previous,
          value: previous.value + pnl,
          count: previous.count + 1,
        });
      }
    }
  const exposureRows = [...exposure.values()].sort(
    (a, b) => b.value - a.value || a.label.localeCompare(b.label),
  );
  const contributionRows = [...contribution.values()].sort(
    (a, b) =>
      Math.abs(b.value) - Math.abs(a.value) || a.label.localeCompare(b.label),
  );
  return {
    basis,
    available,
    exposure: exposureRows,
    contribution: contributionRows,
    grossExposure:
      available && !missingExposure
        ? exposureRows.reduce((sum, row) => sum + row.value, 0)
        : null,
    unrealizedPnl:
      available && !missingPnl
        ? contributionRows.reduce((sum, row) => sum + row.value, 0)
        : null,
    missingExposure,
    missingPnl,
  };
}

export function matchingPortfolio(
  group: StrategyGroup,
  equity: GroupEquity,
  groupId: string,
  selectedRun: string,
): boolean {
  return (
    group.id === groupId &&
    equity.groupId === groupId &&
    group.runId === equity.runId &&
    (!selectedRun || group.runId === selectedRun)
  );
}

export function barGeometry(value: number, maximum: number, signed: boolean) {
  const share =
    maximum > 0 && Number.isFinite(maximum)
      ? Math.min(1, Math.abs(value) / maximum)
      : 0;
  return signed
    ? { left: value < 0 ? 50 - share * 50 : 50, width: share * 50 }
    : { left: 0, width: share * 100 };
}
