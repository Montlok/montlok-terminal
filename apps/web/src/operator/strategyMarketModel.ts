import type { Row } from './api';
import { normalizeSymbol } from './watchlistModel';

export type StrategyInstrument = {
  instrument: string;
  quantity: string;
  weight?: number;
  sector?: string;
  notional?: number;
  targetQuantity?: string;
};
const symbol = (value: unknown) =>
  typeof value === 'string'
    ? normalizeSymbol(value.replace(/\.OKX$/, ''))
    : undefined;
const finite = (value: unknown) =>
  value !== null &&
  value !== undefined &&
  value !== '' &&
  Number.isFinite(Number(value))
    ? Number(value)
    : undefined;

/** Display scope follows the selected run, never an unrelated paper account. */
export function strategyInstruments(strategy?: Row): StrategyInstrument[] {
  const result = new Map<string, StrategyInstrument>();
  for (const row of strategy?.universe || []) {
    const id = symbol(row.instrument);
    if (!id) continue;
    result.set(id, {
      ...row,
      instrument: id,
      quantity: String(row.quantity ?? '0'),
      weight: finite(row.weight),
      notional: finite(row.notional),
    });
  }
  for (const row of strategy?.positions || []) {
    const id = symbol(row.instrument);
    if (!id) continue;
    result.set(id, {
      ...result.get(id),
      instrument: id,
      quantity: String(row.quantity ?? '0'),
      notional: finite(row.notional),
      sector: row.sector || result.get(id)?.sector,
    });
  }
  for (const row of strategy?.orders || []) {
    const id = symbol(row.instrument);
    if (id && !result.has(id))
      result.set(id, { instrument: id, quantity: '0' });
  }
  return [...result.values()].sort(
    (a, b) =>
      (b.weight ?? 0) - (a.weight ?? 0) ||
      (b.notional ?? 0) - (a.notional ?? 0) ||
      a.instrument.localeCompare(b.instrument),
  );
}

export function primaryStrategyInstrument(rows: StrategyInstrument[]): string {
  return (
    rows.find((row) => Math.abs(Number(row.quantity)) > 0)?.instrument ||
    rows[0]?.instrument ||
    ''
  );
}

export function watchedStrategySymbols(
  rows: StrategyInstrument[],
  scope: string,
): string[] {
  return rows
    .filter(
      (row) => scope !== 'positions' || Math.abs(Number(row.quantity)) > 0,
    )
    .map((row) => row.instrument);
}
