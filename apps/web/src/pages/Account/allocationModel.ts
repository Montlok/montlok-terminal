import type { Row } from '../../operator/api';

export type Allocation = { currency: string; value: number; share: number };
export function assetAllocation(balances: Row[] = []): {
  slices: Allocation[];
  positive: number;
  negative: number;
  missing: number;
} {
  const values = new Map<string, number>();
  let negative = 0;
  let missing = 0;
  for (const row of balances) {
    if (
      !row.ccy ||
      row.eqUsd === undefined ||
      row.eqUsd === null ||
      row.eqUsd === '' ||
      !Number.isFinite(Number(row.eqUsd))
    ) {
      missing++;
      continue;
    }
    const value = Number(row.eqUsd);
    if (value < 0) negative += value;
    if (value > 0)
      values.set(String(row.ccy), (values.get(String(row.ccy)) || 0) + value);
  }
  const positive = [...values.values()].reduce((sum, value) => sum + value, 0);
  return {
    slices: [...values]
      .map(([currency, value]) => ({
        currency,
        value,
        share: value / positive,
      }))
      .sort((a, b) => b.value - a.value),
    positive,
    negative,
    missing,
  };
}
