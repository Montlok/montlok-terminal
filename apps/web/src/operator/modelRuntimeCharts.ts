import type { Row } from './api';
import { orderedPoints } from './resultChartModel';

export function modelRuntimeCharts(rows?: Row[]) {
  const observations = Array.isArray(rows) ? rows.slice(-64) : [];
  const series = (key: string, scale: number) =>
    orderedPoints(
      observations
        .filter(
          (row) =>
            typeof row[key] === 'number' &&
            Number.isFinite(row[key]) &&
            Number.isFinite(Number(row.completedAtNs)) &&
            Number(row.completedAtNs) > 0,
        )
        .map((row) => ({
          time: Math.floor(Number(row.completedAtNs) / 1e9),
          value: row[key] * scale,
        })),
    );
  return {
    predictions: series('prediction', 10000),
    targets: series('targetFraction', 100),
  };
}
