import { memo } from 'react';
import DepthChart from './DepthChart';
import { MarketChart } from './MarketChart';
import type { ResultChart } from './resultChartModel';
import { TimeSeriesChart } from './TimeSeriesChart';
import './resultCharts.css';

export const ResultCharts = memo(function ResultCharts({
  charts,
}: {
  charts: ResultChart[];
}) {
  return (
    <div className="result-charts">
      {charts.map((chart) => (
        <section className="result-chart" key={`${chart.kind}-${chart.label}`}>
          <div className="panel-heading">
            <strong>{chart.label}</strong>
            {chart.kind === 'series' && (
              <span>{chart.points.length} 个观测值</span>
            )}
          </div>
          {chart.kind === 'candles' ? (
            <MarketChart
              candles={chart.candles}
              showVolume={chart.showVolume}
            />
          ) : chart.kind === 'depth' ? (
            <DepthChart book={chart.book} height={360} />
          ) : (
            <TimeSeriesChart points={chart.points} label={chart.label} />
          )}
        </section>
      ))}
    </div>
  );
});
