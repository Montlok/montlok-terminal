import {
  ColorType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  LineSeries,
  type UTCTimestamp,
} from 'lightweight-charts';
import { memo, useEffect, useMemo, useRef } from 'react';
import {
  CHART_TIME_ZONE_LABEL,
  chartCrosshairFormatter,
  chartTickFormatter,
} from './chartTime';
import { orderedPoints, type TimePoint } from './resultChartModel';
import './resultCharts.css';

export const TimeSeriesChart = memo(function TimeSeriesChart({
  points,
  label,
  color = '#7fabff',
  height = 260,
}: {
  points: TimePoint[];
  label: string;
  color?: string;
  height?: number;
}) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | undefined>(undefined);
  const series = useRef<ISeriesApi<'Line'> | undefined>(undefined);
  const previous = useRef<{ first?: number; last?: number }>({});
  const data = useMemo(() => orderedPoints(points), [points]);
  useEffect(() => {
    if (!container.current) return;
    const instance = createChart(container.current, {
      autoSize: true,
      localization: { locale: 'zh-CN', timeFormatter: chartCrosshairFormatter },
      layout: {
        background: { type: ColorType.Solid, color: '#121518' },
        textColor: '#929ba6',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: '#20252a' },
        horzLines: { color: '#20252a' },
      },
      rightPriceScale: { borderColor: '#2a2e33' },
      timeScale: {
        borderColor: '#2a2e33',
        timeVisible: true,
        tickMarkFormatter: chartTickFormatter,
        fixRightEdge: true,
        rightOffset: 0,
        minBarSpacing: 0.05,
        fixLeftEdge: false,
      },
    });
    chart.current = instance;
    series.current = instance.addSeries(LineSeries, {
      color,
      lineWidth: 2,
      priceLineVisible: false,
      pointMarkersVisible: true,
      pointMarkersRadius: 2,
      priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
    });
    previous.current = {};
    return () => {
      instance.remove();
      chart.current = undefined;
      series.current = undefined;
    };
  }, [color]);
  useEffect(() => {
    series.current?.applyOptions({ pointMarkersVisible: data.length <= 40 });
    series.current?.setData(
      data.map((point) => ({ ...point, time: point.time as UTCTimestamp })),
    );
    const first = data[0]?.time;
    const last = data.at(-1)?.time;
    if (
      first !== previous.current.first ||
      (last ?? 0) < (previous.current.last ?? 0)
    )
      chart.current?.timeScale().fitContent();
    previous.current = { first, last };
  }, [data, color]);
  return (
    <div className="time-series-chart" style={{ height }}>
      <div
        className="time-series-canvas"
        ref={container}
        role="img"
        aria-label={`${label} · ${CHART_TIME_ZONE_LABEL}`}
        title={`图表时间 ${CHART_TIME_ZONE_LABEL}`}
      />
      {!data.length && (
        <div className="chart-empty">等待首个时间序列采样点</div>
      )}
    </div>
  );
});
