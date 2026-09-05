import {
  type CandlestickData,
  CandlestickSeries,
  ColorType,
  createChart,
  type HistogramData,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  LineSeries,
  type UTCTimestamp,
} from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import type { Row } from './api';
import { lastMovingAverage, movingAverage } from './indicators';

const AVERAGES = [
  { period: 20, color: '#d3ad59' },
  { period: 60, color: '#9c83cb' },
];

type Stamp = UTCTimestamp;

function bar(row: Row): CandlestickData<Stamp> {
  return {
    time: row.time as Stamp,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
  };
}

function bucket(row: Row): HistogramData<Stamp> {
  return {
    time: row.time as Stamp,
    value: row.volume,
    color: row.close >= row.open ? '#25b77d66' : '#f05b6566',
  };
}

export function MarketChart({
  candles,
  candle,
  averages = true,
}: {
  candles: Row[];
  candle?: Row;
  averages?: boolean;
}) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | undefined>(undefined);
  const price = useRef<ISeriesApi<'Candlestick'> | undefined>(undefined);
  const volume = useRef<ISeriesApi<'Histogram'> | undefined>(undefined);
  const lastTime = useRef(0);
  const history = useRef<Row[]>([]);
  const lines = useRef<ISeriesApi<'Line'>[]>([]);
  useEffect(() => {
    if (!container.current) return;
    const instance = createChart(container.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: '#121518' },
        textColor: '#929ba6',
        fontSize: 11,
        attributionLogo: true,
      },
      grid: {
        vertLines: { color: '#20252a' },
        horzLines: { color: '#20252a' },
      },
      rightPriceScale: {
        borderColor: '#2a2e33',
        scaleMargins: { top: 0.08, bottom: 0.25 },
      },
      timeScale: {
        borderColor: '#2a2e33',
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: { mode: 0 },
    });
    chart.current = instance;
    price.current = instance.addSeries(CandlestickSeries, {
      upColor: '#25b77d',
      downColor: '#f05b65',
      wickUpColor: '#25b77d',
      wickDownColor: '#f05b65',
      borderVisible: false,
    });
    volume.current = instance.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    lines.current = AVERAGES.map(({ color }) =>
      instance.addSeries(LineSeries, {
        color,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        visible: averages,
      }),
    );
    instance
      .priceScale('volume')
      .applyOptions({ scaleMargins: { top: 0.8, bottom: 0 }, visible: false });
    return () => {
      instance.remove();
      chart.current = undefined;
      price.current = undefined;
      volume.current = undefined;
      lines.current = [];
    };
  }, []);
  useEffect(() => {
    // Private copy: live ticks mutate it in place without touching React state.
    history.current = [...candles];
    AVERAGES.forEach(({ period }, index) => {
      lines.current[index]?.setData(
        movingAverage(candles, period) as LineData<Stamp>[],
      );
    });
    price.current?.setData(candles.map(bar));
    volume.current?.setData(candles.map(bucket));
    lastTime.current = candles.at(-1)?.time || 0;
    chart.current?.timeScale().fitContent();
  }, [candles]);
  useEffect(() => {
    if (!candle || candle.time < lastTime.current) return;
    price.current?.update(bar(candle));
    volume.current?.update(bucket(candle));
    lastTime.current = candle.time;
    const rows = history.current;
    if (rows.at(-1)?.time === candle.time) rows[rows.length - 1] = candle;
    else rows.push(candle);
    AVERAGES.forEach(({ period }, index) => {
      const point = lastMovingAverage(rows, period);
      if (point) lines.current[index]?.update(point as LineData<Stamp>);
    });
  }, [candle]);
  useEffect(() => {
    for (const line of lines.current) line.applyOptions({ visible: averages });
  }, [averages]);
  return (
    <div
      className="market-chart"
      ref={container}
      role="img"
      aria-label="实时 K 线与成交量图"
    />
  );
}
