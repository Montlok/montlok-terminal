import {
  CandlestickSeries,
  ColorType,
  createChart,
  HistogramSeries,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import type { Row } from './api';
import { movingAverage } from './indicators';

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
    lines.current = ['#d3ad59','#9c83cb'].map(color=>instance.addSeries(LineSeries,{color,lineWidth:1,priceLineVisible:false,lastValueVisible:false,visible:averages}));
    instance
      .priceScale('volume')
      .applyOptions({ scaleMargins: { top: 0.8, bottom: 0 }, visible: false });
    return () => {
      instance.remove();
      chart.current = undefined;
      price.current = undefined;
      volume.current = undefined;
      lines.current=[];
    };
  }, []);
  useEffect(() => {
    history.current=candles;
    [20,60].forEach((period,index)=>lines.current[index]?.setData(movingAverage(candles,period) as any));
    price.current?.setData(
      candles.map((row) => ({ ...row, time: row.time as UTCTimestamp })) as any,
    );
    volume.current?.setData(
      candles.map((row) => ({
        time: row.time as UTCTimestamp,
        value: row.volume,
        color: row.close >= row.open ? '#25b77d66' : '#f05b6566',
      })),
    );
    lastTime.current = candles.at(-1)?.time || 0;
    chart.current?.timeScale().fitContent();
  }, [candles]);
  useEffect(() => {
    if (!candle || candle.time < lastTime.current) return;
    price.current?.update({
      ...candle,
      time: candle.time as UTCTimestamp,
    } as any);
    volume.current?.update({
      time: candle.time as UTCTimestamp,
      value: candle.volume,
      color: candle.close >= candle.open ? '#25b77d66' : '#f05b6566',
    });
    lastTime.current = candle.time;
    const rows=history.current;
    history.current = rows.at(-1)?.time===candle.time ? [...rows.slice(0,-1),candle] : [...rows,candle];
    [20,60].forEach((period,index)=>{
      const last=movingAverage(history.current,period).at(-1);
      if(last) lines.current[index]?.update(last as any);
    });
  }, [candle]);
  useEffect(()=>{lines.current.forEach(line=>line.applyOptions({visible:averages}));},[averages]);
  return (
    <div
      className="market-chart"
      ref={container}
      role="img"
      aria-label="实时 K 线与成交量图"
    />
  );
}
