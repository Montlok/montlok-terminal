<script setup lang="ts">
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type Time,
} from 'lightweight-charts';
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';

import type { MarketSnapshot } from '../types';

const props = defineProps<{ market: MarketSnapshot }>();

const container = ref<HTMLDivElement>();
const range = ref('5m');
const ranges = ['1m', '5m', '15m', '1H', '4H', '1D'];
let chart: IChartApi | undefined;
let resizeObserver: ResizeObserver | undefined;

const price = computed(() => props.market.last.toLocaleString('en-US', {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
}));

function seededCandles(last: number): { candles: CandlestickData[]; volume: HistogramData[] } {
  const candles: CandlestickData[] = [];
  const volume: HistogramData[] = [];
  const count = 96;
  const intervalSeconds = 300;
  const now = Math.floor(Date.now() / 1000 / intervalSeconds) * intervalSeconds;
  let close = last - 1_150;

  for (let index = 0; index < count; index += 1) {
    const drift = index < 56 ? 18 : index < 70 ? -24 : 17;
    const wave = Math.sin(index * 0.57) * 105 + Math.sin(index * 0.19) * 72;
    const open = close;
    close = index === count - 1 ? last : open + drift + wave * 0.16;
    const high = Math.max(open, close) + 42 + Math.abs(Math.sin(index)) * 48;
    const low = Math.min(open, close) - 38 - Math.abs(Math.cos(index * 0.8)) * 44;
    const time = (now - (count - 1 - index) * intervalSeconds) as Time;
    candles.push({ time, open, high, low, close });
    volume.push({
      time,
      value: 300 + Math.abs(Math.sin(index * 0.43)) * 730 + (index % 17 === 0 ? 880 : 0),
      color: close >= open ? 'rgba(52,199,89,.55)' : 'rgba(255,59,48,.48)',
    });
  }
  return { candles, volume };
}

function createMarketChart() {
  if (!container.value) return;
  chart?.remove();
  chart = createChart(container.value, {
    width: container.value.clientWidth,
    height: container.value.clientHeight,
    layout: {
      background: { type: ColorType.Solid, color: '#ffffff' },
      textColor: '#6e6e73',
      fontFamily: '-apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif',
      fontSize: 11,
      attributionLogo: true,
    },
    grid: {
      vertLines: { color: '#f0f0f2' },
      horzLines: { color: '#f0f0f2' },
    },
    rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.22 } },
    timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false, rightOffset: 2 },
    crosshair: {
      vertLine: { color: '#aeb4bd', width: 1, style: LineStyle.Dashed, labelVisible: false },
      horzLine: { color: '#aeb4bd', width: 1, style: LineStyle.Dashed },
    },
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
  });

  const candleSeries = chart.addSeries(CandlestickSeries, {
    upColor: '#16a568',
    downColor: '#ff453a',
    wickUpColor: '#16a568',
    wickDownColor: '#ff453a',
    borderVisible: false,
    priceLineColor: '#ff453a',
    priceLineStyle: LineStyle.Dashed,
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
  });
  const volumeSeries = chart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: 'volume',
  });
  volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.83, bottom: 0 } });

  const data = seededCandles(props.market.last);
  candleSeries.setData(data.candles);
  volumeSeries.setData(data.volume);
  createSeriesMarkers(candleSeries, [
    { time: data.candles[59].time, position: 'aboveBar', color: '#ff453a', shape: 'arrowDown', text: 'S' },
    { time: data.candles[67].time, position: 'belowBar', color: '#16a568', shape: 'arrowUp', text: 'B' },
    { time: data.candles[85].time, position: 'belowBar', color: '#16a568', shape: 'arrowUp', text: 'B' },
    { time: data.candles[91].time, position: 'aboveBar', color: '#ff453a', shape: 'arrowDown', text: 'S' },
  ]);
  candleSeries.createPriceLine({ price: 103_650, color: '#16a568', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '' });
  chart.timeScale().fitContent();

  resizeObserver = new ResizeObserver(() => {
    if (container.value && chart) {
      chart.applyOptions({ width: container.value.clientWidth, height: container.value.clientHeight });
    }
  });
  resizeObserver.observe(container.value);
}

onMounted(() => void nextTick(createMarketChart));
watch(() => props.market.last, () => void nextTick(createMarketChart));
onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  chart?.remove();
});
</script>

<template>
  <section class="market-panel">
    <header class="market-header">
      <div class="instrument-name">{{ market.instrumentId }} <UIcon name="i-mdi-menu-down" /></div>
      <strong>{{ price }}</strong><span>USDT</span>
      <em :class="market.changePct >= 0 ? 'positive' : 'negative'">
        {{ market.changePct >= 0 ? '+' : '' }}{{ market.changePct.toFixed(2) }}%
      </em>
    </header>
    <div class="chart-toolbar">
      <div class="chart-ranges">
        <button v-for="item in ranges" :key="item" :class="{ active: range === item }" @click="range = item">
          {{ item }}
        </button>
      </div>
      <div class="chart-tools">
        <button aria-label="图表类型"><UIcon name="i-mdi-chart-line" /></button>
        <button><UIcon name="i-mdi-function-variant" /> 指标</button>
        <button aria-label="截图"><UIcon name="i-mdi-camera-outline" /></button>
        <button aria-label="全屏"><UIcon name="i-mdi-fullscreen" /></button>
      </div>
    </div>
    <div class="ohlc-line">
      <span>开 <b>{{ (market.last - 12.5).toLocaleString() }}</b></span>
      <span>高 <b>{{ (market.last + 27.7).toLocaleString() }}</b></span>
      <span>低 <b>{{ (market.last - 30.2).toLocaleString() }}</b></span>
      <span>收 <b>{{ market.last.toLocaleString() }}</b></span>
      <strong>+12.5 (+0.01%)</strong>
    </div>
    <div ref="container" class="chart-canvas" />
  </section>
</template>
