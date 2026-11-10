<script setup lang="ts">
import { computed } from 'vue';

import type { RiskSnapshot, SystemHealth } from '../types';

const props = defineProps<{
  risk: RiskSnapshot;
  systems: SystemHealth[];
}>();

const healthySystems = computed(
  () => props.systems.filter((system) => system.level === 'HEALTHY').length,
);

const numberFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
</script>

<template>
  <section class="metrics-band" aria-label="账户关键指标">
    <div class="metric">
      <span>今日 PnL</span>
      <strong class="metric-positive">+{{ numberFormatter.format(risk.dailyPnl) }}</strong>
      <small>USDT</small>
    </div>
    <div class="metric">
      <span>净敞口</span>
      <strong>{{ risk.netExposure.toFixed(3) }}</strong>
      <small>BTC</small>
    </div>
    <div class="metric">
      <span>保证金</span>
      <strong>{{ Math.round(risk.margin).toLocaleString('en-US') }}</strong>
      <small>USDT</small>
    </div>
    <div class="metric">
      <span>系统安全</span>
      <strong class="metric-positive">{{ healthySystems }}</strong>
      <small>/ {{ systems.length }}</small>
    </div>
  </section>
</template>
