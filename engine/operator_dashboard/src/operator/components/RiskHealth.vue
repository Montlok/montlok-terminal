<script setup lang="ts">
import { computed } from 'vue';

import type { RiskSnapshot, SystemHealth } from '../types';

const props = defineProps<{
  risk: RiskSnapshot;
  systems: SystemHealth[];
}>();

const statusRows = computed(() => {
  const byId = new Map(props.systems.map((system) => [system.id, system]));
  return [
    { label: '市场数据', value: '新鲜', level: byId.get('market')?.level ?? 'CRITICAL' },
    { label: '执行连接', value: '已连接', level: byId.get('execution')?.level ?? 'CRITICAL' },
    { label: '事件存储', value: '已封存', level: byId.get('event-store')?.level ?? 'CRITICAL' },
  ];
});
</script>

<template>
  <aside class="risk-health" aria-label="风险与健康">
    <h2>风险与健康</h2>

    <div class="risk-loss">
      <div><span>Daily loss</span><strong>{{ risk.dailyLossUsedPct }}%</strong></div>
      <div class="progress-track"><i :style="{ width: `${risk.dailyLossUsedPct}%` }" /></div>
    </div>

    <dl class="risk-list">
      <div><dt>净敞口 (BTC)</dt><dd>{{ risk.netExposure.toFixed(3) }}</dd></div>
      <div><dt>Delta (BTC)</dt><dd>{{ risk.delta.toFixed(3) }}</dd></div>
      <div><dt>保证金率</dt><dd>{{ risk.marginRatioPct.toFixed(2) }}%</dd></div>
      <div><dt>已用保证金</dt><dd>{{ Math.round(risk.margin).toLocaleString('en-US') }} USDT</dd></div>
      <div class="queue-row">
        <dt>队列压力</dt>
        <dd><span class="queue-track"><i :style="{ width: `${risk.queuePressurePct}%` }" /></span>{{ risk.queuePressurePct }}%</dd>
      </div>
    </dl>

    <div class="health-list">
      <div v-for="status in statusRows" :key="status.label">
        <i :class="status.level.toLowerCase()" />
        <span>{{ status.label }}</span>
        <small>{{ status.value }}</small>
      </div>
      <div class="reconcile-time">
        <span>最后对账</span>
        <small>{{ risk.lastReconcileAt }} ({{ risk.reconcileAge }})</small>
      </div>
    </div>
  </aside>
</template>
