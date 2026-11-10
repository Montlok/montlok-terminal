<script setup lang="ts">
import { computed, ref } from 'vue';

import type { OperatorException, OrderRow, PositionRow, StrategyInstance } from '../types';

type WorkspaceTab = 'positions' | 'orders' | 'exceptions' | 'strategies';

const props = defineProps<{
  positions: PositionRow[];
  orders: OrderRow[];
  exceptions: OperatorException[];
  strategies: StrategyInstance[];
  initialTab?: WorkspaceTab;
}>();

const activeTab = ref<WorkspaceTab>(props.initialTab ?? 'positions');
const emit = defineEmits<{ inspectException: [exception: OperatorException] }>();

const tabs = computed(() => [
  { id: 'positions' as const, label: '持仓', count: props.positions.length },
  { id: 'orders' as const, label: '订单', count: props.orders.length },
  { id: 'exceptions' as const, label: '异常', count: props.exceptions.filter((item) => item.status === 'OPEN').length },
  { id: 'strategies' as const, label: '策略', count: props.strategies.length },
]);

function number(value: number, digits = 2) {
  return value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
</script>

<template>
  <section class="workspace-tabs">
    <div class="tab-list" role="tablist" aria-label="交易工作区">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        :class="{ active: activeTab === tab.id }"
        type="button"
        role="tab"
        :aria-selected="activeTab === tab.id"
        @click="activeTab = tab.id"
      >
        {{ tab.label }} <small>{{ tab.count }}</small>
      </button>
    </div>

    <div class="table-wrap">
      <table v-if="activeTab === 'positions'">
        <thead><tr><th>策略</th><th>合约</th><th>方向</th><th>数量</th><th>均价 (USDT)</th><th>未实现盈亏 (USDT)</th></tr></thead>
        <tbody>
          <tr v-for="position in positions" :key="`${position.strategy}-${position.instrument}`">
            <td>{{ position.strategy }}</td><td>{{ position.instrument }}</td>
            <td :class="position.side.toLowerCase()">{{ position.side }}</td>
            <td>{{ number(position.quantity, 3) }}</td><td>{{ number(position.averagePrice) }}</td>
            <td :class="position.unrealizedPnl >= 0 ? 'positive' : 'negative'">{{ position.unrealizedPnl >= 0 ? '+' : '' }}{{ number(position.unrealizedPnl) }}</td>
          </tr>
        </tbody>
      </table>

      <table v-else-if="activeTab === 'orders'">
        <thead><tr><th>策略</th><th>合约</th><th>方向</th><th>类型</th><th>数量</th><th>价格</th><th>状态</th></tr></thead>
        <tbody>
          <tr v-for="order in orders" :key="order.id">
            <td>{{ order.strategy }}</td><td>{{ order.instrument }}</td>
            <td :class="order.side === 'Buy' ? 'long' : 'short'">{{ order.side }}</td><td>{{ order.type }}</td>
            <td>{{ number(order.quantity, 3) }}</td><td>{{ order.price ? number(order.price) : 'Market' }}</td><td>{{ order.status }}</td>
          </tr>
        </tbody>
      </table>

      <div v-else-if="activeTab === 'exceptions'" class="exception-list">
        <button v-for="item in exceptions" :key="item.id" @click="emit('inspectException', item)">
          <i :class="item.severity.toLowerCase()" />
          <span><strong>{{ item.title }}</strong><small>{{ item.detail }}</small></span>
          <time>{{ item.age }}</time><UIcon name="i-mdi-chevron-right" />
        </button>
      </div>

      <table v-else>
        <thead><tr><th>策略实例</th><th>合约</th><th>模式</th><th>配置</th><th>Heartbeat</th><th>PnL (USDT)</th></tr></thead>
        <tbody>
          <tr v-for="strategy in strategies" :key="strategy.id">
            <td>{{ strategy.id }}</td><td>{{ strategy.instrument }}</td><td>{{ strategy.mode }}</td>
            <td>{{ strategy.configVersion }}</td><td>{{ strategy.heartbeat }}</td>
            <td :class="strategy.pnl >= 0 ? 'positive' : 'negative'">{{ strategy.pnl >= 0 ? '+' : '' }}{{ number(strategy.pnl) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
