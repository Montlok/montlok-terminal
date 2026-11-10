<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';

import MarketChart from './operator/components/MarketChart.vue';
import NautilusMark from './operator/components/NautilusMark.vue';
import ReconcileBanner from './operator/components/ReconcileBanner.vue';
import RiskHealth from './operator/components/RiskHealth.vue';
import TopMetrics from './operator/components/TopMetrics.vue';
import WorkspaceTabs from './operator/components/WorkspaceTabs.vue';
import { useOperatorApi } from './operator/composables/useOperatorApi';
import type { OperatorCommand, OperatorException } from './operator/types';

type NavId = 'overview' | 'market' | 'account' | 'orders' | 'risk' | 'strategies' | 'alerts' | 'settings';
type WorkspaceTab = 'positions' | 'orders' | 'exceptions' | 'strategies';

const navigation: { id: NavId; label: string; icon: string; count?: number }[] = [
  { id: 'overview', label: '概览', icon: 'i-mdi-home-outline' },
  { id: 'market', label: '行情', icon: 'i-mdi-chart-line' },
  { id: 'account', label: '账户', icon: 'i-mdi-account-outline' },
  { id: 'orders', label: '订单', icon: 'i-mdi-text-box-outline' },
  { id: 'risk', label: '风险', icon: 'i-mdi-scale-balance' },
  { id: 'strategies', label: '策略', icon: 'i-mdi-layers-outline' },
  { id: 'alerts', label: '异常', icon: 'i-mdi-bell-outline', count: 2 },
  { id: 'settings', label: '设置', icon: 'i-mdi-cog-outline' },
];

const selectedNav = ref<NavId>('overview');
const now = ref(new Date());
const moreOpen = ref(false);
const mobileNavOpen = ref(false);
const dialog = ref<{
  title: string;
  body: string;
  command?: OperatorCommand;
  dangerous?: boolean;
} | null>(null);
const commandPending = ref(false);
const commandMessage = ref('');
let clockTimer: number | undefined;

const { snapshot, source, refresh, sendCommand, start, stop } = useOperatorApi();
const isTradingReady = computed(() => source.value === 'live' && snapshot.value.readyToTrade);
const primaryException = computed(() => snapshot.value.exceptions.find((item) => item.severity === 'CRITICAL'));
const activeWorkspaceTab = computed<WorkspaceTab>(() => {
  if (selectedNav.value === 'orders') return 'orders';
  if (selectedNav.value === 'alerts') return 'exceptions';
  if (selectedNav.value === 'strategies') return 'strategies';
  return 'positions';
});

function selectNav(id: NavId) {
  selectedNav.value = id;
  mobileNavOpen.value = false;
}

function openCommand(command: OperatorCommand) {
  const copy = {
    PAUSE: ['暂停策略', '停止产生新订单。现有订单与仓位保持不变。'],
    REDUCE_ONLY: ['切换为仅减仓', '只允许减少已有风险敞口，禁止开新仓。'],
  } as const;
  const [title, body] = copy[command];
  dialog.value = { title, body, command };
  commandMessage.value = '';
}

function openDangerous(title: string, body: string) {
  moreOpen.value = false;
  dialog.value = { title, body, dangerous: true };
  commandMessage.value = '该危险命令尚未接入经过认证的控制 API，当前不会发送。';
}

function openLicense() {
  moreOpen.value = false;
  dialog.value = {
    title: '开源许可',
    body: 'Nautilus Operator 前端改造自 FreqUI，依 GPL-3.0 提供且不附带任何担保。行情图使用 TradingView Lightweight Charts，依 Apache-2.0 提供。完整许可与上游来源保存在仓库中。',
  };
}

function inspectException(exception?: OperatorException) {
  if (!exception) return;
  dialog.value = {
    title: exception.title,
    body: `${exception.detail}。首次发现 ${exception.firstSeen}，当前状态 ${exception.status}。`,
  };
}

async function confirmCommand() {
  const command = dialog.value?.command;
  if (!command) return;
  commandPending.value = true;
  commandMessage.value = '';
  try {
    const receipt = await sendCommand(command);
    commandMessage.value = `已提交：${receipt.receiptId}`;
    await refresh();
  } catch (error) {
    commandMessage.value = error instanceof Error ? error.message : '控制命令失败';
  } finally {
    commandPending.value = false;
  }
}

onMounted(() => {
  start();
  clockTimer = window.setInterval(() => (now.value = new Date()), 1_000);
});

onBeforeUnmount(() => {
  stop();
  if (clockTimer !== undefined) window.clearInterval(clockTimer);
});
</script>

<template>
  <UApp>
    <div class="operator-app">
      <header class="topbar">
        <button class="mobile-menu" aria-label="打开导航" @click="mobileNavOpen = !mobileNavOpen">
          <UIcon name="i-mdi-menu" />
        </button>
        <div class="brand-block"><NautilusMark /><strong>NAUTILUS</strong></div>
        <div class="top-field"><span>环境</span><strong class="demo">{{ snapshot.environment }}</strong></div>
        <div class="top-field account-field"><span>账户</span><strong>{{ snapshot.accountId }} <UIcon name="i-mdi-menu-down" /></strong></div>
        <div class="top-connection">
          <i :class="source" />
          <span>{{ source === 'live' ? '已连接' : '演示数据' }}</span>
          <b>OKX&nbsp; / &nbsp;WS</b>
        </div>
        <div class="top-time"><span>时间</span><strong>{{ now.toLocaleString('zh-CN', { hour12: false }) }} UTC+8</strong></div>
        <div class="top-actions">
          <span class="state-label">仅减仓</span>
          <button class="button secondary" @click="openCommand('PAUSE')"><UIcon name="i-mdi-pause" />暂停</button>
          <button class="button primary" @click="openCommand('REDUCE_ONLY')"><UIcon name="i-mdi-information-outline" />仅减仓</button>
          <div class="more-wrap">
            <button class="button more" @click="moreOpen = !moreOpen"><UIcon name="i-mdi-dots-horizontal" />更多…</button>
            <div v-if="moreOpen" class="more-menu">
              <button @click="openDangerous('撤销全部挂单', '撤销 OKX-001 账户当前运行策略的全部活动订单。需要二次确认和控制 API 授权。')">
                <UIcon name="i-mdi-cancel" />撤销全部挂单
              </button>
              <button class="danger" @click="openDangerous('平仓', '按账户范围仅使用减仓订单关闭持仓，并在完成后重新对账。需要二次确认和控制 API 授权。')">
                <UIcon name="i-mdi-alert-outline" />平仓…
              </button>
              <button @click="openLicense"><UIcon name="i-mdi-source-branch" />开源许可</button>
            </div>
          </div>
        </div>
      </header>

      <aside class="sidebar" :class="{ open: mobileNavOpen }">
        <nav aria-label="主导航">
          <button
            v-for="item in navigation"
            :key="item.id"
            :class="{ active: selectedNav === item.id }"
            :aria-label="item.label"
            :title="item.label"
            @click="selectNav(item.id)"
          >
            <UIcon :name="item.icon" /><span>{{ item.label }}</span><small v-if="item.count">{{ item.count }}</small>
          </button>
        </nav>
        <button class="sidebar-collapse" aria-label="收起导航"><UIcon name="i-mdi-chevron-double-left" /></button>
      </aside>

      <main class="workspace">
        <TopMetrics :risk="snapshot.risk" :systems="snapshot.systems" />
        <ReconcileBanner :exception="primaryException" @inspect="inspectException(primaryException)" />
        <section class="primary-grid">
          <MarketChart :market="snapshot.market" />
          <RiskHealth :risk="snapshot.risk" :systems="snapshot.systems" />
        </section>
        <WorkspaceTabs
          :key="activeWorkspaceTab"
          :positions="snapshot.positions"
          :orders="snapshot.orders"
          :exceptions="snapshot.exceptions"
          :strategies="snapshot.strategies"
          :initial-tab="activeWorkspaceTab"
          @inspect-exception="inspectException"
        />
      </main>

      <footer class="statusbar">
        <span>运行 ID：<b>{{ snapshot.runId }}</b></span>
        <span>策略版本：<b>{{ snapshot.configVersion }}</b></span>
        <span>配置版本：<b>v1.2.4</b></span>
        <span>事件存储：<b>{{ snapshot.eventStore }}</b><i /></span>
      </footer>

      <div v-if="dialog" class="modal-backdrop" @click.self="dialog = null">
        <section class="confirm-modal" role="dialog" aria-modal="true" :aria-label="dialog.title">
          <div class="modal-icon" :class="{ dangerous: dialog.dangerous }">
            <UIcon :name="dialog.dangerous ? 'i-mdi-alert-outline' : 'i-mdi-shield-check-outline'" />
          </div>
          <h2>{{ dialog.title }}</h2>
          <p>{{ dialog.body }}</p>
          <p v-if="dialog.command && !isTradingReady" class="modal-warning">
            当前不是已认证的实时 Operator API 会话，不会发送命令。
          </p>
          <p v-if="commandMessage" class="command-message">{{ commandMessage }}</p>
          <div class="modal-actions">
            <button class="button secondary" @click="dialog = null">关闭</button>
            <button
              v-if="dialog.command"
              class="button primary"
              :disabled="!isTradingReady || commandPending"
              @click="confirmCommand"
            >{{ commandPending ? '提交中…' : '确认提交' }}</button>
          </div>
        </section>
      </div>
    </div>
  </UApp>
</template>
