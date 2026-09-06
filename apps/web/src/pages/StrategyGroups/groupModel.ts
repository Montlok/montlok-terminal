import type { Row } from '../../operator/api';
import type { TimePoint } from '../../operator/resultChartModel';

export type StrategyGroup = {
  id: string;
  name: string;
  description: string;
  mode: 'nautilus_sandbox' | 'live' | 'shadow' | null;
  modeLabel: string;
  accountId: string | null;
  runId: string | null;
  status: string;
  observedAt: number | null;
  signalAsOf?: string | null;
  signalVersion?: string | null;
  executionVersion?: string | null;
  startedAt?: number | null;
  scheduledStopAt?: number | null;
  completedAt?: number | null;
  elapsedSeconds?: number | null;
  sources?: Row;
  ordersTotal?: number | null;
  fillsTotal?: number | null;
  marketSource?: string | null;
  metrics: Record<string, number | null>;
  capabilities: { start: boolean; reason: string };
  alpha: Row[];
  version: Row[];
  health: Row;
  positions?: Row[];
  orders?: Row[];
  fills?: Row[];
  strategies?: Row[];
  systems?: Row[];
  exceptions?: Row[];
  runtimeConfig?: Row[];
};

export function runDateTime(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '未记录';
  const date = new Date(value * 1000);
  if (!Number.isFinite(date.getTime())) return '未记录';
  return date.toLocaleString('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export function runElapsed(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0)
    return '未记录';
  const total = Math.floor(value);
  return `${Math.floor(total / 3600)}h ${Math.floor((total % 3600) / 60)}m`;
}

export function runRecordCount(
  group: Pick<
    StrategyGroup,
    'sources' | 'ordersTotal' | 'fillsTotal' | 'orders' | 'fills' | 'positions'
  >,
  kind: 'orders' | 'fills' | 'positions',
): number | string {
  if (group.sources?.[kind === 'positions' ? 'view' : kind] === false)
    return '未知';
  const total =
    kind === 'orders'
      ? group.ordersTotal
      : kind === 'fills'
        ? group.fillsTotal
        : undefined;
  return typeof total === 'number' ? total : group[kind]?.length || 0;
}

export type GroupEquity = {
  groupId: string;
  runId: string | null;
  points: TimePoint[];
  drawdown: TimePoint[];
  sampleCount: number;
  partial: boolean;
  invalidLines: number;
  sourceAvailable: boolean;
  sourceStatus: 'available' | 'missing' | 'unreadable' | 'unconfigured';
  sourceIssue: string | null;
};

export function statusLabel(status: string): string {
  return (
    {
      running: '运行中',
      stopped: '已停止',
      halted: '已暂停',
      reducing: '仅减仓',
      recovering: '恢复中',
      error: '运行异常',
      unknown: '状态未知',
      completed: '已结束',
      stale: '数据延迟',
      unavailable: '无运行数据',
      pending_validation: '待验证',
      ready: '待运行',
      interrupted: '运行中断',
      engine_stopped: '进程收尾中',
      starting: '启动中',
      stopping: '停止中',
      failed: '运行失败',
      unresponsive: '引擎未响应',
    }[status] || '未知状态'
  );
}

export function stateWarning(
  status: string,
): { type: 'info' | 'warning' | 'error'; title: string } | undefined {
  if (status === 'halted')
    return {
      type: 'info',
      title: '已暂停开仓；当前持仓与行情仍持续监控，可按需恢复运行',
    };
  if (status === 'error')
    return { type: 'error', title: '引擎运行异常，请检查运行健康与异常记录' };
  if (status === 'reducing')
    return { type: 'warning', title: '引擎处于仅减仓状态' };
  if (status === 'recovering')
    return { type: 'warning', title: '引擎正在恢复，交易状态尚未恢复正常' };
  if (status === 'unknown')
    return { type: 'warning', title: '无法确认引擎交易状态' };
  return undefined;
}

/** A group switch must never display the previous group's holdings or returns. */
export function matchingGroup<T extends { id?: string; groupId?: string }>(
  value: T | undefined,
  id: string,
): T | undefined {
  return value && (value.id ?? value.groupId) === id ? value : undefined;
}
