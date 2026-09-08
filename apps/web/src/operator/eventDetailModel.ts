import {
  EventEnvelope,
  EventType,
  OrderState,
  Side,
} from '@montlok/sdk/protocol';

const formatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  hour12: false,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});
export function eventTime(ns: bigint) {
  if (ns <= 0n) return '—';
  return `${formatter.format(Number(ns / 1_000_000_000n) * 1000)}.${(ns % 1_000_000_000n).toString().padStart(9, '0')}`;
}
export function elapsedMs(start: bigint, end: bigint) {
  if (start <= 0n || end < start) return '—';
  const delta = end - start;
  return `${delta / 1_000_000n}.${(delta % 1_000_000n).toString().padStart(6, '0')}`;
}
export const EVENT_LABELS: Partial<Record<EventType, string>> = {
  [EventType.ORDER_SUBMITTED]: '委托提交',
  [EventType.ORDER_ACCEPTED]: '委托确认',
  [EventType.ORDER_REJECTED]: '委托拒绝',
  [EventType.ORDER_CANCEL_REQUESTED]: '撤单提交',
  [EventType.ORDER_CANCELED]: '撤单确认',
  [EventType.FILL_RECEIVED]: '成交',
  [EventType.POSITION_UPDATED]: '持仓更新',
  [EventType.PNL_UPDATED]: '收益更新',
  [EventType.RUN_STATE_CHANGED]: '运行状态',
  [EventType.MODEL_INFERENCE_COMPLETED]: '模型推理',
  [EventType.SIGNAL_GENERATED]: '信号',
  [EventType.RISK_DECISION]: '风控',
  [EventType.TARGET_POSITION_CHANGED]: '目标更新',
  [EventType.ROUTE_UPDATED]: '路由更新',
  [EventType.ALERT_RAISED]: '告警',
  [EventType.ALERT_CLEARED]: '告警恢复',
  [EventType.OPERATION_RECEIPT_UPDATED]: '操作回执',
};
export const eventLabel = (event: EventEnvelope) =>
  EVENT_LABELS[event.eventType] || EventType[event.eventType];
export function eventFields(event: EventEnvelope): [string, string][] {
  const fields: [string, string][] = [
    ['事件', eventLabel(event)],
    ['发生时间 / UTC+8', eventTime(event.occurredAtNs)],
    ['接收时间 / UTC+8', eventTime(event.receivedAtNs)],
    ['采集耗时 / ms', elapsedMs(event.occurredAtNs, event.receivedAtNs)],
    ['账户', event.accountId],
    ['策略组', event.strategyGroupId],
    ['运行实例', event.runId],
    ['品种', event.instrumentId],
  ];
  const payload = event.payload;
  if (payload?.$case === 'orderEvent') {
    const value = payload.orderEvent;
    fields.push(
      [
        '方向',
        value.side === Side.BUY ? '买' : value.side === Side.SELL ? '卖' : '—',
      ],
      ['价格', value.price?.value || ''],
      ['数量', value.quantity?.value || ''],
      ['订单状态', OrderState[value.state]],
      ['客户订单号', value.clientOrderId],
      ['交易所订单号', value.venueOrderId],
      ['操作编号', value.operationId],
      ['风控编号', value.riskDecisionId],
      ['拒单原因', value.rejectReason],
    );
  } else if (payload?.$case === 'fillReceived') {
    const value = payload.fillReceived;
    fields.push(
      [
        '方向',
        value.side === Side.BUY ? '买' : value.side === Side.SELL ? '卖' : '—',
      ],
      ['成交价格', value.price?.value || ''],
      ['成交数量', value.quantity?.value || ''],
      [
        '费用',
        value.fee?.value === undefined
          ? ''
          : `${value.fee.value} ${value.feeCurrency}`,
      ],
      ['流动性', value.liquidity],
      ['Route', value.routeId],
      ['交易所订单号', value.venueOrderId],
      ['成交编号', value.tradeId],
    );
  } else if (payload?.$case === 'routeUpdated') {
    const value = payload.routeUpdated;
    fields.push(
      ['Route', value.routeId],
      ['客户订单号', value.clientOrderId],
      ['交易所订单号', value.venueOrderId],
      ['交易场所', value.venue],
      ['订单状态', OrderState[value.state]],
    );
  }
  fields.push(
    ['Correlation ID', event.correlationId],
    ['Causation ID', event.causationId],
    ['事件编号', event.eventId],
    ['来源', event.source],
    ['数据流', event.stream],
    ['流序号', event.streamSeq.toString()],
    ['协议版本', String(event.schemaVersion)],
  );
  return fields;
}
export function decodeRelated(value: {
  encoding: string;
  events: string[];
  truncated: boolean;
}) {
  if (
    value.encoding !== 'protobuf-base64' ||
    !Array.isArray(value.events) ||
    value.events.length > 2048
  )
    throw new Error('关联事件响应格式不完整');
  return value.events.map((encoded) =>
    EventEnvelope.decode(
      Uint8Array.from(atob(encoded), (char) => char.charCodeAt(0)),
    ),
  );
}
export function sameRun(left: EventEnvelope, right: EventEnvelope) {
  return (
    left.accountId === right.accountId &&
    left.runId === right.runId &&
    left.strategyGroupId === right.strategyGroupId
  );
}
