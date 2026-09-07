import type { Row } from '../../operator/api';
import type { TimePoint } from '../../operator/resultChartModel';

export type ExecutionDetail = {
  id: string;
  runId?: string | null;
  name?: string;
  mode?: string | null;
  modeLabel?: string;
  observedAt?: number | null;
  sources?: Record<string, unknown>;
  ordersTotal?: number | null;
  fillsTotal?: number | null;
  orders?: Row[];
  fills?: Row[];
  health?: Row;
  systems?: Row[];
  exceptions?: Row[];
  metrics?: { fees?: number | null };
};

export type Distribution = {
  label: string;
  value: number;
  tone: 'normal' | 'warning' | 'negative';
};

export function finite(value: unknown): number | undefined {
  if (
    value === null ||
    value === undefined ||
    typeof value === 'boolean' ||
    (typeof value === 'string' && !value.trim())
  )
    return undefined;
  const result = Number(value);
  return Number.isFinite(result) ? result : undefined;
}

export function executionTime(value: unknown): number | undefined {
  const numeric = finite(value);
  const seconds =
    numeric === undefined
      ? typeof value === 'string'
        ? Date.parse(value) / 1000
        : NaN
      : numeric >= 1e18
        ? numeric / 1e9
        : numeric >= 1e15
          ? numeric / 1e6
          : numeric >= 1e12
            ? numeric / 1000
            : numeric;
  return Number.isFinite(seconds) &&
    seconds >= 946684800 &&
    seconds < 7258118400
    ? Math.floor(seconds)
    : undefined;
}

/** Contracts require explicit notional; raw contracts are not base-asset units. */
export function fillNotional(fill: Row): number | undefined {
  const instrument = String(fill.instrument || '');
  const quotedUSDT = /-USDT(?:-SWAP)?(?:\.OKX)?$/.test(instrument);
  const reported = finite(fill.notional);
  if (
    reported !== undefined &&
    reported >= 0 &&
    (fill.quoteCurrency === 'USDT' || quotedUSDT)
  )
    return reported;
  const quantity = finite(fill.quantity);
  const price = finite(fill.price);
  if (
    !/^[A-Z0-9]+-USDT(?:\.OKX)?$/.test(instrument) ||
    quantity === undefined ||
    price === undefined ||
    quantity <= 0 ||
    price <= 0
  )
    return undefined;
  const result = quantity * price;
  return Number.isFinite(result) ? result : undefined;
}

const STATUS: Record<string, { label: string; tone: Distribution['tone'] }> = {
  INITIALIZED: { label: '已创建', tone: 'normal' },
  SUBMITTED: { label: '已提交', tone: 'normal' },
  ACCEPTED: { label: '已接受', tone: 'normal' },
  PENDING_UPDATE: { label: '修改中', tone: 'warning' },
  PENDING_CANCEL: { label: '撤单中', tone: 'warning' },
  PARTIALLY_FILLED: { label: '部分成交', tone: 'normal' },
  FILLED: { label: '已成交', tone: 'normal' },
  CANCELED: { label: '已撤单', tone: 'normal' },
  CANCELLED: { label: '已撤单', tone: 'normal' },
  EXPIRED: { label: '已过期', tone: 'warning' },
  REJECTED: { label: '拒单', tone: 'negative' },
  DENIED: { label: '风控拒绝', tone: 'negative' },
};

function recordCount(
  detail: ExecutionDetail,
  kind: 'orders' | 'fills',
): number | null {
  if (detail.sources?.[kind] !== true || !Array.isArray(detail[kind]))
    return null;
  const reported = finite(
    kind === 'orders' ? detail.ordersTotal : detail.fillsTotal,
  );
  return reported !== undefined &&
    Number.isInteger(reported) &&
    reported >= detail[kind].length
    ? reported
    : null;
}

export function executionAnalytics(detail: ExecutionDetail) {
  const orders =
    Array.isArray(detail.orders) && detail.sources?.orders !== false
      ? detail.orders
      : [];
  const fills =
    Array.isArray(detail.fills) && detail.sources?.fills !== false
      ? detail.fills
      : [];
  const buckets = new Map<number, number>();
  const fees = new Map<string, number>();
  const statuses = new Map<string, Distribution>();
  let notional = 0;
  let notionalRecords = 0;
  let amountWithoutTime = 0;
  let usdtFee = 0;
  let feeRecords = 0;
  let feesWithoutCurrency = 0;
  let feesOtherCurrency = 0;
  for (const fill of fills) {
    const amount = fillNotional(fill);
    const at = executionTime(fill.time);
    if (amount !== undefined) {
      notional += amount;
      notionalRecords += 1;
      if (at === undefined) amountWithoutTime += 1;
      else {
        const bucket = Math.floor(at / 300) * 300;
        buckets.set(bucket, (buckets.get(bucket) || 0) + amount);
      }
    }
    const fee = finite(fill.fee);
    if (fee !== undefined && fill.feeCurrency === 'USDT') {
      feeRecords += 1;
      usdtFee += fee;
      const instrument = String(fill.instrument || '品种未记录');
      fees.set(instrument, (fees.get(instrument) || 0) + fee);
    } else if (fee !== undefined && fill.feeCurrency) feesOtherCurrency += 1;
    else if (fee !== undefined) feesWithoutCurrency += 1;
  }
  for (const order of orders) {
    const key = String(order.status || '').toUpperCase();
    const { label, tone } = STATUS[key] || {
      label: '未知状态',
      tone: 'warning' as const,
    };
    const previous = statuses.get(label);
    statuses.set(label, { label, value: (previous?.value || 0) + 1, tone });
  }
  const ordersTotal = recordCount(detail, 'orders');
  const fillsTotal = recordCount(detail, 'fills');
  const knownEmpty = detail.sources?.fills === true && fillsTotal === 0;
  return {
    orders,
    fills,
    ordersTotal,
    fillsTotal,
    ordersSource: detail.sources?.orders === true,
    fillsSource: detail.sources?.fills === true,
    ordersPartial: ordersTotal === null || ordersTotal > orders.length,
    fillsPartial: fillsTotal === null || fillsTotal > fills.length,
    notional: notionalRecords || knownEmpty ? notional : null,
    notionalRecords,
    missingNotional: fills.length - notionalRecords,
    amountWithoutTime,
    usdtFee: feeRecords || knownEmpty ? usdtFee : null,
    feeRecords,
    feesWithoutCurrency,
    feesOtherCurrency,
    unknownFee:
      fills.length - feeRecords - feesWithoutCurrency - feesOtherCurrency,
    volume: [...buckets]
      .sort(([a], [b]) => a - b)
      .map(([time, value]): TimePoint => ({ time, value })),
    feesByInstrument: [...fees]
      .sort(([, a], [, b]) => Math.abs(b) - Math.abs(a))
      .map(
        ([label, value]): Distribution => ({
          label,
          value,
          tone: value < 0 ? 'normal' : 'warning',
        }),
      ),
    statuses: [...statuses.values()].sort((a, b) => b.value - a.value),
    rejected:
      detail.sources?.orders !== true || statuses.has('未知状态')
        ? null
        : orders.length
          ? orders.filter((order) =>
              ['REJECTED', 'DENIED'].includes(
                String(order.status).toUpperCase(),
              ),
            ).length
          : ordersTotal === 0
            ? 0
            : null,
  };
}
