import { Button, Descriptions, Drawer, Tabs } from 'antd';
import { type ReactNode, useMemo, useState } from 'react';
import { dataOf, type Row } from './api';
import { DataGrid } from './DataGrid';
import { fieldLabel, valueLabel } from './labels';
import { ResultCharts } from './ResultCharts';
import { resultCharts } from './resultChartModel';

export function RecordDetails({ value }: { value: Row }) {
  return (
    <Descriptions
      size="small"
      column={1}
      items={Object.entries(value || {}).map(([key, item]) => ({
        key,
        label: fieldLabel(key),
        children: cell(item),
      }))}
    />
  );
}
function cell(value: any): ReactNode {
  if (typeof value === 'object' && value !== null)
    return (
      <details>
        <summary>详情</summary>
        <pre className="json-output">{JSON.stringify(value, null, 2)}</pre>
      </details>
    );
  if (typeof value === 'boolean') return value ? '是' : '否';
  return valueLabel(value);
}
export function ResultView({
  value,
  operation,
}: {
  value?: any;
  operation?: string;
}) {
  const [selected, setSelected] = useState<Row>();
  const charts = useMemo(
    () => resultCharts(value, operation),
    [value, operation],
  );
  if (value === undefined) return null;
  const unwrapped = dataOf(value);
  const data =
    Array.isArray(unwrapped) &&
    unwrapped.length === 1 &&
    Array.isArray(unwrapped[0]?.details)
      ? unwrapped[0].details
      : unwrapped;
  const rows: Row[] = Array.isArray(data)
    ? data.map((row, i) =>
        Array.isArray(row)
          ? Object.fromEntries(row.map((value, j) => [String(j), value]))
          : typeof row === 'object' && row !== null
            ? row
            : { index: i + 1, value: row },
      )
    : [];
  const available = [
    ...new Set(rows.slice(0, 50).flatMap((row) => Object.keys(row))),
  ];
  const preferred = available.includes('fillPx')
    ? [
        'fillTime',
        'instId',
        'side',
        'fillPx',
        'fillSz',
        'fee',
        'feeCcy',
        'ordId',
      ]
    : available.includes('ordId')
      ? ['instId', 'side', 'ordType', 'px', 'sz', 'accFillSz', 'state', 'ordId']
      : available.includes('ccy')
        ? ['ccy', 'cashBal', 'availBal', 'frozenBal', 'eqUsd', 'upl', 'liab']
        : available.includes('last')
          ? [
              'instId',
              'last',
              'open24h',
              'high24h',
              'low24h',
              'vol24h',
              'volCcy24h',
            ]
          : available.slice(0, 10);
  const keys = preferred.filter((key) => available.includes(key));
  const content = Array.isArray(data) ? (
    <DataGrid
      rows={rows}
      height={360}
      columns={[
        ...keys.map((key) => ({
          key,
          title: fieldLabel(key),
          width: /Id$|Time$/.test(key) ? 210 : 145,
          render: cell,
        })),
        {
          key: '__details',
          title: '',
          width: 70,
          render: (_: unknown, row: Row) => (
            <Button type="link" onClick={() => setSelected(row)}>
              详情
            </Button>
          ),
        },
      ]}
    />
  ) : typeof data === 'object' && data !== null ? (
    <RecordDetails value={data} />
  ) : (
    <p>{String(data ?? '—')}</p>
  );
  return (
    <>
      <Tabs
        className="result-view"
        items={[
          ...(charts.length
            ? [
                {
                  key: 'charts',
                  label: '图表',
                  children: <ResultCharts charts={charts} />,
                },
              ]
            : []),
          { key: 'data', label: '结果', children: content },
          {
            key: 'raw',
            label: '原始数据',
            children: (
              <pre className="json-output">
                {JSON.stringify(value, null, 2)}
              </pre>
            ),
          },
        ]}
        destroyOnHidden
      />
      <Drawer
        title="详情"
        open={!!selected}
        onClose={() => setSelected(undefined)}
        size={640}
        destroyOnHidden
      >
        <RecordDetails value={selected || {}} />
      </Drawer>
    </>
  );
}
