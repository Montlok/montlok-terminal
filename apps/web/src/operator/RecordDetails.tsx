import { Descriptions } from 'antd';
import type { ReactNode } from 'react';
import type { Row } from './api';
import { fieldLabel, valueLabel } from './labels';

export function recordCell(value: unknown): ReactNode {
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

/** Lightweight detail renderer without the data-grid and chart packages. */
export function RecordDetails({ value }: { value: Row }) {
  return (
    <Descriptions
      size="small"
      column={1}
      items={Object.entries(value || {}).map(([key, item]) => ({
        key,
        label: fieldLabel(key),
        children: recordCell(item),
      }))}
    />
  );
}
