import { Cell, Column, Table } from '@blueprintjs/table';
import { memo } from 'react';
import type { Row } from './api';
export type GridColumn = {
  key: string;
  title: string;
  width?: number;
  render?: (value: any, row: Row) => React.ReactNode;
};
/** Blueprint table; memoized so market ticks do not re-render the blotter. */
export const DataGrid = memo(function DataGrid({
  rows,
  columns,
  height = 260,
}: {
  rows: Row[];
  columns: GridColumn[];
  height?: number;
}) {
  if (!rows.length)
    return (
      <div className="empty-state" style={{ minHeight: height }}>
        暂无记录
      </div>
    );
  return (
    <div className="data-grid bp6-dark" style={{ height }}>
      <Table
        numRows={rows.length}
        defaultRowHeight={29}
        enableRowHeader={false}
        enableColumnResizing
        columnWidths={columns.map((column) => column.width || 150)}
        numFrozenColumns={1}
      >
        {columns.map((column) => (
          <Column
            key={column.key}
            name={column.title}
            cellRenderer={(index) => {
              const row = rows[index];
              return (
                <Cell>
                  {column.render
                    ? column.render(row[column.key], row)
                    : String(row[column.key] ?? '—')}
                </Cell>
              );
            }}
          />
        ))}
      </Table>
    </div>
  );
});
