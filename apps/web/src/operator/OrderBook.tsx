import { HTMLTable } from '@blueprintjs/core';
import { memo } from 'react';
import { type Row, timeOf } from './api';
import { instrumentUnits, marketNumber } from './instrumentFormat';

type Level = string[];

function depthBar(row: Level, maxSize: number, color: string) {
  const share = (Number(row[1]) / maxSize) * 100;
  return {
    background: `linear-gradient(to left, ${color} ${share}%, transparent 0)`,
  };
}

/** Five-level ladder with size bars; re-renders only when the book frame changes. */
export const OrderBook = memo(function OrderBook({
  book,
  last,
  definition,
}: {
  book?: Row;
  last?: string;
  definition?: Row;
}) {
  const asks: Level[] = book?.asks || [];
  const bids: Level[] = book?.bids || [];
  const units = instrumentUnits(definition);
  const maxSize = Math.max(
    1,
    ...asks.map((row) => Number(row[1])),
    ...bids.map((row) => Number(row[1])),
  );
  return (
    <>
      <div className="book-labels">
        <span>价格 ({units.quote})</span>
        <span>数量 ({units.size})</span>
      </div>
      {[...asks].reverse().map((row) => (
        <div
          key={`ask-${row[0]}`}
          className="book-row"
          style={depthBar(row, maxSize, '#f05b6518')}
        >
          <span className="negative">
            {marketNumber(row[0], definition?.tickSz)}
          </span>
          <span>{marketNumber(row[1], definition?.lotSz)}</span>
        </div>
      ))}
      <div className="book-mid">
        {marketNumber(last || bids[0]?.[0], definition?.tickSz)}{' '}
        <span>{units.quote}</span>
      </div>
      {bids.map((row) => (
        <div
          key={`bid-${row[0]}`}
          className="book-row"
          style={depthBar(row, maxSize, '#25b77d18')}
        >
          <span className="positive">
            {marketNumber(row[0], definition?.tickSz)}
          </span>
          <span>{marketNumber(row[1], definition?.lotSz)}</span>
        </div>
      ))}
      {units.contractValue && (
        <p className="run-detail">{units.contractValue}</p>
      )}
    </>
  );
});

export const RecentTrades = memo(function RecentTrades({
  trades,
  limit = 18,
  definition,
}: {
  trades?: Row[];
  limit?: number;
  definition?: Row;
}) {
  const units = instrumentUnits(definition);
  return (
    <div className="latest-trades">
      <HTMLTable compact>
        <thead>
          <tr>
            <th>价格 ({units.quote})</th>
            <th>数量 ({units.size})</th>
            <th>时间</th>
          </tr>
        </thead>
        <tbody>
          {(trades || []).slice(0, limit).map((row: Row) => (
            <tr key={row.tradeId}>
              <td className={row.side === 'buy' ? 'positive' : 'negative'}>
                {marketNumber(row.px, definition?.tickSz)}
              </td>
              <td>{marketNumber(row.sz, definition?.lotSz)}</td>
              <td>{timeOf(Number(row.ts))}</td>
            </tr>
          ))}
        </tbody>
      </HTMLTable>
    </div>
  );
});
