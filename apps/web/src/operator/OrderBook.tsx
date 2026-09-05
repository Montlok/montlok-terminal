import { HTMLTable } from '@blueprintjs/core';
import { memo } from 'react';
import { number, type Row, timeOf } from './api';

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
}: {
  book?: Row;
  last?: string;
}) {
  const asks: Level[] = book?.asks || [];
  const bids: Level[] = book?.bids || [];
  const maxSize = Math.max(
    1,
    ...asks.map((row) => Number(row[1])),
    ...bids.map((row) => Number(row[1])),
  );
  return (
    <>
      <div className="book-labels">
        <span>价格 (USDT)</span>
        <span>数量</span>
      </div>
      {[...asks].reverse().map((row) => (
        <div
          key={`ask-${row[0]}`}
          className="book-row"
          style={depthBar(row, maxSize, '#f05b6518')}
        >
          <span className="negative">{number(row[0], 4)}</span>
          <span>{number(row[1], 5)}</span>
        </div>
      ))}
      <div className="book-mid">
        {number(last || bids[0]?.[0], 4)} <span>USDT</span>
      </div>
      {bids.map((row) => (
        <div
          key={`bid-${row[0]}`}
          className="book-row"
          style={depthBar(row, maxSize, '#25b77d18')}
        >
          <span className="positive">{number(row[0], 4)}</span>
          <span>{number(row[1], 5)}</span>
        </div>
      ))}
    </>
  );
});

export const RecentTrades = memo(function RecentTrades({
  trades,
  limit = 18,
}: {
  trades?: Row[];
  limit?: number;
}) {
  return (
    <div className="latest-trades">
      <HTMLTable compact>
        <thead>
          <tr>
            <th>价格</th>
            <th>数量</th>
            <th>时间</th>
          </tr>
        </thead>
        <tbody>
          {(trades || []).slice(0, limit).map((row: Row) => (
            <tr key={row.tradeId}>
              <td className={row.side === 'buy' ? 'positive' : 'negative'}>
                {number(row.px, 4)}
              </td>
              <td>{number(row.sz, 5)}</td>
              <td>{timeOf(Number(row.ts))}</td>
            </tr>
          ))}
        </tbody>
      </HTMLTable>
    </div>
  );
});
