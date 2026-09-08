import { HTMLTable, Icon } from '@blueprintjs/core';
import { AutoComplete, Tabs } from 'antd';
import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import { api, dataOf, number, type Row, timeOf } from './api';
import {
  type StrategyInstrument,
  watchedStrategySymbols,
} from './strategyMarketModel';
import { usePoll } from './usePoll';
import {
  addSymbol,
  loadWatchlist,
  normalizeSymbol,
  priceDigits,
  removeSymbol,
  saveWatchlist,
  WATCHLIST_LIMIT,
  type WatchRow,
  watchRows,
} from './watchlistModel';

const POLL_MS = 1000;

function query(name: string, args: Row): Promise<Row[]> {
  return api('query', { kind: 'rest', name, arguments: args }).then(
    (value) => dataOf(value) || [],
  );
}

/** Live SPOT instrument ids, fetched once per page and shared by every watchlist. */
let instrumentsCache: Promise<string[]> | undefined;
function instruments(): Promise<string[]> {
  instrumentsCache ||= query('GET /api/v5/public/instruments', {
    instType: 'SPOT',
  })
    .then((rows) =>
      rows.filter((row) => row.state === 'live').map((row) => row.instId),
    )
    .catch((error) => {
      instrumentsCache = undefined;
      throw error;
    });
  return instrumentsCache;
}

function formatChange(value?: number): string {
  if (value === undefined) return '—';
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;
}

const WatchTableRow = memo(function WatchTableRow({
  row,
  active,
  onSelect,
  onRemove,
  position,
}: {
  row: WatchRow;
  active: boolean;
  onSelect: (symbol: string) => void;
  onRemove?: (symbol: string) => void;
  position?: StrategyInstrument;
}) {
  const tone =
    row.changePct === undefined
      ? ''
      : row.changePct < 0
        ? 'negative'
        : 'positive';
  return (
    <tr
      className={active ? 'active' : undefined}
      tabIndex={0}
      aria-selected={active}
      onClick={() => onSelect(row.symbol)}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          onSelect(row.symbol);
        }
      }}
    >
      <td>
        <strong>{row.symbol.replace('-', '/')}</strong>
        {position && (
          <small className="watch-position" title={position.quantity?`持仓 ${position.quantity}`:'持仓待同步'}>
            {Math.abs(Number(position.quantity)) > 0
              ? `持仓 ${position.quantity}`
              : position.weight
                ? `目标 ${number(position.weight * 100, 2)}%`
                : position.quantity===''?'持仓待同步':'未持有'}
          </small>
        )}
      </td>
      <td className={tone}>{number(row.last, priceDigits(row.last))}</td>
      <td className={tone}>{formatChange(row.changePct)}</td>
      {onRemove && (
        <td>
          <button
            type="button"
            className="watch-remove"
            aria-label={`移除 ${row.symbol}`}
            onClick={(event) => {
              event.stopPropagation();
              onRemove(row.symbol);
            }}
          >
            <Icon icon="small-cross" size={12} />
          </button>
        </td>
      )}
    </tr>
  );
});

/**
 * Persistent multi-instrument monitor. Polls one public ticker per symbol
 * every 5 s while the tab is visible; clicking a row switches the terminal.
 */
export function Watchlist({
  selected,
  onSelect,
  universe,
  scopeKey,
  groupName,
  positionsAvailable=true,
}: {
  selected: string;
  onSelect: (symbol: string) => void;
  universe?: StrategyInstrument[];
  scopeKey?: string;
  groupName?: string;
  positionsAvailable?: boolean;
}) {
  const [customSymbols, setSymbols] = useState(loadWatchlist);
  const [scope, setScope] = useState(universe ? 'strategy' : 'custom');
  useEffect(() => {
    if (universe) setScope('strategy');
  }, [scopeKey]);
  const symbols = useMemo(
    () =>
      universe && scope !== 'custom'
        ? watchedStrategySymbols(universe, scope)
        : customSymbols,
    [universe, scope, customSymbols],
  );
  const byInstrument = useMemo(
    () => new Map((universe || []).map((row) => [row.instrument, row])),
    [universe],
  );
  const [tickers, setTickers] = useState<Row[]>([]);
  const [updatedAt, setUpdatedAt] = useState(0);
  const [error, setError] = useState('');
  const [draft, setDraft] = useState('');
  const [candidates, setCandidates] = useState<string[]>([]);

  const add = useCallback((value: string) => {
    setSymbols((current) => {
      const next = addSymbol(current, value);
      if (next !== current) saveWatchlist(next);
      return next;
    });
    setDraft('');
  }, []);
  const remove = useCallback((symbol: string) => {
    setSymbols((current) => {
      const next = removeSymbol(current, symbol);
      saveWatchlist(next);
      return next;
    });
  }, []);

  const refresh = useCallback(
    async (signal: AbortSignal) => {
      if (symbols.every((s) => /^[A-Z0-9]+-USDT$/.test(s))) {
        try {
          const value = await api(
            `market/watchlist?instruments=${encodeURIComponent(symbols.join(','))}`,
          );
          if (signal.aborted) return;
          setTickers(value.tickers || []);
          setUpdatedAt(value.observedAt * 1000);
          setError(
            value.missing?.length
              ? `${value.missing.length} 个品种等待报价`
              : '',
          );
        } catch (reason) {
          if (!signal.aborted)
            setError(reason instanceof Error ? reason.message : String(reason));
        }
        return;
      }
      const settled = await Promise.allSettled(
        symbols.map((instId) => query('GET /api/v5/market/ticker', { instId })),
      );
      if (signal.aborted) return;
      const fresh = settled.flatMap((item) =>
        item.status === 'fulfilled' ? item.value : [],
      );
      const failure = settled.find(
        (item): item is PromiseRejectedResult => item.status === 'rejected',
      );
      setTickers(fresh);
      setUpdatedAt(Date.now());
      setError(
        failure ? String(failure.reason?.message || failure.reason) : '',
      );
    },
    [symbols],
  );
  usePoll(refresh, POLL_MS, symbols.length > 0);

  const rows = useMemo(() => watchRows(symbols, tickers), [symbols, tickers]);
  const options = useMemo(() => {
    const needle = draft.trim().toUpperCase().replace('/', '-');
    if (!needle) return [];
    return candidates
      .filter((id) => id.includes(needle) && !symbols.includes(id))
      .slice(0, 20)
      .map((value) => ({ value, label: value.replace('-', '/') }));
  }, [candidates, draft, symbols]);

  return (
    <section className="watch-panel panel" data-scope={scope}>
      <div className="panel-heading">
        <strong>{universe ? '策略行情' : '自选'}</strong>
        <span className="muted">
          {symbols.length}
          {scope === 'custom' ? `/${WATCHLIST_LIMIT}` : ' 个'} · OKX
        </span>
      </div>
      {universe && (
        <>
          <div className="watch-group-name" title={groupName}>
            {groupName}
          </div>
          <Tabs
            size="small"
            activeKey={scope}
            onChange={setScope}
            className="watch-scope-tabs"
            items={[
              { key: 'strategy', label: `标的 ${universe.length}` },
              {
                key: 'positions',
                label: `持仓 ${positionsAvailable?watchedStrategySymbols(universe, 'positions').length:'—'}`,
              },
              { key: 'custom', label: '自选' },
            ]}
          />
        </>
      )}
      {scope === 'custom' && (
        <div className="watch-add">
          <AutoComplete
            aria-label="添加自选"
            value={draft}
            options={options}
            placeholder="添加品种，如 SOL-USDT"
            disabled={symbols.length >= WATCHLIST_LIMIT}
            onFocus={() => {
              if (!candidates.length)
                instruments()
                  .then(setCandidates)
                  .catch(() => undefined);
            }}
            onChange={setDraft}
            onSelect={add}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && normalizeSymbol(draft)) add(draft);
            }}
          />
        </div>
      )}
      {rows.length ? (
        <HTMLTable compact interactive className="watch-table">
          <thead>
            <tr>
              <th>品种</th>
              <th>最新价</th>
              <th>24h</th>
              {scope === 'custom' && <th />}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <WatchTableRow
                key={row.symbol}
                row={row}
                active={row.symbol === selected}
                onSelect={onSelect}
                onRemove={scope === 'custom' ? remove : undefined}
                position={
                  scope === 'custom' ? undefined : byInstrument.get(row.symbol)
                }
              />
            ))}
          </tbody>
        </HTMLTable>
      ) : (
        <div className="empty-state" style={{ minHeight: 80 }}>
          {scope === 'custom'
            ? '在上方添加自选品种'
            : scope === 'positions'
              ? positionsAvailable?'持仓记录将随成交更新':'持仓待同步'
              : '读取策略标的'}
        </div>
      )}
      <div className={`watch-footer ${error ? 'negative' : 'muted'}`}>
        {error ||
          (updatedAt ? `更新 ${timeOf(updatedAt)} · 每秒刷新` : '等待行情')}
      </div>
    </section>
  );
}
