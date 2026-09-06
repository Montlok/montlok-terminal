import { useEffect, useRef, useState } from 'react';
import { api, dataOf, ensureSession, type Row } from './api';
import { instrumentType } from './instrumentFormat';
import { MarketBuffer } from './marketBuffer';

export function useMarket(
  instrument: string,
  bar: string,
  mode: string,
): Row & { candles: Row[]; candle?: Row } {
  const [state, setState] = useState<Row>({
    ticker: {},
    book: { asks: [], bids: [] },
    trades: [],
    connected: false,
  });
  const [candles, setCandles] = useState<Row[]>([]);
  const [candle, setCandle] = useState<Row>();
  const generation = useRef(0);
  useEffect(() => {
    const current = ++generation.current;
    let socket: WebSocket | undefined;
    let reconnect: ReturnType<typeof setTimeout> | undefined;
    let disposed = false;
    const buffer = new MarketBuffer();
    let lastCandle: Row | undefined;
    const parameters = new URLSearchParams({ instrument, bar, mode });
    setState({
      ticker: {},
      book: { asks: [], bids: [] },
      trades: [],
      connected: false,
    });
    setCandles([]);
    setCandle(undefined);
    void api('query', {
      kind: 'rest',
      name: 'GET /api/v5/public/instruments',
      arguments: { instType: instrumentType(instrument), instId: instrument },
    })
      .then((result) => {
        if (disposed || current !== generation.current) return;
        const rows = dataOf(result);
        const definition = Array.isArray(rows)
          ? rows.find((row: Row) => row.instId === instrument)
          : undefined;
        setState((value) => ({ ...value, instrumentDefinition: definition }));
      })
      .catch(() => {});
    void api(`market/snapshot?${parameters}`)
      .then((snapshot) => {
        if (disposed || current !== generation.current) return;
        // Streaming data wins if it arrived while the bootstrap request was in flight.
        setState((value) => ({
          ...snapshot,
          ...value,
          ticker: value.ticker?.last ? value.ticker : snapshot.ticker,
          book: value.book?.bids?.length ? value.book : snapshot.book,
          trades: value.trades?.length ? value.trades : snapshot.trades,
        }));
        if (!buffer.trades.length) buffer.appendTrades(snapshot.trades || []);
      })
      .catch(() => {});
    const parseCandle = (values: string[]): Row => ({
      time: Number(values[0]) / 1000,
      open: Number(values[1]),
      high: Number(values[2]),
      low: Number(values[3]),
      close: Number(values[4]),
      volume: Number(values[5]),
    });
    void api(`market/candles?${parameters}`)
      .then((result) => {
        if (current === generation.current && !disposed)
          setCandles(
            result.data
              .map(parseCandle)
              .sort((a: Row, b: Row) => a.time - b.time),
          );
      })
      .catch((error) => {
        if (!disposed)
          setState((value) => ({ ...value, error: String(error) }));
      });
    const connect = async () => {
      await ensureSession();
      if (disposed) return;
      socket = new WebSocket(
        `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/api/market/stream?${parameters}`,
      );
      socket.onopen = () => {
        if (!disposed)
          setState((value) => ({ ...value, connected: true, error: '' }));
      };
      socket.onmessage = (message) => {
        if (disposed) return;
        const envelope = JSON.parse(message.data);
        const payload = envelope.payload;
        if (payload.event === 'error') {
          buffer.pending.error = payload.msg;
          return;
        }
        const channel = payload.arg?.channel;
        const data = payload.data;
        if (!data?.length) return;
        buffer.pending.receivedAt = envelope.receivedAt;
        buffer.pending.exchangeAt = Number(data[0]?.ts) || undefined;
        if (channel === 'tickers') buffer.pending.ticker = data[0];
        if (channel === 'books5') buffer.pending.book = data[0];
        if (channel === 'trades') buffer.appendTrades(data);
        if (channel?.startsWith('candle')) lastCandle = parseCandle(data[0]);
      };
      socket.onclose = () => {
        if (disposed) return;
        setState((value) => ({ ...value, connected: false }));
        reconnect = setTimeout(() => void connect(), 3000);
      };
    };
    void connect().catch((error) => {
      if (!disposed) setState((value) => ({ ...value, error: String(error) }));
    });
    const flush = setInterval(() => {
      const batch = buffer.drain();
      if (batch) setState((value) => ({ ...value, ...batch }));
      if (lastCandle) {
        setCandle(lastCandle);
        lastCandle = undefined;
      }
    }, 50);
    return () => {
      disposed = true;
      socket?.close();
      clearInterval(flush);
      clearTimeout(reconnect);
    };
  }, [instrument, bar, mode]);
  return { ...state, candles, candle };
}
