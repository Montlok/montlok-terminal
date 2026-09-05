import type { Row } from './api';

/** Captures a batch by value before React may defer the state updater. */
export class MarketBuffer {
  pending: Row = {};
  trades: Row[] = [];
  tradesChanged = false;
  appendTrades(rows: Row[]) {
    this.trades = [...rows, ...this.trades].slice(0, 60);
    this.tradesChanged = true;
  }
  drain(): Row | undefined {
    if (!Object.keys(this.pending).length && !this.tradesChanged) return undefined;
    const batch = {...this.pending, ...(this.tradesChanged ? {trades:this.trades} : {})};
    this.pending = {};
    this.tradesChanged = false;
    return batch;
  }
}
