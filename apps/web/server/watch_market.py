"""Authenticated, filtered watchlist snapshots from a shared public market fetch."""
from __future__ import annotations

import asyncio
import copy
import re
import time


def parse_symbols(value):
    symbols = list(dict.fromkeys(value.split(',')))
    if not 1 <= len(symbols) <= 64 or any(not re.fullmatch(r'[A-Z0-9]{1,20}-USDT', s) for s in symbols):
        raise ValueError('请选择 1 至 64 个 USDT 现货交易对')
    return symbols


class PublicWatchMarket:
    def __init__(self, fetch, ttl=1.0):
        self.fetch = fetch
        self.ttl = ttl
        self.snapshot = None
        self.cached_at = 0.0
        self.pending = None

    async def _refresh(self):
        rows = await self.fetch()
        self.snapshot = {row['instId']: row for row in rows if isinstance(row, dict) and isinstance(row.get('instId'), str)}
        self.cached_at = time.monotonic()
        return self.snapshot

    async def read(self, symbols):
        if self.snapshot is None or time.monotonic() - self.cached_at > self.ttl:
            if self.pending is None:
                self.pending = asyncio.create_task(self._refresh())
            task = self.pending
            try:
                await asyncio.shield(task)
            finally:
                if task.done() and self.pending is task:
                    self.pending = None
        return {'tickers': [copy.deepcopy(self.snapshot[s]) for s in symbols if s in self.snapshot],
                'missing': [s for s in symbols if s not in self.snapshot],
                'observedAt': time.time() - (time.monotonic() - self.cached_at)}
