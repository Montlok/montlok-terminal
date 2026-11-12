# Montlok operator terminal

Trading and operations UI for the Weilan (NautilusTrader) engine, OKX first:
React 19 · Umi Max 4 · antd 6 + ProComponents (navigation, forms) · Blueprint 6
(dense tables) · TradingView lightweight-charts (K lines). The backend is the
aiohttp BFF in `server/` (sessions, CSRF, credential vault, OKX REST/WS relay,
strategy-group supervisor). Vendored code: [UPSTREAM.md](./UPSTREAM.md). Design
contract: [DESIGN.md](./DESIGN.md).

Language: English | [简体中文](./README.zh-CN.md)

## Commands

| Task | Command |
| --- | --- |
| First checkout | `npm ci && npx max setup` (generates `src/.umi` for the `@umijs/max` types) |
| Dev server (proxies `/api` to `127.0.0.1:18081`) | `npm run dev` |
| Lint + type-check | `npm run lint` (`biome lint` + `tsc`), `npx antd lint ./src` |
| Format | `npm run biome` |
| Unit tests | `npm run test` (Vitest, happy-dom) |
| Production build | `npm run build` → `dist/` (served by `server/app.py --static-dir`) |
| Backend tests | `cd server && python -m pytest tests` |
| Engine tests | `cd engine && PYTHONPATH=.:../server python -m unittest test_live_hft_worker test_live_model_worker` |

Node ≥ 22 with `package-lock.json`; commitlint enforces conventional commits.

## Layout

```
config/        Umi config; routes are generated from src/operator/navigation.ts
src/app.tsx    runtime layout, dark antd theme, session bootstrap (getInitialState)
src/models/    useModel('operator'): account, paper, profiles, catalog, SSE /api/events
src/operator/  shared widgets and pure logic (see below)
src/pages/     one directory per route: Terminal, Account, ApiWorkbench, Engine,
               Server, Connections, Operations, OperatorLogin, exception/404
src/locales/   zh-CN (the 404 page uses useIntl)
server/        aiohttp BFF, OKX catalog (server/catalog/*.json), group supervisor, tests
engine/        Nautilus strategy workers and model inference (GROUP_RUNTIME.md, MODEL_RUNTIME.md)
deploy/        Nginx/TLS configuration, backup, publication scripts
docs/          Ant Design Pro cheatsheet and roadmap (upstream reference)
```

`src/operator/`:

- `api.ts` — same-origin `fetch` with session/CSRF handling; `dataOf`, `number`, `timeOf` formatters.
- `useMarket.ts` + `marketBuffer.ts` — one WebSocket per instrument, frames coalesced and flushed every 50 ms.
- `MarketChart.tsx`, `DepthChart.tsx`, `OrderBook.tsx`, `DataGrid.tsx`, `OrderTicket.tsx` — memoized widgets; a tick re-renders the widget whose data changed.
- `Watchlist.tsx` + `watchlistModel.ts` + `usePoll.ts` — persistent multi-instrument monitor (localStorage `operator.watchlist`, polled every 5 s while the tab is visible).
- `indicators.ts` — moving averages and cumulative depth (pure, unit-tested).
- `theme.css` — the dark palette as `--op-*` tokens and the 4/3/2/1-column terminal grid.

## Conventions

- Pure logic lives in `*Model.ts` / `indicators.ts` / `marketBuffer.ts` with a
  `*.test.ts` beside it; components stay thin and memoized.
- Read-only exchange calls go through `POST /api/query`; writes go through
  `prepare` → `ConfirmOperation`. Live profiles are read-only by server policy.
- Screens show real exchange values; missing data renders as "—" or an
  unavailable state.
- Route component paths are case-sensitive on Linux (`./Account` ↔ `src/pages/Account/`).
- `src/services/ant-design-pro/` is generated. Biome formats and lints the code.
- `npx antd info <Component>` documents an antd API before first use.

## Deployment

Build with `npm run build`, copy `dist/` to the host's `--static-dir`, and run
`server/app.py` under `server/nautilus-operator-terminal.service` behind Nginx
TLS (`deploy/nginx.conf`, `deploy/https.conf`). Exchange credentials stay in the
server's vault; [SECURITY.md](../../SECURITY.md) covers the trust model.
