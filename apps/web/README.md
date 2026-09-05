# Montlok operator terminal

Trading and operations UI for the Weilan (NautilusTrader) engine, OKX first.
React 19 · Umi Max 4 · antd 6 + ProComponents (navigation, forms) · Blueprint 6
(dense tables) · TradingView lightweight-charts (K lines). The backend is the
aiohttp bridge in `server/` (sessions, CSRF, credential vault, OKX REST/WS relay,
paper-engine snapshot). Provenance of vendored code: [UPSTREAM.md](./UPSTREAM.md).
Design contract: [DESIGN.md](./DESIGN.md). Production hardening record:
[SECURITY_DEPLOYMENT.md](./SECURITY_DEPLOYMENT.md).

Language: English | [简体中文](./README.zh-CN.md)

## Commands

| Task | Command |
| --- | --- |
| First checkout | `npm ci && npx max setup` (generates `src/.umi`, required for `@umijs/max` types) |
| Dev server (proxies `/api` to `127.0.0.1:18081`) | `npm run dev` |
| Lint + type-check | `npm run lint` (`biome lint` + `tsc`), `npx antd lint ./src` |
| Auto-format | `npm run biome` |
| Unit tests | `npm run test` (Vitest, happy-dom) |
| Production build | `npm run build` → `dist/` (served by `server/app.py --static-dir`) |
| Backend tests | `cd server && python -m pytest tests` |

Node ≥ 22, `package-lock.json` only. Conventional commits are enforced by commitlint.

## Layout

```
config/        Umi config; routes are generated from src/operator/navigation.ts
src/app.tsx    runtime layout, dark antd theme, session bootstrap (getInitialState)
src/models/    useModel('operator'): account, paper, profiles, catalog, SSE /api/events
src/operator/  shared widgets and pure logic (see below)
src/pages/     one directory per route: Terminal, Account, ApiWorkbench, Engine,
               Server, Connections, Operations, OperatorLogin, exception/404
src/locales/   zh-CN only (the 404 page is the only useIntl consumer)
server/        aiohttp bridge, OKX catalog (server/catalog/*.json), systemd unit, tests
```

`src/operator/`:

- `api.ts` — same-origin `fetch` with session/CSRF handling; `dataOf`, `number`, `timeOf` formatters.
- `useMarket.ts` + `marketBuffer.ts` — one WebSocket per instrument, frames coalesced and flushed every 50 ms.
- `MarketChart.tsx`, `DepthChart.tsx`, `OrderBook.tsx`, `DataGrid.tsx`, `OrderTicket.tsx` — memoized widgets; a tick only re-renders the widget whose data changed.
- `Watchlist.tsx` + `watchlistModel.ts` + `usePoll.ts` — persistent multi-instrument monitor (localStorage `operator.watchlist`, 5 s polling only while the tab is visible).
- `indicators.ts` — moving averages and cumulative depth (pure, unit-tested).
- `theme.css` — the whole dark palette via `--op-*` tokens and the 4/3/2/1-column terminal grid.

## Conventions

- Pure logic lives in `*Model.ts` / `indicators.ts` / `marketBuffer.ts` and has a
  `*.test.ts` next to it; components stay thin and memoized.
- Read-only exchange calls go through `POST /api/query`; every write goes through
  `prepare` → `ConfirmOperation`. Live profiles are read-only by server policy.
- Real exchange values only; missing data renders an explicit "—" / unavailable state.
- Route component paths are case-sensitive on Linux (`./Account` ↔ `src/pages/Account/`).
- Never edit `src/services/ant-design-pro/` by hand (generated). Biome only — no ESLint/Prettier.
- Run `npx antd info <Component>` before using an unfamiliar antd API.

## Deployment

Build with `npm run build`, copy `dist/` to the host's `--static-dir`, and run
`server/app.py` under the provided systemd unit (`server/nautilus-operator-terminal.service`)
behind Nginx TLS as described in [SECURITY_DEPLOYMENT.md](./SECURITY_DEPLOYMENT.md).
No exchange credentials are ever part of the frontend build.
