# Montlok 交易操作台

Weilan（NautilusTrader）引擎的交易 / 运维前端，以 OKX 为主。
技术栈：React 19 · Umi Max 4 · antd 6 + ProComponents（导航、表单）· Blueprint 6
（高密度表格）· TradingView lightweight-charts（K 线）。后端是 `server/` 中的 aiohttp
桥接服务（会话、CSRF、凭据保险库、OKX REST/WS 转发、模拟引擎快照）。
第三方来源见 [UPSTREAM.md](./UPSTREAM.md)，设计约定见 [DESIGN.md](./DESIGN.md)，
生产加固记录见 [SECURITY_DEPLOYMENT.md](./SECURITY_DEPLOYMENT.md)。

语言：[English](./README.md) | 简体中文

## 常用命令

| 任务 | 命令 |
| --- | --- |
| 首次检出 | `npm ci && npx max setup`（生成 `src/.umi`，否则 `@umijs/max` 类型缺失） |
| 开发服务器（`/api` 代理到 `127.0.0.1:18081`） | `npm run dev` |
| Lint + 类型检查 | `npm run lint`（`biome lint` + `tsc`）、`npx antd lint ./src` |
| 自动格式化 | `npm run biome` |
| 单元测试 | `npm run test`（Vitest，happy-dom） |
| 生产构建 | `npm run build` → `dist/`（由 `server/app.py --static-dir` 提供） |
| 后端测试 | `cd server && python -m pytest tests` |

Node ≥ 22，只使用 `package-lock.json`。提交信息需符合 Conventional Commits（commitlint 强制）。

## 目录结构

```
config/        Umi 配置；路由由 src/operator/navigation.ts 生成
src/app.tsx    运行时布局、antd 暗色主题、会话初始化（getInitialState）
src/models/    useModel('operator')：账户、模拟盘、连接、目录、SSE /api/events
src/operator/  共享组件与纯逻辑（见下）
src/pages/     每个路由一个目录：Terminal、Account、ApiWorkbench、Engine、
               Server、Connections、Operations、OperatorLogin、exception/404
src/locales/   仅 zh-CN（只有 404 页使用 useIntl）
server/        aiohttp 桥接、OKX 目录（server/catalog/*.json）、systemd 单元、测试
```

`src/operator/`：

- `api.ts` — 同源 `fetch`，处理会话 / CSRF；`dataOf`、`number`、`timeOf` 格式化。
- `useMarket.ts` + `marketBuffer.ts` — 每个品种一条 WebSocket，帧合并后每 50 ms 刷一次。
- `MarketChart.tsx`、`DepthChart.tsx`、`OrderBook.tsx`、`DataGrid.tsx`、`OrderTicket.tsx` — memo 化组件；一次行情 tick 只重绘数据变化的组件。
- `Watchlist.tsx` + `watchlistModel.ts` + `usePoll.ts` — 持久化多品种监控（localStorage `operator.watchlist`，标签页可见时每 5 秒轮询）。
- `indicators.ts` — 均线与累计深度（纯函数，有单元测试）。
- `theme.css` — 全部暗色配色通过 `--op-*` 变量；终端 4/3/2/1 列自适应网格。

## 约定

- 纯逻辑放在 `*Model.ts` / `indicators.ts` / `marketBuffer.ts`，旁边配 `*.test.ts`；组件保持薄且 memo 化。
- 只读交易所查询走 `POST /api/query`；所有写操作走 `prepare` → `ConfirmOperation`。实盘连接由服务端策略强制只读。
- 只展示真实交易所数据；缺失数据显示明确的 “—” / 不可用状态。
- 路由组件路径在 Linux 上区分大小写（`./Account` ↔ `src/pages/Account/`）。
- 不要手改 `src/services/ant-design-pro/`（自动生成）。只用 Biome，不用 ESLint / Prettier。
- 使用不熟悉的 antd API 前先 `npx antd info <Component>`。

## 部署

`npm run build` 后把 `dist/` 复制到主机的 `--static-dir`，用提供的 systemd 单元
（`server/nautilus-operator-terminal.service`）运行 `server/app.py`，前置 Nginx TLS，
详见 [SECURITY_DEPLOYMENT.md](./SECURITY_DEPLOYMENT.md)。前端构建产物中永远不包含交易所凭据。
