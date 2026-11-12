# Montlok 交易操作台

Weilan（NautilusTrader）引擎的交易与运维前端，以 OKX 为主。
技术栈：React 19 · Umi Max 4 · antd 6 + ProComponents（导航、表单）· Blueprint 6
（高密度表格）· TradingView lightweight-charts（K 线）。后端是 `server/` 中的 aiohttp
BFF（会话、CSRF、凭据保险库、OKX REST/WS 转发、策略组 supervisor）。
第三方来源见 [UPSTREAM.md](./UPSTREAM.md)，设计约定见 [DESIGN.md](./DESIGN.md)。

语言：[English](./README.md) | 简体中文

## 常用命令

| 任务 | 命令 |
| --- | --- |
| 首次检出 | `npm ci && npx max setup`（生成 `src/.umi`，提供 `@umijs/max` 类型） |
| 开发服务器（`/api` 代理到 `127.0.0.1:18081`） | `npm run dev` |
| Lint + 类型检查 | `npm run lint`（`biome lint` + `tsc`）、`npx antd lint ./src` |
| 格式化 | `npm run biome` |
| 单元测试 | `npm run test`（Vitest，happy-dom） |
| 生产构建 | `npm run build` → `dist/`（由 `server/app.py --static-dir` 提供） |
| 后端测试 | `cd server && python -m pytest tests` |
| 引擎测试 | `cd engine && PYTHONPATH=.:../server python -m unittest test_live_hft_worker test_live_model_worker` |

Node ≥ 22，使用 `package-lock.json`；commitlint 校验 Conventional Commits。

## 目录结构

```
config/        Umi 配置；路由由 src/operator/navigation.ts 生成
src/app.tsx    运行时布局、antd 暗色主题、会话初始化（getInitialState）
src/models/    useModel('operator')：账户、模拟盘、连接、目录、SSE /api/events
src/operator/  共享组件与纯逻辑（见下）
src/pages/     每个路由一个目录：Terminal、Account、ApiWorkbench、Engine、
               Server、Connections、Operations、OperatorLogin、exception/404
src/locales/   zh-CN（404 页使用 useIntl）
server/        aiohttp BFF、OKX 目录（server/catalog/*.json）、策略组 supervisor、测试
engine/        Nautilus 策略 worker 与模型推理（GROUP_RUNTIME.md、MODEL_RUNTIME.md）
deploy/        Nginx/TLS 配置、备份与发布脚本
docs/          Ant Design Pro 速查与路线图（上游参考）
```

`src/operator/`：

- `api.ts` — 同源 `fetch`，处理会话 / CSRF；`dataOf`、`number`、`timeOf` 格式化。
- `useMarket.ts` + `marketBuffer.ts` — 每个品种一条 WebSocket，帧合并后每 50 ms 刷新一次。
- `MarketChart.tsx`、`DepthChart.tsx`、`OrderBook.tsx`、`DataGrid.tsx`、`OrderTicket.tsx` — memo 化组件；一次行情 tick 只重绘数据有变化的组件。
- `Watchlist.tsx` + `watchlistModel.ts` + `usePoll.ts` — 持久化多品种监控（localStorage `operator.watchlist`，标签页可见时每 5 秒轮询）。
- `indicators.ts` — 均线与累计深度（纯函数，有单元测试）。
- `theme.css` — 暗色配色的 `--op-*` 变量与终端 4/3/2/1 列网格。

## 约定

- 纯逻辑放在 `*Model.ts` / `indicators.ts` / `marketBuffer.ts`，旁边配 `*.test.ts`；组件保持轻薄并 memo 化。
- 只读交易所查询走 `POST /api/query`；写操作走 `prepare` → `ConfirmOperation`。实盘连接由服务端策略设为只读。
- 页面展示真实交易所数据；缺失数据显示为 “—” 或不可用状态。
- 路由组件路径在 Linux 上区分大小写（`./Account` ↔ `src/pages/Account/`）。
- `src/services/ant-design-pro/` 为自动生成代码；格式化与 lint 使用 Biome。
- 初次使用某个 antd API 前，用 `npx antd info <Component>` 查看说明。

## 部署

`npm run build` 后把 `dist/` 复制到主机的 `--static-dir`，用 `server/nautilus-operator-terminal.service`
运行 `server/app.py`，前置 Nginx TLS（`deploy/nginx.conf`、`deploy/https.conf`）。交易所凭据保存在
服务端保险库中，信任模型见 [SECURITY.md](../../SECURITY.md)。
