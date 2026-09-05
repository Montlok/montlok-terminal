# Operator design contract

Approved references: the user's OKX trading screenshot and explicit Ant Design Pro +
Palantir Blueprint combination. These existing product references are the specification;
the production UI is code-native and uses actual upstream components.

- Ant Pro: application navigation, routes, settings, connection/API forms, data pages.
- Blueprint: dense operational data tables; Ant owns shared buttons, inputs and dialogs.
- Main trading surface: pair and price strip; chart, order book, order ticket; lower
  orders/fills/assets/strategy tabs. No marketing header or large decorative KPI cards.
- Dark neutral background #101214, panel #15181b, divider #2a2e33, primary text #e6e8eb,
  secondary #939ba5, action blue #1677ff, bid green #25b77d, ask red #f05b65.
- Shared system/PingFang typography; 13px controls, 12px table labels, tabular numbers;
  compact 28–32px rows. Shared 4px radius and 8/12/16px spacing.
- A session and account identity is always visible. PAPER, OKX DEMO and live read-only
  are separate data contexts; public market prices are identified independently.
- Real exchange values only. Missing data is an explicit unavailable state, never
  generated candle or balance data. Market data is streamed; account timestamps are
  independently visible.
- API tools have searchable schemas, actual read/write classification, confirmation
  receipts, and explicit unsupported/policy-blocked explanations.
- Credential changes stay server-side and require validation and confirmation.
- Desktop first; below 1100px the order ticket moves below the chart; below 760px the
  navigation collapses and tables scroll inside their panels.

Acceptance checks: layout hierarchy, typography, neutral palette, compact table density,
pair/timeframe navigation, real-time update timestamps, API forms, key-switch workflow,
confirmation cancellation, no unsolicited write execution, responsive panel bounds.

## 2026-09-05 copy and navigation revision

This is a correction inside the accepted Ant/Blueprint design system. No new image concept,
decorative assets or framework switch is needed. Keep the existing dark trading layout.

- Login: brand, account, password, login. No deployment-location subtitle or security claims.
- Primary navigation: 行情 / 交易 / 资产 / 策略 / 引擎 / 设置.
- Ant Pro split menus: primary categories in the header, product groups and task pages in
  the sidebar. Only the active product group needs to be open. Deep links survive refresh.
- Secondary groups describe products; leaf routes describe tasks, not transport protocols.
- Common tasks display concise Chinese operation labels, field forms and tabular results.
- Technical descriptions use tooltips/disclosure. Raw requests/responses and source-method
  directories belong under advanced views. Do not equate source callbacks with remote controls.
- Keep only decision-changing context persistent: active account, Demo/live status, market
  source, connection freshness and the scope of a pending operation.
- Button labels are actions: 查询 / 提交 / 保存 / 编辑 / 撤单 / 确认 / 取消.
- No framework attribution in the working UI; license/provenance remains in repository docs.

QA comparison points: copy deletion, top/side menu hierarchy, system typography, dark-neutral
palette, chart/book/ticket structure, form controls, confirmation scope and responsive bounds.
