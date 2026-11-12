# Operator design contract

The references are the operator's OKX trading screenshot and the Ant Design Pro +
Palantir Blueprint combination. The production UI implements them in code with
the actual upstream components.

- Ant Pro: application navigation, routes, settings, connection/API forms, data pages.
- Blueprint: dense operational data tables; Ant owns shared buttons, inputs and dialogs.
- The first screen is the trading surface: pair and price strip; chart, order book,
  order ticket; orders/fills/assets/strategy tabs below.
- Dark neutral background #101214, panel #15181b, divider #2a2e33, primary text #e6e8eb,
  secondary #939ba5, action blue #1677ff, bid green #25b77d, ask red #f05b65.
- Shared system/PingFang typography; 13px controls, 12px table labels, tabular numbers;
  compact 28–32px rows. Shared 4px radius and 8/12/16px spacing.
- The session and account identity stay visible. PAPER, OKX DEMO and live read-only
  are separate data contexts; public market prices carry their own label.
- Values come from the exchange. Missing data shows an explicit unavailable state.
  Market data is streamed; account timestamps are shown separately.
- API tools have searchable schemas, actual read/write classification, confirmation
  receipts, and explanations for unsupported or policy-blocked calls.
- Credential changes stay server-side and require validation and confirmation.
- Desktop first; below 1100px the order ticket moves below the chart; below 760px the
  navigation collapses and tables scroll inside their panels.

Acceptance checks: layout hierarchy, typography, neutral palette, compact table density,
pair/timeframe navigation, real-time update timestamps, API forms, key-switch workflow,
confirmation cancellation, writes only after confirmation, responsive panel bounds.

## 2026-09-05 copy and navigation revision

This revision works inside the accepted Ant/Blueprint system and the existing dark
trading layout.

- Login shows the brand, account, password and the login button.
- Primary navigation: 行情 / 交易 / 资产 / 策略 / 引擎 / 设置.
- Ant Pro split menus: primary categories in the header, product groups and task pages in
  the sidebar. The active product group is open. Deep links survive refresh.
- Secondary groups name products; leaf routes name tasks.
- Common tasks show concise Chinese operation labels, field forms and tabular results.
- Technical descriptions use tooltips/disclosure. Raw requests/responses and source-method
  directories sit under advanced views; source callbacks are listed apart from remote controls.
- Persistent context is what changes a decision: active account, Demo/live status, market
  source, connection freshness and the scope of a pending operation.
- Button labels are actions: 查询 / 提交 / 保存 / 编辑 / 撤单 / 确认 / 取消.
- Framework attribution and licences live in the repository docs.

QA comparison points: removed copy, top/side menu hierarchy, system typography, dark-neutral
palette, chart/book/ticket structure, form controls, confirmation scope and responsive bounds.
