# Nautilus Operator visual specification

The approved production reference is `operator-console-approved.png` at 1536×960.

## Color lock

- Application canvas: `#f5f5f7`.
- Working surfaces: true white `#ffffff`.
- Primary text: `#1d1d1f`; secondary text: `#6e6e73`.
- Separator: `#e5e5e7`; strong separator: `#d2d2d7`.
- Primary action: Apple blue `#0071e3`.
- Positive: `#00a340`; warning: `#d96c00`; destructive: `#d70015`.
- No gradients, glow, dark terminal treatment, or tinted white content surfaces.

## Typography

- UI family: `-apple-system`, `BlinkMacSystemFont`, `SF Pro Text`, `Segoe UI`, sans-serif.
- Numeric family: `SF Mono`, `ui-monospace`, monospace where tabular alignment matters.
- Top-level values: 28–30px / 500.
- Section headings: 17–20px / 600.
- Controls: 13–14px / 500–600.
- Table and secondary data: 12–13px / 400–500.

## Layout and containers

- 62px top bar, 120px left rail, 44px footer at the 1536×960 reference viewport.
- Main content has a 36px horizontal gutter and a 14–16px vertical rhythm.
- Metrics are an open band, not cards.
- Reconciliation uses one slim amber banner.
- Chart and risk column use a single subtle frame each; no nested card grid.
- Positions, orders, exceptions, and strategies share one tabbed workspace.
- Destructive controls live under the More menu and always require confirmation.

## Component families

- Navigation: 20px outline icons, selected item on a pale blue field.
- Buttons: 36px height, 8px radius; blue primary, white secondary, quiet More control.
- Tabs: text labels with compact neutral count discs and a 2px blue selected underline.
- Status: 7px semantic dots and thin progress tracks; avoid decorative pills.
- Tables: open white surface, 40px rows, horizontal separators only.

## Interaction contract

- Pause and reduce-only commands open a confirmation sheet and are disabled outside an authenticated, trade-ready Operator API session.
- More reveals cancel/flatten controls but never sends an unsupported command.
- The four workspace tabs switch real local state.
- Navigation remains a compact primary-screen selector; the overview is the initial state.
- The chart is rendered code-native with TradingView Lightweight Charts and carries required attribution.
