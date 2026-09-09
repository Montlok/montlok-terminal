# Source-stamped run observations

`GET /api/v2/strategy-groups/{id}/observation` reads the existing production
run snapshots without going through the legacy HTTP aggregation path.
`run_id` selects a specific run; omission resolves the current or most recent
run from the supervisor's read-only status command. The `include` parameter
selects `universe`, `positions`, `orders`, `fills`, `execution` and `model`.

The gateway validates the directory, immutable manifest, group/run/account
identities, and source timestamps. Monetary values and percentage calculations
use BigDecimal. Missing data stays null, and positions/orders/fills are marked
unavailable when their snapshot differs from the status timestamp by more than
five seconds. A completed run is historical data, not an active market feed.

The result carries actual source names, observation timestamps and the
calculation version. `reported_run_mark_to_market` describes the existing run
marking convention; it does not assert that a new cash-flow-adjusted accounting
ledger has been implemented. Snapshot rows are bounded and total counters are
reported separately from the rows currently present in the source view.

The Web query cache shares one observation between the market dock, metric
inspector and object search. Selecting an instrument changes observation only.
Every metric in the inspector exposes its original value, unit, execution
account, run and source. The original v1 actions and confirmation state remain
independent of these reads. A rolling deployment may use the same live v1
snapshot only if the v2 route returns 404; permission and consistency failures
are never hidden by that fallback.
