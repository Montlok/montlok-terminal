# Source-stamped run observations

`GET /api/v2/strategy-groups/{id}/observation` reads the production run
snapshots directly.
`run_id` selects a specific run; omission resolves the current or most recent
run from the supervisor's read-only status command. The `include` parameter
selects `universe`, `positions`, `orders`, `fills`, `execution` and `model`.

The gateway validates the directory, immutable manifest, group/run/account
identities, and source timestamps. Monetary values and percentage calculations
use BigDecimal. Missing data stays null, and positions/orders/fills are marked
unavailable when their snapshot differs from the status timestamp by more than
five seconds. A completed run reads as historical data.

The result carries actual source names, observation timestamps and the
calculation version. `reported_run_mark_to_market` names the run's existing
marking convention. Snapshot rows are bounded and total counters are
reported separately from the rows currently present in the source view.

The Web query cache shares one observation between the market dock, metric
inspector and object search. Selecting an instrument changes observation only.
Every metric in the inspector exposes its original value, unit, execution
account, run and source. The v1 actions and confirmation state are separate
from these reads. During a rolling deployment the client reads the live v1
snapshot when the v2 route returns 404; permission and consistency errors are
shown as they are.
