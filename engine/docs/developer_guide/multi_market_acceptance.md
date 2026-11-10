# Multi-market acceptance contract

This contract prevents transport availability or schema coverage from being reported as market
support. It applies independently to cryptocurrency, mainland China A-shares, Hong Kong equities,
and United States equities.

## Support levels

A market is supported only after every lower level is satisfied by deterministic tests and
content-addressed evidence.

| Level | Required evidence |
|---|---|
| L0: schema and provenance | Raw-source digest, decoder identity, materialized digest, and every market-context role bound to its market, venue, and effective range. Wrong-role, wrong-venue, stale, missing, and substituted artifacts fail before strategy startup. |
| L1: normalization | Authoritative instrument snapshots with golden assertions for symbol, listing venue, currencies, tick, lot, multiplier, and effective timestamps. |
| L2: market semantics | Calendars, time zones, phases and auctions, tick and lot rules, price controls, fees, settlement, short-sale rules, corporate actions, and FX policy alter matching and accounting as specified. |
| L3: adapter contract | Recorded submit, amend, cancel, status, fill, reconnect, and reconciliation scenarios, including duplicate-message idempotency. |
| L4: sandbox or paper reconciliation | End-to-end order lifecycle, reconnect recovery, and broker or venue reconciliation for balances, positions, and fees. |

Passing L0 alone means **schema modeled**. Passing transport tests alone means **adapter transport
available**. Neither may be described as market support.

## Current capability statement

| Market | Current status | Claim permitted |
|---|---|---|
| OKX cryptocurrency | Native HTTP/WebSocket data and execution adapter. The typed `materialize_swap_backtest_environment` path turns exact raw instrument-response bytes into canonical schema-v3 environment bytes and a source/output `DerivationRecord`. `MarketProfile::GenericCryptoPerpetualV1` still has no OKX fee, funding, margin-tier, account-mode, lifecycle, mark/index, or price-control applicator. One pinned swap fixture covers only part of normalization, so L1 is incomplete. | OKX connector, content-addressed instrument-environment materialization, and generic perpetual simulation. Never an OKX-fidelity simulation claim. |
| Mainland China A-shares | Generic equity model and schema-level context requirements. Equity context applicators and a domestic venue adapter are absent. | Schema modeled only. |
| Hong Kong equities | Generic equity model and Interactive Brokers transport components. Market-rule applicators are absent. | Broker transport components available; schema modeled. |
| United States equities | Generic equity model, Databento data components, and Interactive Brokers transport components. Integrated market-rule applicators are absent. | Data and broker components available; schema modeled. |

The secure backtest environment must fail closed for a context it cannot decode and apply. A
successful round trip of an equity environment is not an executable equity backtest.

## Operational release state of this branch

This branch is a research and DEMO/paper foundation, not a real-money release. The hardened OKX
transport and execution paths have deterministic local tests, but no credentialed OKX DEMO session
or broker reconciliation run is part of the repository evidence. The execution examples default to
`DEMO`, disable risk bypass, and keep dry-run or explicit operator gates where the example supports
them.

Canonical production event-store boot requires an explicitly pinned isolated verifier. Its current
protocol verifies and crash-seals stores but does not return bounded cache or risk-policy projection
DTOs. Automatic predecessor recovery therefore remains `Halted` behind an incomplete-recovery
interlock, and configured production replay is unavailable. Ordinary state changes, reset, and risk
bypass cannot clear that interlock; an authenticated durable operator re-arm protocol has not been
implemented. External result promotion is also disabled until an authenticated runner or
evaluator-owned semantic reexecution path exists. These safe-disabled boundaries must not be
reported as live readiness. The recovery interlock belongs to the Rust v2 risk/kernel path; the
legacy Cython trading node is not an eligible strict-durability or release path.

## Identity and routing boundary

Listing identity and execution routing are distinct in environment schema v3:

- `ListingVenue::Exchange` identifies the exchange carried by the instrument ID. Schema v3 uses
  Nautilus venue identifiers and does not claim they are all ISO MICs.
- `ExecutionRoute::DirectVenue` identifies a directly addressed venue.
- `ExecutionRoute::BrokerSmartRouter` represents broker routing without pretending the router is a
  listing venue.
- `ExecutionRoute::StockConnect` is distinct from direct mainland exchange access.

Every equity venue carries listing contexts. When execution is not direct on that same listing
venue, schema v3 additionally requires a complete, separately content-addressed
`execution_contexts` set whose `MarketContextTarget::ExecutionRoute` exactly repeats the typed
route. Listing evidence cannot be substituted for ARCX execution, a broker smart router, or Stock
Connect. The runtime still refuses these artifacts until route-aware applicators are installed.

An instrument such as `AAPL.XNAS` may execute on another US venue. `SMART` is not a listing venue,
and schema validation rejects it as one. `StockConnect` is accepted structurally only for
`MarketKind::ChinaAShares`; this typed route is not proof that a domestic or Stock Connect adapter
has been installed.

The profiles currently present in code are:

- `MarketProfile::GenericCryptoPerpetualV1`;
- `MarketProfile::ChinaASharesSchemaV1`;
- `MarketProfile::HongKongEquitiesSchemaV1`; and
- `MarketProfile::UnitedStatesEquitiesSchemaV1`.

The three equity profiles are structural contracts only. The secure runtime rejects them until
their context artifacts have library-owned decoders and applicators.

`MarketProfile::stable_id`, `ListingVenue::stable_id`, and `ExecutionRoute::stable_id` own the
canonical identities consumed by provenance and promotion scopes; callers must not derive these
identities from debug formatting. Provenance retains them as one canonical, sorted
materialized-venue/profile/listing/route binding and requires its venue keys to exactly match the
materialized environment. It never sorts semantic identities into independent sets, which would
erase the route-to-listing relationship or permit a same-count venue substitution.

## Valuation boundary

Native-currency ledgers may operate without conversion only when no cross-currency aggregate is
produced. Any unified portfolio value, cross-market allocation, or common-currency evaluation must
bind a `ValuationPolicyBinding` containing:

- base currency;
- one content-addressed policy/series bundle;
- bid, ask, or midpoint policy;
- maximum quote age;
- triangulation policy;
- valuation cutoff policy; and
- coverage for the full replay interval and every required conversion path.

Missing, stale, future-dated, or ambiguous conversion paths fail before strategy startup. USD and
USDT are distinct currencies and never receive an implicit one-to-one conversion.

Schema v3 binds base currency, `ValuationRateSide`, maximum quote age,
`ValuationTriangulationPolicy`, `ValuationCutoffPolicy`, effective range, and an immutable artifact.
It validates full replay-range coverage and content identity, but the runtime intentionally rejects
every valuation policy because the FX resolver/applicator is not installed yet. Path coverage and
quote freshness cannot be claimed until that applicator exists. Raw-source to materialized-series
lineage is also not present in `ValuationPolicyBinding`; it remains a required production-pipeline
gate rather than a current schema-v3 capability.

## Replay and artifact identity boundary

Every environment binds `[replay_start_ns, replay_end_ns)` and `knowledge_cutoff_ns`, with the
knowledge cutoff no later than replay start. Each market context and valuation policy must cover the
complete replay interval. The schema also rejects one artifact ID mapped to different content
digests and, until semantic envelopes are decoded, conservatively rejects any content digest reused
by more than one environment binding. These checks attest the envelope; they do not decode or apply
the referenced market-context bytes. Equity execution therefore remains fail-closed.

Schema v3 also binds publication and authenticated-observation timestamps for every market context
and valuation policy, requiring `published_at <= observed_at <= knowledge_cutoff`. Instrument
definitions similarly require `ts_event <= ts_init <= knowledge_cutoff`. These checks prevent an
obviously future snapshot from entering a canonical run; production ingestion must additionally
attest those timestamps in raw-source-to-materialized derivation records, because a caller-supplied
timestamp alone is not an independent proof of when information became available.

For OKX swaps, the adapter-owned materializer accepts the raw response bytes, a source artifact ID,
one exact instrument ID, observation time, replay bounds, and settlement-currency balance. It
re-parses and validates those bytes itself, rejects failed/non-live/non-swap definitions, emits a
content-addressed environment artifact, and binds the raw input and canonical output in a v3
derivation record. The record proves transformation lineage, not remote-server authenticity;
production ingestion must obtain the bytes through an authenticated transport and preserve the
capture receipt separately.

## Market-specific deterministic gates

### OKX cryptocurrency

- authoritative swap instrument fixture, including contract multiplier, quantity increment, lot,
  min/max quantity, and price increment;
- maintenance, suspend, and delist transitions;
- funding boundaries and linear/inverse accounting;
- account and position mode, reduce-only behavior, and reconnect reconciliation;
- fee tier, product, and account-specific fees; and
- dynamic exchange price-control rules.

### Mainland China A-shares

- `Asia/Shanghai` holidays, special sessions, midday break, and open/close auctions;
- board-, ST-, and IPO-effective price limits;
- board-lot buys, odd-lot sells, and tick rules;
- T+1 sellable-share inventory distinct from settled cash;
- commissions, minimum commission, transfer fees, and sell-side tax; and
- splits, dividends, rights, suspensions, and symbol changes.

### Hong Kong equities

- `Asia/Hong_Kong` holidays and special sessions, pre-open, and closing auction;
- per-security board lots, odd lots, and variable price tables;
- volatility-control and cooling-period boundaries;
- settlement and statutory levy schedules by effective date;
- designated short selling; and
- dual-counter instruments and explicit FX conversion.

### United States equities

- `America/New_York` daylight-saving changes, early closes, regular and extended sessions;
- opening and closing auctions;
- listing venue separated from execution venue;
- LULD boundaries, halts, resumes, and short-sale restrictions;
- round- and odd-lot behavior;
- settled-cash, borrow, and locate accounting; and
- splits, dividends, mergers, and symbol changes.

## Promotion rule

Promotion evidence must name the exact market profile and all applied context artifacts. Release
promotion rejects unscoped execution environments. The evaluator-signed attestation, signed
strategy manifest, and host trust policy must carry the same canonical paired-market-scope
commitment; matching deployment-scope text alone is insufficient. A release cannot be promoted
for a market whose required acceptance level is incomplete, and evidence from one market profile,
listing, or route cannot authorize another.

The serialized external-evaluator request is an audit and transport envelope, not proof of result
semantics. Its statistics and `run_inputs_verified` value originate in runner-supplied result
material; a matching result digest, provenance commitment, and clean event-store receipt show that
those claims are mutually consistent, but the event store does not independently derive portfolio
statistics. Production verification therefore returns `AuthenticatedRunnerRequired` and exposes no
signable bytes. Enabling it requires either evaluator-owned deterministic semantic reexecution or
an evaluator-policy trusted runner signature that binds the measured runner binary digest, exact
configuration and input/provenance commitments, complete result/evidence digest, and exact isolated
event-store receipt. Private `cfg(test)` consistency-pipeline helpers are mechanism tests only and
must never authorize a release.
