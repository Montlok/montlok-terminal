# Architecture

## Safety boundary

NautilusTrader owns market ingestion, strategy decisions, risk checks, and
exchange execution. It persists events locally before any terminal component
sees them. The adapter tails that durable log by offset and publishes to NATS
JetStream. Publication is asynchronous and bounded; backpressure cannot enter
the trading loop.

```text
research / DGX -> signed releases -> Tokyo inference + Nautilus live
                                           |
                              persisted event log
                                           |
                                      adapter
                                           v
                                    NATS JetStream
                                      |         |
                                projector    gateway
                                      |         |
                         SQLite/Arrow/Parquet  HTTPS/WSS
                                                |   |
                                              Web  Qt
```

## Event consistency

Every stream has an independent monotonic sequence. A subscription starts with
an initial Arrow snapshot and its sequence watermark, followed by incremental
events. A client detecting a sequence gap asks for replay from its last
contiguous sequence. The gateway replays from JetStream when retained data is
available and otherwise sends a new snapshot.

Order, route, fill, risk, control, model, position, and PnL events are never
conflated. Quote and book events may be conflated for rendering, while both the
conflated count and actual sequence loss remain visible.

## Query model

Clients submit structured dataset, field, filter, grouping, aggregation, sort,
time-range, cursor, and limit requests. Browsers never submit SQL. DataFusion
produces JSON for small responses or Arrow IPC for detail tables.

## Command model

Changing a tab, symbol, strategy group, run, model, or layout only updates
`TerminalContext`. It never changes execution state. Mutations use an expiring
prepared operation, an explicit execution request, and a queryable receipt.
Operation identifiers are idempotency keys; an uncertain response is queried,
not replayed.
