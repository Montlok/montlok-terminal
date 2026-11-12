# Architecture

## Safety boundary

NautilusTrader owns market ingestion, strategy decisions, risk checks, and
exchange execution. It persists events locally before any terminal component
sees them. The adapter tails that durable log by offset and publishes to NATS
JetStream. Publication is asynchronous and bounded; the trading loop keeps its
own pace.

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

Order, route, fill, risk, control, model, position and PnL events are delivered
one by one. Quote and book events may be conflated for rendering; the conflated
count and any sequence loss stay visible.

## Query model

Clients submit structured dataset, field, filter, grouping, aggregation, sort,
time-range, cursor, and limit requests, which the server plans in DataFusion.
Small responses are JSON; detail tables are Arrow IPC.

## Command model

Changing a tab, symbol, strategy group, run, model or layout updates
`TerminalContext`. Execution state changes through mutations: an expiring
prepared operation, an explicit execution request and a queryable receipt.
Operation identifiers are idempotency keys; after an uncertain response the
client queries the receipt.
