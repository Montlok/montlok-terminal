# Execution detail

Selecting an event is a read. Web and native clients use `GET /api/v2/events/{event_id}/related?limit=500` to investigate it.
The endpoint requires the existing browser session or signed device request.

## Indexed history

The Rust gateway writes link keys in the same SQLite transaction as its replay
journal. Each link is scoped to the account and run, and the query also checks
the strategy group. The traversal visits each link key once, so duplicate paths
and cycles expand once. Order, route,
venue-order, trade and position identities connect records whose correlation
IDs are absent or differ. Inference, signal, target decision, risk decision,
operation and explicit causation identities use the same traversal.

File-backed detail queries use their own read-only WAL connections on the
blocking pool, apart from the event-ingestion writer. Each request observes one
consistent read transaction. The reader opens the journal read-only and holds
no exchange credentials.

Responses contain Base64-encoded Protobuf envelopes, an explicit encoding
identifier, and `truncated`. The default limit is 500, with an upper bound of
2,048. A limited response always includes the selected event. Returned events
are ordered by occurrence time, stream and sequence; sequence numbers from
different streams are not interpreted as one global counter.

## Client presentation

- Summary: identity, source, account/group/run, event and receive timestamps,
  sequence, order/route/fill identifiers and supplied values.
- Related timeline: the indexed source events that were recorded, with measured
  intervals between them.
- Raw fields: the selected envelope, including original source bytes.

Nanoseconds and sequence numbers retain 64-bit precision. Decimal values stay
strings in detail tables. The receive-minus-occurrence interval is labelled
collection time; an interval needs two valid timestamps in order.

Changing selection invalidates older asynchronous responses. Selecting an
instrument filters the view; the run stream and its sequence checks stay whole.
The Web table and native event model have bounded visible
history; the Rust index provides older linked records on demand.

## Verification

`services/gateway/src/journal.rs` tests linkage, scope isolation, missing records
and bounded history. The Web event-detail tests cover Decimal and nanosecond
precision. Native QtTest covers exact presentation, context changes and a
25,000-event input with a 20,000-row retained view. `pipeline_replay.py` exercises
the compiled Rust services and NATS locally, including gateway restart and
HTTP detail-query timing. Runtime candidates ship the compiled services.
