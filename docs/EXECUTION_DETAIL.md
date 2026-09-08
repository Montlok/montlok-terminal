# Execution detail

Selecting an event is an observation, not a trading operation. Web and native
clients use `GET /api/v2/events/{event_id}/related?limit=500` to investigate it.
The endpoint requires the existing browser session or signed device request.

## Indexed history

The Rust gateway writes link keys in the same SQLite transaction as its replay
journal. Each link is scoped to the account and run, and the query also checks
the strategy group. The traversal visits each link key once; duplicate paths
and cyclic relationships do not repeatedly expand the graph. Order, route,
venue-order, trade and position identities connect records whose correlation
IDs are absent or differ. Inference, signal, target decision, risk decision,
operation and explicit causation identities use the same traversal.

File-backed detail queries use independent read-only WAL connections on the
blocking pool. They do not acquire the event-ingestion writer mutex. Each
request observes one consistent read transaction. The reader never changes a
strategy or accesses an exchange credential.

Responses contain Base64-encoded Protobuf envelopes, an explicit encoding
identifier, and `truncated`. The default limit is 500, with an upper bound of
2,048. A limited response always includes the selected event. Returned events
are ordered by occurrence time, stream and sequence; sequence numbers from
different streams are not interpreted as one global counter.

## Client presentation

- Summary: identity, source, account/group/run, event and receive timestamps,
  sequence, order/route/fill identifiers and supplied values.
- Related timeline: indexed source events and measured intervals. Missing
  stages remain absent; the client does not manufacture a full lifecycle.
- Raw fields: the selected envelope, including original source bytes.

Nanoseconds and sequence numbers retain 64-bit precision. Decimal values stay
strings in detail tables. A display interval is not a trading-latency promise:
the receive-minus-occurrence interval is labelled collection time. A negative
or missing timestamp cannot produce a valid interval.

Changing selection invalidates older asynchronous responses. Selecting an
instrument does not narrow the run stream and therefore cannot create false
sequence gaps. The Web table and native event model have bounded visible
history; the Rust index provides older linked records on demand.

## Verification

`services/gateway/src/journal.rs` tests linkage, scope isolation, missing records
and bounded history. The Web event-detail tests cover Decimal and nanosecond
precision. Native QtTest covers exact presentation, context changes and a
25,000-event input with a 20,000-row retained view. `pipeline_replay.py` exercises
the compiled Rust services and NATS locally, including gateway restart and
HTTP detail-query timing. Test scripts are excluded from runtime candidates.
