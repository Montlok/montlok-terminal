# Montlok Terminal

Montlok Terminal is an open-source, institutional-style terminal for operating,
investigating, and publishing systematic trading workloads. It provides two
clients over one typed data and control plane. The v2 migration is in progress:

- `apps/web`: the production React terminal, migrated with its existing Git
  history and complete operator capabilities.
- `apps/native`: a C++20/Qt 6 desktop terminal for macOS, Windows, and Linux.
- `services/gateway`: the Rust HTTPS/WebSocket edge for snapshots, streams,
  queries, device authorization, and operation receipts.
- `services/projector`: the Rust Arrow/DataFusion event projector and history
  query service.
- `adapters/nautilus`: a non-blocking adapter that exports the persisted
  Nautilus event log without entering the trading hot path.
- `packages/contracts`: Protobuf, OpenAPI, Arrow, and JSON Schema contracts.

The live trading process remains independent of every UI and data-plane
component. Restarting a terminal client, the gateway, NATS, or the projector
must not stop or alter a strategy run.

## Workspaces

The Web and native clients expose the same six logical workspaces:

1. Live Operations
2. Execution Investigation
3. Research and Release
4. Portfolio and Risk
5. Market and Data
6. Operations and Security

`capabilities.yaml` is the machine-readable parity contract. A business
capability is complete only when both clients expose the same fields, actions,
states, and receipts. Workspace and capability declarations describe the target
contract; they are not completion flags. Existing Web business forms remain
available while specialized native panels are implemented.

The Web shell uses one dockable tab layer and a keyboard-searchable function
catalog. Both clients can inspect a selected event through the Rust indexed
order/route/fill history service. See [Execution detail](docs/EXECUTION_DETAIL.md)
for scope, precision, recovery and bounded-query semantics.

## Development

Prerequisites are Node.js 22+, Rust 1.94, CMake 3.28+, Qt 6.8, Protobuf, and
Ninja. Third-party source dependencies are fetched directly from their
upstream releases at pinned versions; this project does not require GitHub
forks of those repositories.

```bash
npm ci
npm --prefix apps/web test
cargo test --workspace
cmake --preset macos-debug
cmake --build --preset macos-debug
```

See [Architecture](docs/ARCHITECTURE.md), [protocol contracts](packages/contracts),
and [contributing](CONTRIBUTING.md) for the repository rules.

## License

Montlok Terminal is licensed under the GNU Affero General Public License,
version 3 or later. Third-party attributions and exceptions are recorded in
`THIRD_PARTY_NOTICES.md`.
