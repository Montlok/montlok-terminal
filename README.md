# Montlok Terminal

Montlok Terminal operates, investigates and publishes systematic trading
workloads. A Web client and a native desktop client share one typed data and
control plane; the trading engine runs in its own processes, and the v2 data
plane runs alongside the Web terminal during the migration.

| Path | Contents |
|---|---|
| `apps/web` | React terminal, its aiohttp BFF (`server/`) and the strategy-group supervisor |
| `apps/web/engine` | Nautilus strategy workers (sandbox, model, live HFT, live model and Alpha groups) and local model inference on montlok or PyTorch |
| `apps/native` | C++20/Qt 6 desktop terminal for macOS, Windows and Linux |
| `services/gateway` | Rust HTTPS/WebSocket edge: snapshots, streams, queries, device authorization, operation receipts |
| `services/projector` | Rust Arrow/DataFusion event projector and history queries |
| `adapters/nautilus` | Publishes the persisted Nautilus event log to NATS |
| `packages/contracts` | Protobuf, OpenAPI, Arrow and JSON Schema contracts |

Strategy runs continue while a terminal client, the gateway, NATS or the
projector restarts: Nautilus persists each event first, and the adapter reads
that log asynchronously.

## Workspaces

The Web and native clients expose the same six workspaces:

1. Live Operations
2. Execution Investigation
3. Research and Release
4. Portfolio and Risk
5. Market and Data
6. Operations and Security

`capabilities.yaml` lists every business capability as the target contract. A
capability is complete when both clients expose the same fields, actions,
states and receipts; until then the existing Web forms serve it.

The Web shell has dockable tabs and a searchable function catalog. Both clients
trace a selected event through the Rust order/route/fill index
([Execution detail](docs/EXECUTION_DETAIL.md)).

## Development

Prerequisites: Node.js 22+, Rust 1.94, CMake 3.28+, Qt 6.8, Protobuf and
Ninja. Third-party sources come from their upstream releases at pinned
versions.

```bash
npm ci
npm --prefix apps/web test
cargo test --workspace
cmake --preset macos-debug && cmake --build --preset macos-debug
(cd apps/web/server && python -m pytest -q tests)
(cd apps/web/engine && PYTHONPATH=.:../server python -m unittest test_alpha_execution test_live_alpha_worker \
    test_live_controls test_live_hft_worker test_live_model_worker)
```

## Documentation

| Topic | Document |
|---|---|
| Architecture and the trading boundary | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Event detail queries | [docs/EXECUTION_DETAIL.md](docs/EXECUTION_DETAIL.md) |
| Run observations API | [docs/RUN_OBSERVATIONS.md](docs/RUN_OBSERVATIONS.md) |
| Sidecar deployment | [deploy/README.md](deploy/README.md) |
| Web client | [apps/web/README.md](apps/web/README.md) |
| Strategy-group supervisor | [apps/web/engine/GROUP_RUNTIME.md](apps/web/engine/GROUP_RUNTIME.md) |
| Model inference | [apps/web/engine/MODEL_RUNTIME.md](apps/web/engine/MODEL_RUNTIME.md) |
| Model releases | [apps/web/server/MODEL_RELEASES.md](apps/web/server/MODEL_RELEASES.md) |
| Repository rules | [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md) |

## License

Montlok Terminal is licensed under the GNU Affero General Public License,
version 3 or later. Third-party attributions and exceptions are recorded in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
