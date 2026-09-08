# Sidecar deployment

The gateway, projector, NATS, and per-run adapters are independent services.
None of their systemd units requires or restarts a Nautilus strategy service.

Build on CI or a build host. Install only the release binaries, signed manifest,
static assets, and reviewed configuration on Tokyo. Source trees, test suites,
notebooks, package managers, and capacity scripts stay on build/research hosts.

The runtime user requires read access to the existing durable run log and
control socket. Its writable state belongs under `/var/lib/montlok-terminal`.
Set `MONTLOK_EVENT_LOG`, `MONTLOK_ACCOUNT_ID`, `MONTLOK_GROUP_ID`,
`MONTLOK_RUN_ID`, `MONTLOK_ADAPTER_DB`, and `MONTLOK_NATS_URL` for each adapter.
Keep each adapter database paired with the exact source log it has consumed.

Set `MONTLOK_GATEWAY_DB`, `MONTLOK_NATS_URL`, `MONTLOK_CONTROL_SOCKET`,
`MONTLOK_PUBLIC_ORIGIN`, and `MONTLOK_BFF_URL` for the gateway. The optional
native compatibility link uses one private 32-byte-or-longer key file via
`MONTLOK_GATEWAY_SECRET_FILE`; configure that same path as
`--gateway-secret-file` in the legacy BFF. Web sessions use the existing Passkey
authentication. Native devices use the v2 authorization endpoints and system
credential storage.

The Nginx excerpt assumes the existing HTTP-level `connection_upgrade` map.
Private APIs, authentication, streams, operations, and receipts use `no-store`.
Only fingerprinted static assets and versioned third-party WASM/JS bundles
receive immutable caching. The WASM table component requires
`script-src 'self' 'wasm-unsafe-eval'` and `worker-src 'self' blob:` in the
existing content security policy.

Validate configuration and compare snapshots against the current live BFF
before making v2 the main entry. Switching or rolling back a terminal version
changes only these sidecars and static assets.
