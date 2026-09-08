# Security Policy

Please report security issues privately to the repository owner rather than
opening a public issue. Include affected versions, reproduction steps, impact,
and any suggested mitigation.

Montlok clients never receive OKX API secrets. Device refresh tokens and
Ed25519 private keys are stored in the operating-system credential store.
Write operations use an expiring `prepare -> execute -> receipt` flow and an
idempotent operation identifier. NATS, the projector, and Nautilus control
sockets must not be exposed to the public Internet.
