# Security Policy

Report security issues privately to the repository owner, with affected
versions, reproduction steps, impact and any suggested mitigation.

OKX API secrets stay on the server. Clients keep device refresh tokens and
Ed25519 private keys in the operating-system credential store. Write operations
use an expiring `prepare -> execute -> receipt` flow with an idempotent
operation identifier. NATS, the projector and the Nautilus control sockets
belong on private networks.
