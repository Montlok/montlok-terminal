# Contributing

## Repository contents

This repository holds product code and the architecture and API documentation
maintained with it. Run reports, deployment snapshots, research notes and
operator handoffs belong to the operations records.

Credentials, exchange keys, cookies, private certificates, runtime databases,
raw production logs and local environment files stay in local or secret
storage. Tokyo production hosts receive signed release artifacts and reviewed
configuration.

## Compatibility

Contract changes start in `packages/contracts`. Run the schema compatibility,
Rust, Web and native tests before marking a capability complete. Prefer
additive fields; removing or retyping a public field needs an explicit version
transition.

## Commits

Use focused conventional commits. Keep migration, generated code, product
changes and production release changes in separate commits so each layer can be
audited on its own.
