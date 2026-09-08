# Contributing

## Repository boundaries

- Product code and maintained architecture/API documentation belong here.
- Local run reports, deployment snapshots, research notes, and operator
  handoffs do not belong in this repository.
- Do not commit credentials, exchange keys, cookies, private certificates,
  runtime databases, raw production logs, or local environment files.
- Tokyo production hosts receive signed release artifacts and configuration,
  never tests, notebooks, source worktrees, or development scripts.

## Compatibility

Contract changes start in `packages/contracts`. Run the schema compatibility,
Rust, Web, and native tests before marking a capability complete. Additive
fields are preferred. Removing or retyping a public field requires an explicit
version transition.

## Commits

Use focused conventional commits. Keep migration, generated code, product
changes, and production release changes in separate commits so each layer can
be audited independently.
