# Weilan / Nautilus trading engine

This directory is the trading-engine source vendored into
[`Montlok/montlok-terminal`](https://github.com/Montlok/montlok-terminal).
It is a dedicated subtree so it does not join the repository-root Cargo or
CMake workspace (`services/gateway`, `services/projector`, `adapters/nautilus`).

## Source

- Primary tree: [`KeikaJames/nautilus-weilan`](https://github.com/KeikaJames/nautilus-weilan)
  `main` at `012925cee0bfc16e3f036a10bcc57d714ca5e288` (labeled v1.231.0,
  2026-09-12 19:52:11 +0800).
- Additional engine-only files from `origin/KK/montlok-passkeys`
  (`e55d61ccc62481b58e2d3f52c9ee83bbf8dc009b`) that were not on `main`:
  OKX adapter, common/live/risk/system/trading, `model_runtime`, and the
  materialized `research/crypto_alpha` and `research/stock_alpha` trees.
  On `main` those research paths were symlinks into an out-of-repo
  `model_training/` directory.
- The operator UI (`operator_terminal/`) was **not** copied. The production
  operator terminal in this repository is `apps/web`, which is already ahead
  of the weilan tree as of 2026-09-09.
- `operator_dashboard/` is included here as an engine-adjacent research UI
  that has no counterpart under `apps/`.

Upstream NautilusTrader history and trademarks remain those of
[Nautech Systems](https://nautilustrader.io).

## License

Files in this directory keep their original license: **LGPL-3.0-or-later**
(see `LICENSE`, crate-level `LICENSE` symlinks, and upstream notices).
They are **not** relicensed as AGPL.

The rest of `montlok-terminal` (gateway, projector, web/native clients,
contracts) remains **AGPL-3.0-or-later** as declared by the repository-root
`LICENSE`.

## Layout and build

- Rust workspace: `Cargo.toml`, `Cargo.lock`, `crates/`, `rust-toolchain.toml`
  (channel 1.97.1, local to this directory).
- Python package: `nautilus_trader/`, `python/`, `pyproject.toml`, `uv.lock`,
  `build.py`.
- Weilan-unique crates include `crates/model_runtime` and `crates/provenance`.
- Tests, examples, research, scripts, schema, and docs stay beside the
  engine so it can be built and run from this subtree.

Weilan GitHub Actions and composite actions live under `engine/.github/`
and are **not** wired into the repository-root `.github/workflows/`, to
avoid colliding with Montlok Terminal CI.

Build and develop from this directory, for example:

```bash
cd engine
# Rust toolchain is read from ./rust-toolchain.toml
cargo metadata --format-version 1 --no-deps >/dev/null
# Full engine builds need the Rust and Python toolchain described upstream.
```

See the upstream `README.md`, `Makefile`, and `pyproject.toml` in this
directory for the complete build.
