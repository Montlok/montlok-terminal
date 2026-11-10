# Upstream and modification notice

This directory is a modified fork of [FreqUI](https://github.com/freqtrade/frequi),
retrieved at commit `337b8980612410462ef31c0715ab02b2a1edf9c8` on 2026-08-13.
FreqUI is licensed under GPL-3.0; the unmodified license text is retained in
`LICENSE`.

The fork replaces the original Freqtrade shell with a Nautilus Trader operator
cockpit, adds a fail-closed Operator API adapter, and integrates TradingView
Lightweight Charts 5.2.0. Lightweight Charts is licensed under Apache-2.0 and
requires product-creator attribution; the rendered chart keeps its built-in
attribution logo enabled.

Hummingbot Dashboard informed the instance/strategy/reconciliation workflow,
while Prometheus and Grafana remain the telemetry and alerting layer. Their
source is not copied into this directory.
