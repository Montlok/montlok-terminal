# BTC-USDT crypto alpha baseline

This directory contains a small, reproducible research baseline for adapting the numerical
models reviewed in QuantML/Qlib to OKX spot data. It is intentionally separate from the live
execution path: the experiment produces out-of-sample predictions and performance evidence, not
orders.

## Models and strategies

- `hist_gbdt`: histogram gradient boosting on lagged and rolling numerical features. This is the
  first tree-model baseline before introducing LightGBM.
- `lightgbm`: the corresponding LightGBM baseline with chronological early stopping.
- `gru`: a compact PyTorch GRU using rolling feature sequences.
- `trend`: deterministic 24-hour/168-hour moving-average baseline.
- `buy_hold`: fully invested BTC benchmark.

All active strategies are long/flat with no leverage. Features are known at the close of bar `t`;
the simulated position begins at the open of `t+1`. Trading costs are charged on every position
change. The default six-hour rebalance interval matches the six-hour forecast horizon.

## Run

```bash
cd research/crypto_alpha

python download_okx_candles.py \
  --inst-id BTC-USDT \
  --bar 1H \
  --start 2024-01-01 \
  --output data/BTC-USDT-1H.csv

python backtest_btc_usdt.py \
  --data data/BTC-USDT-1H.csv \
  --models hist_gbdt,gru \
  --output-dir outputs/btc-usdt-1h
```

The backtest writes `report.json`, `predictions.csv`, and `equity.csv`. Metrics cover only the
stitched out-of-sample walk-forward windows.

## Research boundary

This is a first-pass model comparison, not an alpha promotion decision. A candidate must later be
re-run with exact account fees, broader market-state features, multiple horizons, parameter
stability analysis, and NautilusTrader's market/execution simulation before it can enter a demo
strategy.
