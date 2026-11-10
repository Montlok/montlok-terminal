# OKX tokenized-stock alpha research

This research module evaluates numerical stock models for the liquid listed underlyings held in
the connected OKX account. It does not use account credentials, submit orders, or modify the live
portfolio.

Current scope:

- `XNVDA` → NVIDIA (`NVDA`)
- `XMU` → Micron Technology (`MU`)
- `XSKHY` → SK Hynix (`000660.KS`), converted from KRW to USD with `KRW=X`

SpaceX is excluded. Model inputs use underlying adjusted daily prices because the corresponding
OKX spot tokens were listed only in July 2026. Execution costs use the observed OKX Lv1 stock-token
spot fee plus slippage.

Models:

- LightGBM
- Qlib-compatible DoubleEnsemble sample reweighting and feature selection
- GRU numerical sequence model

Strategies are long/cash per asset with no leverage. All reported model results are stitched
walk-forward out-of-sample results.
