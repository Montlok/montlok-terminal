#!/usr/bin/env python3
"""Plot stitched out-of-sample equity and drawdown from the BTC-USDT baselines."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib


matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-equity", type=Path, required=True)
    parser.add_argument("--lightgbm-equity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    models = pd.read_csv(args.model_equity, parse_dates=["timestamp"])
    lightgbm = pd.read_csv(args.lightgbm_equity, parse_dates=["timestamp"])[
        ["timestamp", "lightgbm"]
    ]
    equity = models.merge(lightgbm, on="timestamp", how="inner")
    columns = ["lightgbm", "hist_gbdt", "gru", "trend", "buy_hold"]
    labels = {
        "lightgbm": "LightGBM",
        "hist_gbdt": "HistGBDT",
        "gru": "GRU",
        "trend": "MA trend",
        "buy_hold": "Buy & hold",
    }
    colors = {
        "lightgbm": "#007AFF",
        "hist_gbdt": "#FF9500",
        "gru": "#AF52DE",
        "trend": "#34C759",
        "buy_hold": "#8E8E93",
    }

    figure, (equity_axis, drawdown_axis) = plt.subplots(
        2,
        1,
        figsize=(13, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 1]},
    )
    for column in columns:
        series = equity[column]
        drawdown = series / series.cummax() - 1.0
        equity_axis.plot(equity["timestamp"], series, label=labels[column], color=colors[column])
        drawdown_axis.plot(equity["timestamp"], drawdown, color=colors[column])

    equity_axis.set_title("BTC-USDT 1H walk-forward equity (12 bps per position side)")
    equity_axis.set_ylabel("Equity")
    equity_axis.grid(alpha=0.2)
    equity_axis.legend(ncol=5, frameon=False)
    drawdown_axis.set_ylabel("Drawdown")
    drawdown_axis.set_xlabel("UTC")
    drawdown_axis.grid(alpha=0.2)
    figure.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(args.output, dpi=160)


if __name__ == "__main__":
    main()
