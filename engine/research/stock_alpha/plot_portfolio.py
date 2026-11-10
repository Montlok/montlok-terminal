#!/usr/bin/env python3
"""Plot stock portfolio model equity and drawdown."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib


matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    frame = pd.read_csv(args.equity, parse_dates=["timestamp"])
    columns = ["double_ensemble", "lightgbm", "gru", "trend", "buy_hold"]
    labels = {
        "double_ensemble": "DoubleEnsemble",
        "lightgbm": "LightGBM",
        "gru": "GRU",
        "trend": "20d/100d trend",
        "buy_hold": "Current-weight buy & hold",
    }
    colors = {
        "double_ensemble": "#007AFF",
        "lightgbm": "#FF9500",
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
        equity = frame[column]
        drawdown = equity / equity.cummax() - 1.0
        equity_axis.plot(frame["timestamp"], equity, label=labels[column], color=colors[column])
        drawdown_axis.plot(frame["timestamp"], drawdown, color=colors[column])
    equity_axis.set_yscale("log")
    equity_axis.set_title("NVDA / SK Hynix / MU walk-forward portfolio (12 bps per side)")
    equity_axis.set_ylabel("Equity, log scale")
    equity_axis.grid(alpha=0.2)
    equity_axis.legend(ncol=3, frameon=False)
    drawdown_axis.set_ylabel("Drawdown")
    drawdown_axis.set_xlabel("UTC")
    drawdown_axis.grid(alpha=0.2)
    figure.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(args.output, dpi=160)


if __name__ == "__main__":
    main()
