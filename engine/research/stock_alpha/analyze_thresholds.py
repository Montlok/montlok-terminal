#!/usr/bin/env python3
"""Re-evaluate saved model predictions across fixed entry thresholds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from backtest_portfolio import ASSETS
from backtest_portfolio import parse_weights
from backtest_portfolio import portfolio_performance
from backtest_portfolio import prediction_to_position


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--weights", required=True)
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--cost-bps", type=float, default=12.0)
    args = parser.parse_args()

    rows = pd.read_csv(args.predictions, parse_dates=["timestamp"])
    weights = parse_weights(args.weights)
    returns = (
        rows.drop_duplicates(["timestamp", "asset"])
        .pivot(index="timestamp", columns="asset", values="trade_return")
        .loc[:, ASSETS]
    )
    result: dict[str, dict[str, dict[str, float | int]]] = {}
    for model in sorted(rows["model"].unique()):
        prediction = (
            rows.loc[rows["model"] == model]
            .pivot(index="timestamp", columns="asset", values="prediction")
            .loc[:, ASSETS]
        )
        result[model] = {}
        for threshold_bps in (0.0, 25.0, 50.0, 100.0, 200.0):
            position = prediction_to_position(
                prediction,
                threshold_bps / 10_000.0,
                0.0,
                args.rebalance_days,
            )
            _, metrics = portfolio_performance(
                position,
                returns.loc[prediction.index],
                weights,
                args.cost_bps / 10_000.0,
            )
            result[model][f"{threshold_bps:g}_bps"] = metrics
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
