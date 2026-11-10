#!/usr/bin/env python3
"""Measure observed OKX token drawdowns and a fixed-current-quantity basket."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ASSETS = ("XNVDA", "XSKHY", "XMU")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path("outputs/token-drawdown.json"))
    parser.add_argument("--quantities", required=True)
    return parser.parse_args()


def parse_quantities(value: str) -> pd.Series:
    quantities: dict[str, float] = {}
    for item in value.split(","):
        asset, raw_quantity = item.split("=", 1)
        quantities[asset.strip()] = float(raw_quantity)
    if set(quantities) != set(ASSETS):
        raise ValueError(f"quantities must contain exactly {ASSETS}")
    result = pd.Series(quantities, dtype=float)
    if (result < 0).any() or result.sum() <= 0:
        raise ValueError("quantities must be non-negative with a positive sum")
    return result


def drawdown_report(equity: pd.Series) -> dict[str, object]:
    equity = equity.dropna()
    running_peak = equity.cummax()
    drawdown = equity / running_peak - 1.0
    trough = drawdown.idxmin()
    peak = equity.loc[:trough].idxmax()
    recovery_candidates = equity.loc[trough:]
    recovery_candidates = recovery_candidates[recovery_candidates >= equity.loc[peak]]
    recovery = recovery_candidates.index[0] if len(recovery_candidates) else None
    return {
        "total_return": float(equity.iloc[-1] / equity.iloc[0] - 1.0),
        "maximum_drawdown": float(drawdown.loc[trough]),
        "peak_at": peak.isoformat(),
        "trough_at": trough.isoformat(),
        "recovered_at": recovery.isoformat() if recovery is not None else None,
        "current_drawdown": float(drawdown.iloc[-1]),
    }


def main() -> None:
    args = parse_args()
    quantities = parse_quantities(args.quantities)
    prices = {}
    for asset in ASSETS:
        path = args.data_dir / f"okx-{asset}-USDT-1H.csv"
        frame = pd.read_csv(path, parse_dates=["timestamp"]).set_index("timestamp").sort_index()
        prices[asset] = frame["close"].astype(float)
    price_frame = pd.DataFrame(prices).dropna()
    reports = {
        asset: drawdown_report(price_frame[asset] / price_frame[asset].iloc[0])
        for asset in price_frame
    }
    basket_value = (price_frame * quantities).sum(axis=1)
    reports["current_quantity_basket"] = drawdown_report(basket_value / basket_value.iloc[0])
    result = {
        "start": price_frame.index[0].isoformat(),
        "end": price_frame.index[-1].isoformat(),
        "hours": len(price_frame),
        "reports": reports,
        "interpretation": (
            "The basket applies today's quantities to the full observed history. It is not the "
            "account's realized PnL unless the same quantities were held from the first timestamp."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
