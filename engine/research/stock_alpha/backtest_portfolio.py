#!/usr/bin/env python3
"""Walk-forward stock-model comparison for the non-SpaceX OKX holdings."""

from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from double_ensemble import DoubleEnsembleRegressor


TRADING_DAYS_PER_YEAR = 252
ASSETS = ("XNVDA", "XSKHY", "XMU")


@dataclass(frozen=True)
class Fold:
    number: int
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/current-portfolio"))
    parser.add_argument("--models", default="lightgbm,double_ensemble,gru")
    parser.add_argument("--weights", required=True)
    parser.add_argument("--horizon-days", type=int, default=5)
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--train-days", type=int, default=756)
    parser.add_argument("--validation-days", type=int, default=126)
    parser.add_argument("--test-days", type=int, default=252)
    parser.add_argument("--entry-threshold-bps", type=float, default=50.0)
    parser.add_argument("--exit-threshold-bps", type=float, default=0.0)
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--sequence-days", type=int, default=60)
    parser.add_argument("--gru-hidden-size", type=int, default=32)
    parser.add_argument("--gru-epochs", type=int, default=12)
    parser.add_argument("--seed", type=int, default=11)
    return parser.parse_args()


def parse_weights(value: str) -> pd.Series:
    weights: dict[str, float] = {}
    for item in value.split(","):
        asset, raw_weight = item.split("=", 1)
        weights[asset.strip()] = float(raw_weight)
    if set(weights) != set(ASSETS):
        raise ValueError(f"weights must contain exactly {ASSETS}")
    result = pd.Series(weights, dtype=float)
    if (result < 0).any() or result.sum() <= 0:
        raise ValueError("weights must be non-negative with a positive sum")
    return result / result.sum()


def engineer_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    frame = frame.copy().sort_values("timestamp").set_index("timestamp")
    close = frame["close"].astype(float)
    log_return = np.log(close).diff()
    frame["range"] = (frame["high"] - frame["low"]) / close
    frame["body"] = (frame["close"] - frame["open"]) / frame["open"]
    frame["upper_wick"] = (frame["high"] - frame[["open", "close"]].max(axis=1)) / close
    frame["lower_wick"] = (frame[["open", "close"]].min(axis=1) - frame["low"]) / close
    columns = ["range", "body", "upper_wick", "lower_wick"]
    for lag in (1, 2, 3, 5, 10, 20, 40, 60, 120):
        name = f"return_lag_{lag}"
        frame[name] = log_return.shift(lag - 1)
        columns.append(name)
    for window in (5, 10, 20, 40, 60, 120):
        calculations = {
            "return_mean": log_return.rolling(window).mean(),
            "return_vol": log_return.rolling(window).std(),
            "momentum": close.pct_change(window),
            "close_to_mean": close / close.rolling(window).mean() - 1.0,
            "range_mean": frame["range"].rolling(window).mean(),
        }
        for prefix, values in calculations.items():
            name = f"{prefix}_{window}"
            frame[name] = values
            columns.append(name)
    log_volume = np.log1p(frame["volume"].astype(float))
    for window in (20, 60, 120):
        rolling = log_volume.rolling(window)
        name = f"volume_z_{window}"
        frame[name] = (log_volume - rolling.mean()) / rolling.std()
        columns.append(name)
    frame["trend_position"] = (close.rolling(20).mean() > close.rolling(100).mean()).astype(float)
    frame = frame.dropna(subset=columns)
    return frame, columns


def load_panel(data_dir: Path, horizon_days: int) -> tuple[dict[str, pd.DataFrame], list[str], pd.DatetimeIndex]:
    panels: dict[str, pd.DataFrame] = {}
    feature_columns: list[str] | None = None
    for asset in ASSETS:
        raw = pd.read_csv(data_dir / f"{asset}.csv", parse_dates=["timestamp"])
        frame, columns = engineer_features(raw)
        if feature_columns is None:
            feature_columns = columns
        elif columns != feature_columns:
            raise ValueError("feature schema differs by asset")
        panels[asset] = frame
    common_dates = pd.DatetimeIndex(sorted(set.intersection(*(set(frame.index) for frame in panels.values()))))
    if len(common_dates) < 1_500:
        raise ValueError(f"insufficient common stock dates: {len(common_dates)}")
    for asset, frame in list(panels.items()):
        aligned = frame.loc[common_dates].copy()
        aligned["target"] = aligned["close"].shift(-horizon_days) / aligned["close"] - 1.0
        aligned["trade_return"] = aligned["open"].shift(-2) / aligned["open"].shift(-1) - 1.0
        panels[asset] = aligned
    usable_dates = common_dates[:-max(horizon_days, 2)]
    panels = {asset: frame.loc[usable_dates] for asset, frame in panels.items()}
    return panels, feature_columns or [], usable_dates


def make_tabular(
    panels: dict[str, pd.DataFrame],
    feature_columns: list[str],
    dates: pd.DatetimeIndex,
) -> tuple[pd.DataFrame, pd.Series, pd.MultiIndex]:
    rows: list[pd.DataFrame] = []
    for asset in ASSETS:
        item = panels[asset].loc[dates, feature_columns].copy()
        for identity in ASSETS:
            item[f"asset_{identity}"] = float(asset == identity)
        item["target"] = panels[asset].loc[dates, "target"]
        item["asset"] = asset
        item["timestamp"] = dates
        rows.append(item.reset_index(drop=True))
    long = pd.concat(rows, ignore_index=True).set_index(["timestamp", "asset"]).sort_index()
    target = long.pop("target")
    return long, target, long.index


def make_folds(
    dates: pd.DatetimeIndex,
    train_days: int,
    validation_days: int,
    test_days: int,
) -> list[tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex, Fold]]:
    if validation_days >= train_days:
        raise ValueError("validation window must be shorter than training window")
    folds = []
    test_start = train_days
    number = 1
    while test_start < len(dates):
        train_start = max(0, test_start - train_days)
        validation_start = test_start - validation_days
        test_end = min(len(dates), test_start + test_days)
        if test_end - test_start < 40:
            break
        fit_dates = dates[train_start:validation_start]
        validation_dates = dates[validation_start:test_start]
        test_dates = dates[test_start:test_end]
        fold = Fold(
            number=number,
            train_start=fit_dates[0].isoformat(),
            train_end=fit_dates[-1].isoformat(),
            validation_start=validation_dates[0].isoformat(),
            validation_end=validation_dates[-1].isoformat(),
            test_start=test_dates[0].isoformat(),
            test_end=test_dates[-1].isoformat(),
        )
        folds.append((fit_dates, validation_dates, test_dates, fold))
        test_start = test_end
        number += 1
    if not folds:
        raise ValueError("not enough history for a walk-forward fold")
    return folds


def rows_for_dates(index: pd.MultiIndex, dates: pd.DatetimeIndex) -> np.ndarray:
    return np.flatnonzero(index.get_level_values("timestamp").isin(dates))


def fit_lightgbm(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    x_valid: pd.DataFrame,
    y_valid: pd.Series,
    x_test: pd.DataFrame,
    seed: int,
) -> np.ndarray:
    import lightgbm as lgb

    train_set = lgb.Dataset(x_train, label=y_train)
    valid_set = lgb.Dataset(x_valid, label=y_valid, reference=train_set)
    model = lgb.train(
        {
            "objective": "mse",
            "learning_rate": 0.03,
            "num_leaves": 31,
            "min_data_in_leaf": 60,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l1": 0.1,
            "lambda_l2": 1.0,
            "verbosity": -1,
            "seed": seed,
            "num_threads": 4,
        },
        train_set,
        num_boost_round=600,
        valid_sets=[valid_set],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)],
    )
    return model.predict(x_test)


def fit_gru(
    panels: dict[str, pd.DataFrame],
    feature_columns: list[str],
    all_dates: pd.DatetimeIndex,
    fit_dates: pd.DatetimeIndex,
    validation_dates: pd.DatetimeIndex,
    test_dates: pd.DatetimeIndex,
    sequence_days: int,
    hidden_size: int,
    epochs: int,
    seed: int,
) -> pd.DataFrame:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader
    from torch.utils.data import TensorDataset

    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True)
    torch.set_num_threads(max(1, min(4, torch.get_num_threads())))
    date_lookup = {date: index for index, date in enumerate(all_dates)}
    scaler_values = np.concatenate(
        [panels[asset].loc[fit_dates, feature_columns].to_numpy() for asset in ASSETS],
    )
    scaler = StandardScaler().fit(scaler_values)
    scaled = {
        asset: scaler.transform(
            panels[asset].loc[all_dates, feature_columns].to_numpy(),
        ).astype(np.float32)
        for asset in ASSETS
    }
    identity = np.eye(len(ASSETS), dtype=np.float32)

    def samples(sample_dates: pd.DatetimeIndex) -> tuple[np.ndarray, np.ndarray, list[tuple[pd.Timestamp, str]]]:
        x_values: list[np.ndarray] = []
        y_values: list[float] = []
        keys: list[tuple[pd.Timestamp, str]] = []
        for asset_number, asset in enumerate(ASSETS):
            for date in sample_dates:
                date_index = date_lookup[date]
                if date_index < sequence_days - 1:
                    continue
                sequence = scaled[asset][date_index - sequence_days + 1 : date_index + 1]
                ids = np.repeat(identity[asset_number][None, :], sequence_days, axis=0)
                x_values.append(np.concatenate([sequence, ids], axis=1))
                y_values.append(float(panels[asset].loc[date, "target"]) * 100.0)
                keys.append((date, asset))
        return np.stack(x_values), np.asarray(y_values, dtype=np.float32), keys

    train_x, train_y, _ = samples(fit_dates)
    valid_x, valid_y, _ = samples(validation_dates)
    test_x, _, test_keys = samples(test_dates)
    if len(train_x) < 500 or len(test_keys) != len(test_dates) * len(ASSETS):
        raise ValueError("insufficient GRU sequences")

    class GRURegressor(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.gru = nn.GRU(train_x.shape[2], hidden_size, batch_first=True)
            self.output = nn.Linear(hidden_size, 1)

        def forward(self, values: torch.Tensor) -> torch.Tensor:
            encoded, _ = self.gru(values)
            return self.output(encoded[:, -1]).squeeze(-1)

    model = GRURegressor()
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.HuberLoss(delta=1.0)
    generator = torch.Generator().manual_seed(seed)
    loader = DataLoader(
        TensorDataset(torch.from_numpy(train_x), torch.from_numpy(train_y)),
        batch_size=128,
        shuffle=True,
        generator=generator,
    )
    valid_tensor = torch.from_numpy(valid_x)
    valid_target = torch.from_numpy(valid_y)
    best_loss = math.inf
    best_state = None
    stale = 0
    for _ in range(epochs):
        model.train()
        for batch_x, batch_y in loader:
            optimizer.zero_grad(set_to_none=True)
            loss = loss_fn(model(batch_x), batch_y)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
        model.eval()
        with torch.no_grad():
            validation_loss = float(loss_fn(model(valid_tensor), valid_target))
        if validation_loss < best_loss - 1e-6:
            best_loss = validation_loss
            best_state = {key: value.detach().clone() for key, value in model.state_dict().items()}
            stale = 0
        else:
            stale += 1
            if stale >= 3:
                break
    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        prediction = model(torch.from_numpy(test_x)).numpy() / 100.0
    output = pd.DataFrame(test_keys, columns=["timestamp", "asset"])
    output["prediction"] = prediction
    return output.pivot(index="timestamp", columns="asset", values="prediction").loc[test_dates, ASSETS]


def prediction_to_position(
    prediction: pd.DataFrame,
    entry_threshold: float,
    exit_threshold: float,
    rebalance_days: int,
) -> pd.DataFrame:
    position = pd.DataFrame(0.0, index=prediction.index, columns=prediction.columns)
    held = pd.Series(0.0, index=prediction.columns)
    for date_number, date in enumerate(prediction.index):
        if date_number % rebalance_days == 0:
            held.loc[prediction.loc[date] > entry_threshold] = 1.0
            held.loc[prediction.loc[date] < exit_threshold] = 0.0
        position.loc[date] = held
    return position


def portfolio_performance(
    position: pd.DataFrame,
    returns: pd.DataFrame,
    weights: pd.Series,
    cost_per_side: float,
) -> tuple[pd.Series, dict[str, float | int]]:
    position = position.loc[returns.index, weights.index].clip(0.0, 1.0)
    turnover = position.diff().abs()
    turnover.iloc[0] = position.iloc[0].abs()
    gross_return = (position * returns * weights).sum(axis=1)
    cost = (turnover * weights).sum(axis=1) * cost_per_side
    net_return = gross_return - cost
    equity = (1.0 + net_return).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    years = len(equity) / TRADING_DAYS_PER_YEAR
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0) if years > 0 and equity.iloc[-1] > 0 else -1.0
    standard_deviation = net_return.std(ddof=1)
    sharpe = float(net_return.mean() / standard_deviation * math.sqrt(TRADING_DAYS_PER_YEAR)) if standard_deviation > 0 else 0.0
    max_drawdown = float(drawdown.min())
    return equity, {
        "total_return": total_return,
        "cagr": cagr,
        "annualized_volatility": float(standard_deviation * math.sqrt(TRADING_DAYS_PER_YEAR)),
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "calmar": float(cagr / abs(max_drawdown)) if max_drawdown < 0 else 0.0,
        "capital_exposure": float((position * weights).sum(axis=1).mean()),
        "weighted_turnover_sides": float((turnover * weights).sum(axis=1).sum()),
        "rebalance_events": int((turnover.sum(axis=1) > 0).sum()),
    }


def prediction_quality(prediction: pd.DataFrame, target: pd.DataFrame) -> dict[str, float]:
    pred = prediction.stack()
    actual = target.loc[prediction.index, prediction.columns].stack()
    daily_rank = prediction.corrwith(target.loc[prediction.index], axis=1, method="spearman").dropna()
    return {
        "pearson_ic": float(pred.corr(actual, method="pearson")),
        "rank_ic": float(pred.rank().corr(actual.rank(), method="pearson")),
        "mean_daily_cross_sectional_rank_ic": float(daily_rank.mean()),
        "directional_accuracy": float(((pred > 0) == (actual > 0)).mean()),
    }


def main() -> None:
    args = parse_args()
    models = [item.strip() for item in args.models.split(",") if item.strip()]
    unknown = set(models) - {"lightgbm", "double_ensemble", "gru"}
    if unknown:
        raise ValueError(f"unsupported models: {sorted(unknown)}")
    if args.rebalance_days < 1:
        raise ValueError("rebalance-days must be positive")
    random.seed(args.seed)
    np.random.seed(args.seed)
    weights = parse_weights(args.weights)
    panels, feature_columns, dates = load_panel(args.data_dir, args.horizon_days)
    tabular, target_long, tabular_index = make_tabular(panels, feature_columns, dates)
    folds = make_folds(dates, args.train_days, args.validation_days, args.test_days)
    predictions = {
        model: pd.DataFrame(np.nan, index=dates, columns=ASSETS, dtype=float) for model in models
    }
    fold_records: list[Fold] = []

    for fit_dates, validation_dates, test_dates, fold in folds:
        print(
            f"fold={fold.number} train={fold.train_start[:10]}..{fold.train_end[:10]} "
            f"valid={fold.validation_start[:10]}..{fold.validation_end[:10]} "
            f"test={fold.test_start[:10]}..{fold.test_end[:10]}",
            flush=True,
        )
        fit_rows = rows_for_dates(tabular_index, fit_dates)
        validation_rows = rows_for_dates(tabular_index, validation_dates)
        test_rows = rows_for_dates(tabular_index, test_dates)
        x_fit, y_fit = tabular.iloc[fit_rows], target_long.iloc[fit_rows]
        x_valid, y_valid = tabular.iloc[validation_rows], target_long.iloc[validation_rows]
        x_test = tabular.iloc[test_rows]
        test_index = tabular_index[test_rows]

        if "lightgbm" in models:
            output = fit_lightgbm(
                x_fit,
                y_fit,
                x_valid,
                y_valid,
                x_test,
                args.seed + fold.number,
            )
            fold_prediction = pd.Series(output, index=test_index).unstack("asset")
            predictions["lightgbm"].loc[test_dates, ASSETS] = fold_prediction.loc[test_dates, ASSETS]
        if "double_ensemble" in models:
            ensemble = DoubleEnsembleRegressor(seed=args.seed + fold.number)
            ensemble.fit(x_fit, y_fit, x_valid, y_valid)
            output = ensemble.predict(x_test)
            fold_prediction = pd.Series(output, index=test_index).unstack("asset")
            predictions["double_ensemble"].loc[test_dates, ASSETS] = fold_prediction.loc[
                test_dates,
                ASSETS,
            ]
        if "gru" in models:
            fold_prediction = fit_gru(
                panels,
                feature_columns,
                dates,
                fit_dates,
                validation_dates,
                test_dates,
                args.sequence_days,
                args.gru_hidden_size,
                args.gru_epochs,
                args.seed + fold.number,
            )
            predictions["gru"].loc[test_dates, ASSETS] = fold_prediction
        fold_records.append(fold)

    evaluation_mask = pd.Series(True, index=dates)
    for prediction in predictions.values():
        evaluation_mask &= prediction.notna().all(axis=1)
    evaluation_dates = dates[evaluation_mask]
    if len(evaluation_dates) < 252:
        raise RuntimeError("fewer than 252 common out-of-sample dates")
    target = pd.DataFrame(
        {asset: panels[asset].loc[evaluation_dates, "target"] for asset in ASSETS},
        index=evaluation_dates,
    )
    trade_returns = pd.DataFrame(
        {asset: panels[asset].loc[evaluation_dates, "trade_return"] for asset in ASSETS},
        index=evaluation_dates,
    )
    entry_threshold = args.entry_threshold_bps / 10_000.0
    exit_threshold = args.exit_threshold_bps / 10_000.0
    cost_per_side = (args.fee_bps + args.slippage_bps) / 10_000.0
    positions: dict[str, pd.DataFrame] = {}
    metrics: dict[str, dict[str, float | int]] = {}
    quality: dict[str, dict[str, float]] = {}
    equity: dict[str, pd.Series] = {}
    cost_sensitivity: dict[str, dict[str, dict[str, float | int]]] = {}

    for model, prediction in predictions.items():
        selected_prediction = prediction.loc[evaluation_dates]
        position = prediction_to_position(
            selected_prediction,
            entry_threshold,
            exit_threshold,
            args.rebalance_days,
        )
        positions[model] = position
        equity[model], metrics[model] = portfolio_performance(
            position,
            trade_returns,
            weights,
            cost_per_side,
        )
        quality[model] = prediction_quality(selected_prediction, target)
        cost_sensitivity[model] = {}
        for cost_bps in (0.0, 2.0, 5.0, 10.0, args.fee_bps + args.slippage_bps):
            _, sensitivity = portfolio_performance(
                position,
                trade_returns,
                weights,
                cost_bps / 10_000.0,
            )
            cost_sensitivity[model][f"{cost_bps:g}_bps_per_side"] = sensitivity

    buy_hold = pd.DataFrame(1.0, index=evaluation_dates, columns=ASSETS)
    trend = pd.DataFrame(
        {asset: panels[asset].loc[evaluation_dates, "trend_position"] for asset in ASSETS},
        index=evaluation_dates,
    )
    for name, position in {"buy_hold": buy_hold, "trend": trend}.items():
        positions[name] = position
        equity[name], metrics[name] = portfolio_performance(
            position,
            trade_returns,
            weights,
            cost_per_side,
        )

    individual_drawdowns = {}
    for asset in ASSETS:
        asset_equity = (1.0 + trade_returns[asset]).cumprod()
        individual_drawdowns[asset] = float((asset_equity / asset_equity.cummax() - 1.0).min())

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"timestamp": evaluation_dates, **equity}).to_csv(
        args.output_dir / "equity.csv",
        index=False,
    )
    prediction_rows = []
    for model, prediction in predictions.items():
        for date in evaluation_dates:
            for asset in ASSETS:
                prediction_rows.append(
                    {
                        "timestamp": date,
                        "asset": asset,
                        "model": model,
                        "prediction": prediction.loc[date, asset],
                        "position": positions[model].loc[date, asset],
                        "target": target.loc[date, asset],
                        "trade_return": trade_returns.loc[date, asset],
                    },
                )
    pd.DataFrame(prediction_rows).to_csv(args.output_dir / "predictions.csv", index=False)
    report = {
        "assets": list(ASSETS),
        "weights": weights.to_dict(),
        "source": "Yahoo adjusted underlying daily history; SK Hynix converted to USD",
        "evaluation_start": evaluation_dates[0].isoformat(),
        "evaluation_end": evaluation_dates[-1].isoformat(),
        "evaluation_days": len(evaluation_dates),
        "feature_count": len(feature_columns) + len(ASSETS),
        "config": {
            "models": models,
            "horizon_days": args.horizon_days,
            "rebalance_days": args.rebalance_days,
            "train_days": args.train_days,
            "validation_days": args.validation_days,
            "test_days": args.test_days,
            "entry_threshold_bps": args.entry_threshold_bps,
            "exit_threshold_bps": args.exit_threshold_bps,
            "fee_bps": args.fee_bps,
            "slippage_bps": args.slippage_bps,
            "sequence_days": args.sequence_days,
            "gru_hidden_size": args.gru_hidden_size,
            "gru_epochs": args.gru_epochs,
            "seed": args.seed,
        },
        "folds": [asdict(fold) for fold in fold_records],
        "prediction_quality": quality,
        "cost_sensitivity": cost_sensitivity,
        "individual_buy_hold_max_drawdown": individual_drawdowns,
        "metrics": metrics,
    }
    (args.output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output_dir), "metrics": metrics}, indent=2))


if __name__ == "__main__":
    main()
