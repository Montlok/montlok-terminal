#!/usr/bin/env python3
"""Walk-forward BTC-USDT long/flat model comparison with explicit trading costs."""

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
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler


HOURS_PER_YEAR = 24 * 365


@dataclass(frozen=True)
class Fold:
    number: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    train_rows: int
    test_rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=Path("data/BTC-USDT-1H.csv"))
    parser.add_argument("--models", default="hist_gbdt,gru")
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/btc-usdt-1h"))
    parser.add_argument("--horizon-hours", type=int, default=6)
    parser.add_argument("--train-days", type=int, default=365)
    parser.add_argument("--test-days", type=int, default=60)
    parser.add_argument("--sequence-hours", type=int, default=48)
    parser.add_argument("--rebalance-hours", type=int, default=6)
    parser.add_argument("--signal-threshold-bps", type=float, default=10.0)
    parser.add_argument("--exit-threshold-bps", type=float, default=0.0)
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--gru-hidden-size", type=int, default=32)
    parser.add_argument("--gru-epochs", type=int, default=8)
    parser.add_argument("--seed", type=int, default=7)
    return parser.parse_args()


def load_frame(path: Path, horizon_hours: int) -> tuple[pd.DataFrame, list[str]]:
    frame = pd.read_csv(path, parse_dates=["timestamp"])
    frame = frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    required = {"timestamp", "open", "high", "low", "close", "volume", "volume_quote"}
    missing = required - set(frame)
    if missing:
        raise ValueError(f"missing required candle columns: {sorted(missing)}")
    if horizon_hours < 1:
        raise ValueError("horizon-hours must be positive")

    close = frame["close"].astype(float)
    log_close = np.log(close)
    log_return = log_close.diff()
    frame["range"] = (frame["high"] - frame["low"]) / close
    frame["body"] = (frame["close"] - frame["open"]) / frame["open"]
    frame["upper_wick"] = (frame["high"] - frame[["open", "close"]].max(axis=1)) / close
    frame["lower_wick"] = (frame[["open", "close"]].min(axis=1) - frame["low"]) / close

    feature_columns: list[str] = ["range", "body", "upper_wick", "lower_wick"]
    for lag in (1, 2, 3, 6, 12, 24, 48, 72, 168):
        column = f"return_lag_{lag}"
        frame[column] = log_return.shift(lag - 1)
        feature_columns.append(column)
    for window in (6, 12, 24, 48, 72, 168):
        for name, values in (
            ("return_mean", log_return.rolling(window).mean()),
            ("return_vol", log_return.rolling(window).std()),
            ("momentum", close.pct_change(window)),
            ("range_mean", frame["range"].rolling(window).mean()),
        ):
            column = f"{name}_{window}"
            frame[column] = values
            feature_columns.append(column)

    log_volume = np.log1p(frame["volume_quote"].astype(float))
    for window in (24, 72, 168):
        column = f"volume_z_{window}"
        rolling = log_volume.rolling(window)
        frame[column] = (log_volume - rolling.mean()) / rolling.std()
        feature_columns.append(column)

    timestamp = pd.to_datetime(frame["timestamp"], utc=True)
    hour = timestamp.dt.hour
    weekday = timestamp.dt.dayofweek
    frame["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    frame["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    frame["weekday_sin"] = np.sin(2 * np.pi * weekday / 7)
    frame["weekday_cos"] = np.cos(2 * np.pi * weekday / 7)
    feature_columns.extend(["hour_sin", "hour_cos", "weekday_sin", "weekday_cos"])

    frame["target"] = close.shift(-horizon_hours) / close - 1.0
    frame["trade_return"] = frame["open"].shift(-2) / frame["open"].shift(-1) - 1.0
    frame["trend_position"] = (
        close.rolling(24).mean() > close.rolling(168).mean()
    ).astype(float)
    frame = frame.dropna(subset=feature_columns + ["target", "trade_return"]).reset_index(drop=True)

    if not frame["timestamp"].is_monotonic_increasing:
        raise ValueError("timestamps are not monotonic")
    if not np.isfinite(frame[feature_columns + ["target", "trade_return"]].to_numpy()).all():
        raise ValueError("non-finite feature or target values remain")
    return frame, feature_columns


def make_folds(frame: pd.DataFrame, train_days: int, test_days: int) -> list[tuple[np.ndarray, np.ndarray, Fold]]:
    train_rows = train_days * 24
    test_rows = test_days * 24
    if train_rows < 30 * 24 or test_rows < 24:
        raise ValueError("train/test windows are too short")
    folds: list[tuple[np.ndarray, np.ndarray, Fold]] = []
    test_start = train_rows
    number = 1
    while test_start < len(frame):
        train_start = max(0, test_start - train_rows)
        test_end = min(len(frame), test_start + test_rows)
        train_index = np.arange(train_start, test_start)
        test_index = np.arange(test_start, test_end)
        if len(test_index) < 24:
            break
        fold = Fold(
            number=number,
            train_start=frame.loc[train_index[0], "timestamp"].isoformat(),
            train_end=frame.loc[train_index[-1], "timestamp"].isoformat(),
            test_start=frame.loc[test_index[0], "timestamp"].isoformat(),
            test_end=frame.loc[test_index[-1], "timestamp"].isoformat(),
            train_rows=len(train_index),
            test_rows=len(test_index),
        )
        folds.append((train_index, test_index, fold))
        test_start = test_end
        number += 1
    if not folds:
        raise ValueError("dataset is too short for one walk-forward fold")
    return folds


def fit_predict_hist_gbdt(
    features: np.ndarray,
    target: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    seed: int,
) -> np.ndarray:
    model = HistGradientBoostingRegressor(
        learning_rate=0.05,
        max_iter=250,
        max_leaf_nodes=31,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=0.15,
        random_state=seed,
    )
    model.fit(features[train_index], target[train_index])
    return model.predict(features[test_index])


def fit_predict_lightgbm(
    features: np.ndarray,
    target: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    seed: int,
) -> np.ndarray:
    import lightgbm as lgb

    validation_rows = max(1, int(len(train_index) * 0.15))
    fit_index = train_index[:-validation_rows]
    validation_index = train_index[-validation_rows:]
    model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=1_000,
        learning_rate=0.03,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=100,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=seed,
        n_jobs=4,
        verbosity=-1,
    )
    model.fit(
        features[fit_index],
        target[fit_index],
        eval_set=[(features[validation_index], target[validation_index])],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)],
    )
    return model.predict(features[test_index])


def fit_predict_gru(
    features: np.ndarray,
    target: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    sequence_hours: int,
    hidden_size: int,
    epochs: int,
    seed: int,
) -> np.ndarray:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader
    from torch.utils.data import TensorDataset

    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True)
    torch.set_num_threads(max(1, min(4, torch.get_num_threads())))

    scaler = StandardScaler().fit(features[train_index])
    scaled = scaler.transform(features).astype(np.float32)
    target_scale = 1_000.0

    def sequences(indices: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        valid = indices[indices >= sequence_hours - 1]
        x = np.stack([scaled[index - sequence_hours + 1 : index + 1] for index in valid])
        y = target[valid].astype(np.float32) * target_scale
        return x, y

    train_x, train_y = sequences(train_index)
    test_x, _ = sequences(test_index)
    if len(train_x) < 100 or len(test_x) != len(test_index):
        raise ValueError("insufficient contiguous rows for GRU sequence construction")

    validation_rows = max(1, int(len(train_x) * 0.15))
    fit_x, valid_x = train_x[:-validation_rows], train_x[-validation_rows:]
    fit_y, valid_y = train_y[:-validation_rows], train_y[-validation_rows:]

    class GRURegressor(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.gru = nn.GRU(scaled.shape[1], hidden_size, batch_first=True)
            self.output = nn.Linear(hidden_size, 1)

        def forward(self, values: torch.Tensor) -> torch.Tensor:
            encoded, _ = self.gru(values)
            return self.output(encoded[:, -1]).squeeze(-1)

    model = GRURegressor()
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = nn.HuberLoss(delta=1.0)
    generator = torch.Generator().manual_seed(seed)
    loader = DataLoader(
        TensorDataset(torch.from_numpy(fit_x), torch.from_numpy(fit_y)),
        batch_size=256,
        shuffle=True,
        generator=generator,
    )
    valid_tensor = torch.from_numpy(valid_x)
    valid_target = torch.from_numpy(valid_y)
    best_state: dict[str, torch.Tensor] | None = None
    best_loss = math.inf
    stale_epochs = 0

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
            valid_loss = float(loss_fn(model(valid_tensor), valid_target))
        if valid_loss < best_loss - 1e-6:
            best_loss = valid_loss
            best_state = {name: value.detach().clone() for name, value in model.state_dict().items()}
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= 2:
                break

    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        prediction = model(torch.from_numpy(test_x)).numpy()
    return prediction / target_scale


def positions_from_predictions(
    prediction: pd.Series,
    entry_threshold: float,
    exit_threshold: float,
    rebalance_hours: int,
) -> pd.Series:
    if rebalance_hours < 1:
        raise ValueError("rebalance-hours must be positive")
    position = np.zeros(len(prediction), dtype=float)
    held = 0.0
    for index, value in enumerate(prediction.to_numpy()):
        if index % rebalance_hours == 0:
            if value > entry_threshold:
                held = 1.0
            elif value < exit_threshold:
                held = 0.0
        position[index] = held
    return pd.Series(position, index=prediction.index)


def performance(position: pd.Series, trade_return: pd.Series, cost_per_side: float) -> tuple[pd.Series, dict[str, float | int]]:
    position = position.astype(float).clip(0.0, 1.0)
    turnover = position.diff().abs()
    turnover.iloc[0] = abs(position.iloc[0])
    net_return = position * trade_return - turnover * cost_per_side
    equity = (1.0 + net_return).cumprod()
    running_max = equity.cummax()
    drawdown = equity / running_max - 1.0
    years = len(net_return) / HOURS_PER_YEAR
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0) if years > 0 and equity.iloc[-1] > 0 else -1.0
    volatility = float(net_return.std(ddof=1) * math.sqrt(HOURS_PER_YEAR))
    sharpe = float(net_return.mean() / net_return.std(ddof=1) * math.sqrt(HOURS_PER_YEAR)) if net_return.std(ddof=1) > 0 else 0.0
    max_drawdown = float(drawdown.min())
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else 0.0
    active = net_return[position > 0]
    metrics: dict[str, float | int] = {
        "total_return": total_return,
        "cagr": cagr,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "calmar": calmar,
        "exposure": float(position.mean()),
        "turnover_sides": float(turnover.sum()),
        "position_changes": int((turnover > 0).sum()),
        "active_hour_win_rate": float((active > 0).mean()) if len(active) else 0.0,
    }
    return equity, metrics


def prediction_quality(prediction: pd.Series, target: pd.Series) -> dict[str, float]:
    prediction = prediction.reset_index(drop=True)
    target = target.reset_index(drop=True)
    return {
        "pearson_ic": float(prediction.corr(target, method="pearson")),
        "rank_ic": float(prediction.rank().corr(target.rank(), method="pearson")),
        "directional_accuracy": float(((prediction > 0) == (target > 0)).mean()),
        "prediction_mean": float(prediction.mean()),
        "prediction_std": float(prediction.std(ddof=1)),
    }


def main() -> None:
    args = parse_args()
    models = [item.strip() for item in args.models.split(",") if item.strip()]
    unknown = set(models) - {"hist_gbdt", "lightgbm", "gru"}
    if unknown:
        raise ValueError(f"unsupported models: {sorted(unknown)}")
    random.seed(args.seed)
    np.random.seed(args.seed)

    frame, feature_columns = load_frame(args.data, args.horizon_hours)
    features = frame[feature_columns].to_numpy(dtype=np.float64)
    target = frame["target"].to_numpy(dtype=np.float64)
    folds = make_folds(frame, args.train_days, args.test_days)
    predictions = {name: np.full(len(frame), np.nan, dtype=float) for name in models}
    fold_records: list[Fold] = []

    for train_index, test_index, fold in folds:
        print(
            f"fold={fold.number} train={fold.train_start[:10]}..{fold.train_end[:10]} "
            f"test={fold.test_start[:10]}..{fold.test_end[:10]}",
            flush=True,
        )
        if "hist_gbdt" in models:
            predictions["hist_gbdt"][test_index] = fit_predict_hist_gbdt(
                features,
                target,
                train_index,
                test_index,
                args.seed + fold.number,
            )
        if "lightgbm" in models:
            predictions["lightgbm"][test_index] = fit_predict_lightgbm(
                features,
                target,
                train_index,
                test_index,
                args.seed + fold.number,
            )
        if "gru" in models:
            predictions["gru"][test_index] = fit_predict_gru(
                features,
                target,
                train_index,
                test_index,
                args.sequence_hours,
                args.gru_hidden_size,
                args.gru_epochs,
                args.seed + fold.number,
            )
        fold_records.append(fold)

    evaluation_mask = np.ones(len(frame), dtype=bool)
    for values in predictions.values():
        evaluation_mask &= np.isfinite(values)
    evaluation = frame.loc[evaluation_mask].copy().reset_index(drop=True)
    if evaluation.empty:
        raise RuntimeError("walk-forward models produced no common out-of-sample rows")

    entry_threshold = args.signal_threshold_bps / 10_000.0
    exit_threshold = args.exit_threshold_bps / 10_000.0
    cost_per_side = (args.fee_bps + args.slippage_bps) / 10_000.0
    result_columns: dict[str, pd.Series] = {}
    equity_columns: dict[str, pd.Series] = {}
    metrics: dict[str, dict[str, float | int]] = {}
    quality: dict[str, dict[str, float]] = {}
    cost_sensitivity: dict[str, dict[str, dict[str, float | int]]] = {}

    for name, values in predictions.items():
        prediction = pd.Series(values[evaluation_mask])
        position = positions_from_predictions(
            prediction,
            entry_threshold,
            exit_threshold,
            args.rebalance_hours,
        )
        equity, model_metrics = performance(position, evaluation["trade_return"], cost_per_side)
        result_columns[f"prediction_{name}"] = prediction
        result_columns[f"position_{name}"] = position
        equity_columns[name] = equity
        metrics[name] = model_metrics
        quality[name] = prediction_quality(prediction, evaluation["target"])
        cost_sensitivity[name] = {}
        for cost_bps in (0.0, 2.0, 5.0, 10.0, args.fee_bps + args.slippage_bps):
            _, sensitivity_metrics = performance(
                position,
                evaluation["trade_return"],
                cost_bps / 10_000.0,
            )
            cost_sensitivity[name][f"{cost_bps:g}_bps_per_side"] = sensitivity_metrics

    benchmark_positions = {
        "trend": evaluation["trend_position"].reset_index(drop=True),
        "buy_hold": pd.Series(np.ones(len(evaluation))),
    }
    for name, position in benchmark_positions.items():
        equity, benchmark_metrics = performance(position, evaluation["trade_return"], cost_per_side)
        result_columns[f"position_{name}"] = position
        equity_columns[name] = equity
        metrics[name] = benchmark_metrics

    args.output_dir.mkdir(parents=True, exist_ok=True)
    prediction_output = pd.DataFrame(
        {
            "timestamp": evaluation["timestamp"],
            "close": evaluation["close"],
            "target": evaluation["target"],
            "trade_return": evaluation["trade_return"],
            **result_columns,
        },
    )
    prediction_output.to_csv(args.output_dir / "predictions.csv", index=False)
    pd.DataFrame({"timestamp": evaluation["timestamp"], **equity_columns}).to_csv(
        args.output_dir / "equity.csv",
        index=False,
    )

    report = {
        "instrument": "BTC-USDT",
        "bar": "1H",
        "evaluation_start": evaluation["timestamp"].iloc[0].isoformat(),
        "evaluation_end": evaluation["timestamp"].iloc[-1].isoformat(),
        "evaluation_rows": len(evaluation),
        "feature_count": len(feature_columns),
        "features": feature_columns,
        "config": {
            "models": models,
            "horizon_hours": args.horizon_hours,
            "train_days": args.train_days,
            "test_days": args.test_days,
            "sequence_hours": args.sequence_hours,
            "rebalance_hours": args.rebalance_hours,
            "signal_threshold_bps": args.signal_threshold_bps,
            "exit_threshold_bps": args.exit_threshold_bps,
            "fee_bps": args.fee_bps,
            "slippage_bps": args.slippage_bps,
            "gru_hidden_size": args.gru_hidden_size,
            "gru_epochs": args.gru_epochs,
            "seed": args.seed,
        },
        "folds": [asdict(item) for item in fold_records],
        "prediction_quality": quality,
        "cost_sensitivity": cost_sensitivity,
        "metrics": metrics,
    }
    (args.output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output_dir), "metrics": metrics}, indent=2))


if __name__ == "__main__":
    main()
