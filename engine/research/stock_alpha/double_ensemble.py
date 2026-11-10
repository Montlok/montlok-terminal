#!/usr/bin/env python3
"""Standalone LightGBM DoubleEnsemble adapted from Microsoft Qlib's MIT implementation."""

# Upstream: https://github.com/microsoft/qlib/blob/main/qlib/contrib/model/double_ensemble.py
# Copyright (c) Microsoft Corporation. Licensed under the MIT License.

from __future__ import annotations

import numpy as np
import pandas as pd


class DoubleEnsembleRegressor:
    """Qlib-compatible sample reweighting and feature-selection ensemble."""

    def __init__(
        self,
        *,
        num_models: int = 6,
        epochs: int = 150,
        early_stopping_rounds: int = 30,
        alpha1: float = 1.0,
        alpha2: float = 1.0,
        bins_sr: int = 10,
        bins_fs: int = 5,
        decay: float = 0.9,
        sample_ratios: tuple[float, ...] = (0.8, 0.7, 0.6, 0.5, 0.4),
        seed: int = 7,
    ) -> None:
        if len(sample_ratios) != bins_fs:
            raise ValueError("sample_ratios length must equal bins_fs")
        self.num_models = num_models
        self.epochs = epochs
        self.early_stopping_rounds = early_stopping_rounds
        self.alpha1 = alpha1
        self.alpha2 = alpha2
        self.bins_sr = bins_sr
        self.bins_fs = bins_fs
        self.decay = decay
        self.sample_ratios = sample_ratios
        self.seed = seed
        self.ensemble: list[object] = []
        self.sub_features: list[pd.Index] = []
        self._rng = np.random.default_rng(seed)

    def fit(
        self,
        x_train: pd.DataFrame,
        y_train: pd.Series,
        x_valid: pd.DataFrame,
        y_valid: pd.Series,
    ) -> DoubleEnsembleRegressor:
        import lightgbm as lgb

        weights = pd.Series(np.ones(len(x_train)), index=x_train.index, dtype=float)
        features = x_train.columns
        pred_sub = pd.DataFrame(
            np.zeros((len(x_train), self.num_models)),
            index=x_train.index,
        )
        self.ensemble.clear()
        self.sub_features.clear()
        for model_index in range(self.num_models):
            self.sub_features.append(features)
            train_set = lgb.Dataset(
                x_train.loc[:, features],
                label=y_train,
                weight=weights,
                free_raw_data=False,
            )
            valid_set = lgb.Dataset(
                x_valid.loc[:, features],
                label=y_valid,
                reference=train_set,
                free_raw_data=False,
            )
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
                    "seed": self.seed + model_index,
                    "num_threads": 4,
                },
                train_set,
                num_boost_round=self.epochs,
                valid_sets=[valid_set],
                callbacks=[
                    lgb.early_stopping(self.early_stopping_rounds, verbose=False),
                    lgb.log_evaluation(0),
                ],
            )
            self.ensemble.append(model)
            if model_index + 1 == self.num_models:
                break
            loss_curve = self._retrieve_loss_curve(model, x_train.loc[:, features], y_train)
            pred_sub.iloc[:, model_index] = model.predict(x_train.loc[:, features])
            ensemble_prediction = pred_sub.iloc[:, : model_index + 1].mean(axis=1)
            loss_values = (y_train - ensemble_prediction) ** 2
            weights = self._sample_reweight(loss_curve, loss_values, model_index + 1)
            features = self._feature_selection(x_train, y_train, loss_values)
        return self

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        if not self.ensemble:
            raise ValueError("model is not fitted")
        prediction = np.zeros(len(features), dtype=float)
        for model, columns in zip(self.ensemble, self.sub_features, strict=True):
            prediction += model.predict(features.loc[:, columns])
        return prediction / len(self.ensemble)

    @staticmethod
    def _loss(label: np.ndarray, prediction: np.ndarray) -> np.ndarray:
        return (label - prediction) ** 2

    def _retrieve_loss_curve(
        self,
        model: object,
        features: pd.DataFrame,
        label: pd.Series,
    ) -> pd.DataFrame:
        prediction = np.zeros(len(features), dtype=float)
        curves = np.zeros((len(features), model.num_trees()), dtype=float)
        for tree_index in range(model.num_trees()):
            prediction += model.predict(
                features,
                start_iteration=tree_index,
                num_iteration=1,
            )
            curves[:, tree_index] = self._loss(label.to_numpy(), prediction)
        return pd.DataFrame(curves, index=features.index)

    def _sample_reweight(
        self,
        loss_curve: pd.DataFrame,
        loss_values: pd.Series,
        model_number: int,
    ) -> pd.Series:
        normalized_curve = loss_curve.rank(axis=0, pct=True)
        normalized_loss = (-loss_values).rank(pct=True)
        part = max(int(loss_curve.shape[1] * 0.1), 1)
        start_loss = normalized_curve.iloc[:, :part].mean(axis=1)
        end_loss = normalized_curve.iloc[:, -part:].mean(axis=1)
        h_value = self.alpha1 * normalized_loss + self.alpha2 * (end_loss / start_loss).rank(pct=True)
        bins = pd.cut(h_value, self.bins_sr, duplicates="drop")
        averages = h_value.groupby(bins, observed=False).mean()
        weights = pd.Series(np.zeros(len(h_value)), index=h_value.index, dtype=float)
        for interval, average in averages.items():
            if pd.notna(average):
                weights.loc[bins == interval] = 1.0 / (self.decay**model_number * average + 0.1)
        return weights

    def _feature_selection(
        self,
        features: pd.DataFrame,
        label: pd.Series,
        loss_values: pd.Series,
    ) -> pd.Index:
        importance = pd.Series(np.zeros(features.shape[1]), index=features.columns, dtype=float)
        shuffled = features.copy()
        for column in features.columns:
            shuffled.loc[:, column] = self._rng.permutation(shuffled[column].to_numpy())
            prediction = self.predict(shuffled)
            loss_delta = self._loss(label.to_numpy(), prediction) - loss_values.to_numpy()
            importance.loc[column] = np.mean(loss_delta) / (np.std(loss_delta) + 1e-7)
            shuffled.loc[:, column] = features[column]
        importance = importance.fillna(0.0)
        bins = pd.cut(importance, self.bins_fs, duplicates="drop")
        selected: list[str] = []
        ordered_bins = sorted((item for item in bins.dropna().unique()), reverse=True)
        for bin_index, interval in enumerate(ordered_bins):
            candidates = importance.index[bins == interval].to_numpy()
            ratio = self.sample_ratios[min(bin_index, len(self.sample_ratios) - 1)]
            count = max(1, int(np.ceil(ratio * len(candidates))))
            selected.extend(self._rng.choice(candidates, size=count, replace=False).tolist())
        if not selected:
            return features.columns
        return pd.Index(dict.fromkeys(selected))
