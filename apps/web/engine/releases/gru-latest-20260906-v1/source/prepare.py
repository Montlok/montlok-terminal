"""Prepare causal multi-market BTC features and chronological training arrays."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

BAR = pd.Timedelta(minutes=15)
INSTRUMENTS = ("BTC-USDT", "BTC-USDT-SWAP", "ETH-USDT")


def read_market(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["timestamp"]).set_index("timestamp").sort_index()
    if frame.index.has_duplicates or not frame.index.to_series().diff().iloc[1:].eq(BAR).all():
        raise ValueError(f"Duplicate or missing bars: {path}")
    columns = ["open", "high", "low", "close", "volume_quote"]
    if not np.isfinite(frame[columns].to_numpy()).all():
        raise ValueError(f"Nonfinite data: {path}")
    return frame


def features(markets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    btc, swap, eth = (markets[name] for name in INSTRUMENTS)
    x = pd.DataFrame(index=btc.index)
    r = np.log(btc.close).diff()
    for lag in (1, 2, 4, 8, 16, 32, 96):
        x[f"btc_return_lag_{lag}"] = r.shift(lag - 1)
        x[f"btc_momentum_{lag}"] = btc.close.pct_change(lag, fill_method=None)
    for window in (4, 16, 96):
        x[f"btc_vol_{window}"] = r.rolling(window).std()
        volume = np.log1p(btc.volume_quote)
        x[f"btc_volume_z_{window}"] = (volume - volume.rolling(window).mean()) / volume.rolling(window).std()
    x["btc_range"] = (btc.high - btc.low) / btc.close
    x["btc_body"] = (btc.close - btc.open) / btc.open
    basis = swap.close / btc.close - 1
    x["perp_spot_basis"] = basis
    for lag in (1, 4, 16):
        x[f"basis_change_{lag}"] = basis.diff(lag)
    x["basis_z_96"] = (basis - basis.rolling(96).mean()) / basis.rolling(96).std()
    x["perp_spot_log_volume_ratio"] = np.log1p(swap.volume_quote) - np.log1p(btc.volume_quote)
    for window in (1, 4, 16, 96):
        er = eth.close.pct_change(window, fill_method=None)
        x[f"eth_momentum_{window}"] = er
        x[f"btc_eth_relative_{window}"] = btc.close.pct_change(window, fill_method=None) - er
    er = np.log(eth.close).diff()
    x["eth_vol_16"] = er.rolling(16).std()
    x["btc_eth_corr_96"] = r.rolling(96).corr(er)
    hours = btc.index.hour + btc.index.minute / 60
    x["hour_sin"] = np.sin(2 * np.pi * hours / 24)
    x["hour_cos"] = np.cos(2 * np.pi * hours / 24)
    return x.replace([np.inf, -np.inf], np.nan)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[2] / "configs/btc_recent_20260905.json")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    markets = {name: read_market(args.data_dir / f"{name}-15m.csv") for name in INSTRUMENTS}
    start = max(frame.index[0] for frame in markets.values())
    end = min(frame.index[-1] for frame in markets.values())
    index = pd.date_range(start, end, freq="15min")
    markets = {name: frame.reindex(index) for name, frame in markets.items()}
    if any(frame.isna().any().any() for frame in markets.values()):
        raise ValueError("Aligned market data contain gaps")
    x = features(markets)
    btc = markets["BTC-USDT"]
    horizon, sequence = config["horizon_bars"], config["sequence_bars"]
    # Close-t inputs; enter at open t+1 and forecast exit at open t+5.
    y = btc.open.shift(-(horizon + 1)) / btc.open.shift(-1) - 1
    trade_return = btc.open.shift(-2) / btc.open.shift(-1) - 1
    valid_start = pd.Timestamp(config["validation_start"])
    test_start = pd.Timestamp(config["test_start"])
    valid_features = x.notna().all(axis=1)
    first = int(np.flatnonzero(valid_features.to_numpy())[0])
    x, y, trade_return = x.iloc[first:], y.iloc[first:], trade_return.iloc[first:]
    if not np.isfinite(x.to_numpy()).all():
        raise ValueError("Nonfinite features after warmup")
    stamps = x.index
    known_label_time = stamps + (horizon + 1) * BAR
    positions = np.arange(len(x))
    eligible = (positions >= sequence - 1) & y.notna().to_numpy() & trade_return.notna().to_numpy()
    train = np.flatnonzero(eligible & (known_label_time < valid_start))
    valid = np.flatnonzero(eligible & (stamps >= valid_start) & (known_label_time < test_start))
    test = np.flatnonzero(eligible & (stamps >= test_start))
    if len(train) < 5000 or len(valid) < 1000 or len(test) < 1000:
        raise ValueError("Insufficient chronological split sizes")
    mean = x.iloc[train].mean().to_numpy()
    scale = x.iloc[train].std(ddof=0).to_numpy()
    scale = np.where(scale < 1e-10, 1.0, scale)
    scaled = np.clip((x.to_numpy() - mean) / scale, -8, 8).astype(np.float32)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(args.output_dir / "dataset.npz", x=scaled, y=y.to_numpy(dtype=np.float32),
                        trade_return=trade_return.to_numpy(), train=train, valid=valid, test=test,
                        timestamps=stamps.as_unit("s").asi8, sequence=np.array(sequence), mean=mean, scale=scale)
    x.to_csv(args.output_dir / "features.csv", index_label="timestamp")
    sources = {}
    for name in INSTRUMENTS:
        path = args.data_dir / f"{name}-15m.csv"
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        sources[name] = {"path": str(path.resolve()), "sha256": digest}
    metadata = {"instrument": "BTC-USDT", "bar": "15m", "prepared_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                "feature_names": list(x.columns),
                "feature_count": len(x.columns), "sequence_bars": sequence, "horizon_bars": horizon,
                "feature_available": "after close t", "entry": "open t+1", "label_exit": "open t+5",
                "scaler": "train-only z-score clipped to [-8,8]", "sources": sources,
                "last_confirmed_bar": str(end), "splits": {},
                "threshold_grid_bps": config["threshold_grid_bps"], "fee_bps": config["fee_bps"], "slippage_bps": config["slippage_bps"],
                "experiment_config": config,
                "test_is_new_global_blind_holdout": False,
                "note": "Time-separated for this model; overlapping dates were used by earlier research."}
    for name, ix in (("train", train), ("valid", valid), ("test", test)):
        metadata["splits"][name] = {"rows": len(ix), "start": str(stamps[ix[0]]), "end": str(stamps[ix[-1]]),
                                     "last_label_time": str(known_label_time[ix[-1]])}
    (args.output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
