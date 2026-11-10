#!/usr/bin/env python3
"""Download adjusted Yahoo chart history for the listed stock underlyings."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd


CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
ASSETS = {
    "XNVDA": {"symbol": "NVDA", "currency": "USD"},
    "XMU": {"symbol": "MU", "currency": "USD"},
    "XSKHY": {"symbol": "000660.KS", "currency": "KRW"},
}
FX_SYMBOL = "KRW=X"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2016-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    return parser.parse_args()


def request_chart(symbol: str, start: str, end: str | None) -> pd.DataFrame:
    start_ts = int(pd.Timestamp(start, tz="UTC").timestamp())
    end_value = pd.Timestamp(end, tz="UTC") if end else pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)
    end_ts = int(end_value.timestamp())
    query = urllib.parse.urlencode(
        {
            "period1": start_ts,
            "period2": end_ts,
            "interval": "1d",
            "events": "div,splits",
            "includeAdjustedClose": "true",
        },
    )
    url = f"{CHART_URL.format(symbol=urllib.parse.quote(symbol))}?{query}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
    )
    payload: dict[str, object] | None = None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.loads(response.read())
            break
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt == 5:
                raise
            time.sleep(0.5 * (2**attempt))
        except (TimeoutError, urllib.error.URLError):
            if attempt == 5:
                raise
            time.sleep(0.5 * (2**attempt))
    if payload is None:
        raise RuntimeError(f"no response for {symbol}")
    chart = payload.get("chart")
    if not isinstance(chart, dict) or chart.get("error") is not None:
        raise RuntimeError(f"Yahoo chart error for {symbol}: {chart}")
    results = chart.get("result")
    if not isinstance(results, list) or not results:
        raise RuntimeError(f"Yahoo chart returned no result for {symbol}")
    result = results[0]
    timestamps = result["timestamp"]
    quote = result["indicators"]["quote"][0]
    adjusted = result["indicators"]["adjclose"][0]["adjclose"]
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps, unit="s", utc=True).normalize(),
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["close"],
            "volume": quote["volume"],
            "adj_close": adjusted,
        },
    )
    frame = frame.dropna(subset=["open", "high", "low", "close", "adj_close"]).copy()
    factor = frame["adj_close"] / frame["close"]
    for column in ("open", "high", "low", "close"):
        frame[column] = frame[column].astype(float) * factor
    frame["close"] = frame["adj_close"].astype(float)
    frame["volume"] = frame["volume"].fillna(0).astype(float)
    frame = frame.drop(columns=["adj_close"]).drop_duplicates("timestamp").sort_values("timestamp")
    if len(frame) < 500:
        raise RuntimeError(f"insufficient daily history for {symbol}: {len(frame)} rows")
    if not np.isfinite(frame[["open", "high", "low", "close", "volume"]]).all().all():
        raise RuntimeError(f"non-finite values remain for {symbol}")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise RuntimeError(f"non-positive prices found for {symbol}")
    return frame.reset_index(drop=True)


def convert_krw_to_usd(stock: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    fx_rates = fx[["timestamp", "close"]].rename(columns={"close": "krw_per_usd"})
    merged = pd.merge_asof(
        stock.sort_values("timestamp"),
        fx_rates.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=pd.Timedelta(days=5),
    )
    if merged["krw_per_usd"].isna().any():
        raise RuntimeError("KRW/USD coverage does not span SK Hynix history")
    for column in ("open", "high", "low", "close"):
        merged[column] = merged[column] / merged["krw_per_usd"]
    return merged.drop(columns=["krw_per_usd"])


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fx = request_chart(FX_SYMBOL, args.start, args.end)
    summary: dict[str, dict[str, object]] = {}
    for asset, config in ASSETS.items():
        frame = request_chart(config["symbol"], args.start, args.end)
        if config["currency"] == "KRW":
            frame = convert_krw_to_usd(frame, fx)
        frame.insert(0, "asset", asset)
        output = args.output_dir / f"{asset}.csv"
        frame.to_csv(output, index=False)
        summary[asset] = {
            "source_symbol": config["symbol"],
            "currency": "USD",
            "rows": len(frame),
            "start": frame["timestamp"].iloc[0].isoformat(),
            "end": frame["timestamp"].iloc[-1].isoformat(),
            "output": str(output),
        }
        time.sleep(0.5)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
