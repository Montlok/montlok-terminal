#!/usr/bin/env python3
"""Download confirmed OKX historical candles with bounded pagination and validation."""

from __future__ import annotations

import argparse
import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd


BASE_URL = "https://openapi.okx.com"
HISTORY_PATH = "/api/v5/market/history-candles"
BAR_MILLISECONDS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "2H": 7_200_000,
    "4H": 14_400_000,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inst-id", default="BTC-USDT")
    parser.add_argument("--bar", choices=sorted(BAR_MILLISECONDS), default="1H")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--output", type=Path, default=Path("data/BTC-USDT-1H.csv"))
    parser.add_argument("--request-interval", type=float, default=0.13)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--max-pages", type=int, default=10_000)
    return parser.parse_args()


def request_page(
    inst_id: str,
    bar: str,
    after: int | None,
    timeout: float,
) -> list[list[str]]:
    params = {"instId": inst_id, "bar": bar, "limit": "100"}
    if after is not None:
        params["after"] = str(after)
    url = f"{BASE_URL}{HISTORY_PATH}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": "nautilus-crypto-alpha/1"})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read())
            if payload.get("code") != "0":
                raise RuntimeError(f"OKX returned code={payload.get('code')} msg={payload.get('msg')}")
            data = payload.get("data")
            if not isinstance(data, list):
                raise RuntimeError("OKX candle response does not contain a data list")
            return data
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 5:
                raise
            time.sleep(min(2.0, 0.25 * (2**attempt)))
        except (
            ConnectionResetError,
            TimeoutError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
            urllib.error.URLError,
        ):
            if attempt == 5:
                raise
            time.sleep(min(2.0, 0.25 * (2**attempt)))
    raise AssertionError("unreachable")


def download(args: argparse.Namespace) -> pd.DataFrame:
    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1_000)
    after: int | None = None
    rows: dict[int, list[str]] = {}

    for page_number in range(1, args.max_pages + 1):
        page = request_page(args.inst_id, args.bar, after, args.timeout)
        if not page:
            break
        page_oldest = min(int(item[0]) for item in page)
        for item in page:
            if len(item) != 9:
                raise RuntimeError(f"unexpected OKX candle width: {len(item)}")
            if item[8] == "1":
                rows[int(item[0])] = item
        if page_number % 25 == 0:
            print(
                f"pages={page_number} rows={len(rows)} "
                f"oldest={pd.to_datetime(page_oldest, unit='ms', utc=True).isoformat()}",
                flush=True,
            )
        if page_oldest <= start_ms:
            break
        if after is not None and page_oldest >= after:
            raise RuntimeError("OKX history pagination stopped moving backwards")
        after = page_oldest
        time.sleep(args.request_interval)
    else:
        raise RuntimeError("maximum page count reached before requested start")

    selected = [rows[key] for key in sorted(rows) if key >= start_ms]
    if not selected:
        raise RuntimeError("no confirmed candles returned for requested interval")

    frame = pd.DataFrame(
        selected,
        columns=["ts_ms", "open", "high", "low", "close", "volume", "volume_ccy", "volume_quote", "confirm"],
    )
    frame["ts_ms"] = frame["ts_ms"].astype("int64")
    for column in ["open", "high", "low", "close", "volume", "volume_ccy", "volume_quote"]:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
    frame["timestamp"] = pd.to_datetime(frame["ts_ms"], unit="ms", utc=True)
    frame = frame.drop(columns=["confirm"]).sort_values("ts_ms").reset_index(drop=True)

    if frame["ts_ms"].duplicated().any():
        raise RuntimeError("duplicate candle timestamps remain after normalization")
    deltas = frame["ts_ms"].diff().dropna()
    expected = BAR_MILLISECONDS[args.bar]
    gaps = deltas[deltas != expected]
    if len(gaps):
        examples = frame.loc[gaps.index[:5], ["timestamp", "ts_ms"]].to_dict("records")
        raise RuntimeError(f"detected {len(gaps)} candle gaps; examples={examples}")
    if not ((frame["low"] <= frame[["open", "close"]].min(axis=1)).all() and (frame["high"] >= frame[["open", "close"]].max(axis=1)).all()):
        raise RuntimeError("OHLC ordering validation failed")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise RuntimeError("non-positive BTC-USDT price detected")

    return frame


def main() -> None:
    args = parse_args()
    frame = download(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(
        json.dumps(
            {
                "instrument": args.inst_id,
                "bar": args.bar,
                "rows": len(frame),
                "start": frame["timestamp"].iloc[0].isoformat(),
                "end": frame["timestamp"].iloc[-1].isoformat(),
                "output": str(args.output),
            },
            indent=2,
        ),
    )


if __name__ == "__main__":
    main()
