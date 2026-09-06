"""Nautilus actor producing original GRU features from actual OKX closed bars.

HTTP is public data only. No credentials, orders, paper market emulation, or
remote inference are used. Quote-currency volume comes from volCcyQuote; it is
never approximated as close * base volume.
"""
from __future__ import annotations

import asyncio
import json
import math
import ssl
import sys
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from model_probe import gru_window_from_markets
from model_release import import_file, sha

MARKETS = ("BTC-USDT", "BTC-USDT-SWAP", "ETH-USDT")


def parse_completed_candles(payload, now_ns, bar_seconds=900, minimum_rows=128):
    import pandas as pd
    if payload.get("code") != "0" or not isinstance(payload.get("data"), list):
        raise ValueError("OKX public candle request did not succeed")
    rows = []
    for values in payload["data"]:
        if not isinstance(values, list) or len(values) != 9:
            raise ValueError("Unexpected OKX candle schema")
        if values[8] != "1":
            continue
        timestamp = int(values[0])
        period_ms = bar_seconds * 1000
        if timestamp % period_ms or (timestamp + period_ms) * 1_000_000 > now_ns:
            raise ValueError("Confirmed candle has unclosed or misaligned timestamp")
        o, h, l, c = map(float, values[1:5])
        volume_quote = float(values[7])
        volume, volume_ccy = map(float, values[5:7])
        if not all(math.isfinite(v) for v in (o, h, l, c, volume, volume_ccy, volume_quote)) or min(o, h, l, c) <= 0 or min(volume, volume_ccy, volume_quote) < 0:
            raise ValueError("Invalid OHLC/quote volume")
        if h < max(o, c) or l > min(o, c) or h < l:
            raise ValueError("OHLC price ordering invalid")
        rows.append((timestamp, o, h, l, c, volume, volume_ccy, volume_quote))
    frame = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "volume_ccy", "volume_quote"])
    if len(frame) < minimum_rows:
        raise ValueError(f"Fewer than {minimum_rows} confirmed warmup bars")
    frame["ts_ms"] = frame["timestamp"].astype("int64")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame = frame.set_index("timestamp").sort_index()
    if frame.index.has_duplicates or not frame.index.to_series().diff().iloc[1:].eq(pd.Timedelta(seconds=bar_seconds)).all():
        raise ValueError("Duplicate or missing market bars")
    return frame


def request_public_candles(instrument, bar, after=None):
    allowed = {"BTC-USDT", "BTC-USDT-SWAP", "ETH-USDT", "XNVDA-USDT"}
    if instrument not in allowed or bar not in {"1m", "15m", "1H"}:
        raise ValueError("Unsupported published public candle input")
    params = {"instId": instrument, "bar": bar, "limit": "300"}
    if after is not None:
        params["after"] = str(after)
    url = "https://www.okx.com/api/v5/market/history-candles?" + urlencode(params)
    try:
        import certifi
        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        context = ssl.create_default_context()
    with urlopen(Request(url, headers={"Accept": "application/json", "User-Agent": "Montlok-Model-ClosedBars/1"}),
                 timeout=10, context=context) as response:
        from urllib.parse import urlparse
        if urlparse(response.url).hostname != "www.okx.com":
            raise ValueError("Unapproved public-market redirect")
        data = response.read(512001)
    if len(data) > 512000:
        raise ValueError("Oversized public candle response")
    return json.loads(data)


def fetch_bar_history(instrument, bar, bar_seconds, minimum_rows, requester=request_public_candles):
    """Bounded initial history fetch; production callers retain a rolling cache."""
    import pandas as pd
    parts, after = [], None
    for _ in range(5):
        page = parse_completed_candles(requester(instrument, bar, after), time.time_ns(), bar_seconds, 1)
        oldest = int(page.ts_ms.iloc[0])
        if after is not None and oldest >= after:
            raise ValueError("Historical candle pagination did not progress")
        parts.append(page)
        combined = pd.concat(parts).sort_index()
        if combined.index.has_duplicates:
            raise ValueError("Historical pages overlap completed timestamps")
        if len(combined) >= minimum_rows:
            if not combined.index.to_series().diff().iloc[1:].eq(pd.Timedelta(seconds=bar_seconds)).all():
                raise ValueError("Historical market warmup contains gaps")
            return combined.iloc[-minimum_rows:]
        after = oldest
    raise ValueError("Insufficient confirmed history within bounded initial fetch")


def rdt_feature_module(manifest, runner_root, contract_key):
    if contract_key not in {"crypto:BTC-USDT", "token_hour:XNVDA"}:
        raise ValueError("This RDT input contract is not connected to continuous public data")
    source_root = Path(runner_root).resolve()
    relative = "rdt4quant/prepare.py" if contract_key.startswith("crypto:") else "rdt4quant_fullpass/prepare.py"
    required = [relative] + (["rdt4quant/download.py"] if contract_key.startswith("crypto:") else [])
    records = {r["path"]: r for r in manifest["sources"]}
    for path in required:
        record = records.get("source/" + path)
        installed = (source_root / path).resolve()
        if record is None or not installed.is_relative_to(source_root) or sha(installed) != record["sha256"]:
            raise ValueError("Host-installed RDT feature source differs from reviewed release provenance")
    if contract_key.startswith("crypto:"):
        # prepare.py imports only atomic_json from its reviewed local download.
        sys.path.insert(0, str(source_root / "rdt4quant"))
    return import_file("montlok_rdt_features_" + contract_key.split(":")[0], source_root / relative)


def rdt_window_from_markets(manifest, markets, module, contract_key):
    import numpy as np
    import pandas as pd
    c = manifest["domainContracts"][contract_key]
    crypto = contract_key == "crypto:BTC-USDT"
    expected = MARKETS if crypto else ("XNVDA-USDT",)
    period = 60 if crypto else 3600
    minimum = (480 if crypto else 64) + c["sequenceBars"]
    if c["barSeconds"] != period or set(markets) != set(expected):
        raise ValueError("RDT market cadence/instrument mapping differs from selected contract")
    for frame in markets.values():
        if len(frame) < minimum or frame.index.has_duplicates or not frame.index.to_series().diff().iloc[1:].eq(pd.Timedelta(seconds=period)).all():
            raise ValueError("Incomplete or discontinuous RDT raw warmup")
    common = pd.date_range(max(f.index[0] for f in markets.values()), min(f.index[-1] for f in markets.values()), freq=f"{period}s")
    aligned = {name: markets[name].reindex(common).rename_axis("timestamp").reset_index() for name in expected}
    if len(common) < minimum or any(f.isna().any().any() for f in aligned.values()):
        raise ValueError("RDT aligned market inputs contain gaps")
    features = module.features(aligned) if crypto else module.features(aligned["XNVDA-USDT"], period)
    if list(features.columns) != c["names"]:
        raise ValueError("Original RDT feature names/order differ from the published checkpoint contract")
    x = features.iloc[-c["sequenceBars"]:].to_numpy(dtype="float64")
    if x.shape != (c["sequenceBars"], len(c["names"])) or not np.isfinite(x).all():
        raise ValueError("RDT feature window is nonfinite or has wrong shape")
    asof = int(common[-1].value) + period * 1_000_000_000
    if asof > time.time_ns():
        raise ValueError("RDT market bar is not yet complete")
    return {"domain": c["domain"], "instrument": c["instrument"], "featureNames": c["names"],
        "inputs": x.tolist(), "asOfNs": asof}


def make_rdt_market_actor(manifest, runner_root, contract_key, *, loop=None, poll_seconds=20, fetcher=fetch_bar_history):
    """One explicit domain/instrument per Actor; no all-domain auto-execution."""
    if manifest["runnerId"] != "rdt4quant_v1" or contract_key not in {"crypto:BTC-USDT", "token_hour:XNVDA"}:
        raise ValueError("Unsupported continuous RDT contract")
    if contract_key not in manifest["domainContracts"]:
        raise ValueError("Selected RDT contract was not published")
    c = manifest["domainContracts"][contract_key]
    crypto = contract_key == "crypto:BTC-USDT"
    names, period, bar = (MARKETS, 60, "1m") if crypto else (("XNVDA-USDT",), 3600, "1H")
    minimum = (480 if crypto else 64) + c["sequenceBars"]
    module = rdt_feature_module(manifest, runner_root, contract_key)
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import ActorConfig
    loop = loop or asyncio.get_running_loop()

    class ClosedBarRdtActor(Actor):
        def __init__(self):
            super().__init__(ActorConfig())
            self.task, self.accepting, self.last_asof_ns = None, False, 0
            self.last_error, self.fetch_errors, self.window_ready = None, 0, False
            self.market_frames = {}

        def on_start(self):
            self.accepting = True
            self.task = loop.create_task(self.poll())

        async def poll(self):
            import pandas as pd
            while self.accepting:
                try:
                    count = minimum + 3 if not self.market_frames else 3
                    frames = await asyncio.gather(*[asyncio.to_thread(fetcher, name, bar, period, count) for name in names])
                    for name, frame in zip(names, frames):
                        if name in self.market_frames:
                            frame = pd.concat([self.market_frames[name], frame])
                            frame = frame.loc[~frame.index.duplicated(keep="last")].sort_index()
                        self.market_frames[name] = frame.iloc[-minimum - 3:]
                    window = await asyncio.to_thread(rdt_window_from_markets, manifest, self.market_frames, module, contract_key)
                    age_ms = (time.time_ns() - window["asOfNs"]) / 1e6
                    # Contract cadence caps freshness even if a multi-domain
                    # release's broad worker limit also accommodates daily data.
                    if age_ms > min(manifest["runtime"]["maxInputAgeMs"], 2 * period * 1000):
                        raise ValueError("RDT completed input is stale for its selected cadence")
                    if self.accepting and window["asOfNs"] > self.last_asof_ns:
                        self.msgbus.publish(topic=f"model.features.{manifest['releaseId']}", msg=window)
                        self.last_asof_ns = window["asOfNs"]
                    self.window_ready, self.last_error = True, None
                except asyncio.CancelledError:
                    return
                except Exception as failure:
                    self.fetch_errors += 1
                    self.window_ready, self.last_error = False, str(failure)
                    # Gaps after a disconnected interval require a bounded
                    # rebootstrap rather than fabricating missing bars.
                    self.market_frames = {}
                await asyncio.sleep(poll_seconds)

        def on_stop(self):
            self.accepting = False
            if self.task:
                self.task.cancel()

        async def quiesce(self):
            self.on_stop()
            if self.task:
                await asyncio.gather(self.task, return_exceptions=True)

        def model_market_snapshot(self):
            return {"releaseId": manifest["releaseId"], "contractKey": contract_key,
                "marketSource": f"OKX public confirmed {bar} candles", "windowReady": self.window_ready,
                "asOfNs": self.last_asof_ns or None, "fetchErrors": self.fetch_errors, "lastError": self.last_error,
                "requiredMarkets": list(names), "volumeField": "volCcyQuote" if crypto else "vol (base currency)",
                "warmupBars": minimum, "sequenceBars": c["sequenceBars"]}

    return ClosedBarRdtActor()


def fetch_completed_candles(instrument):
    if instrument not in MARKETS:
        raise ValueError("Unpublished market input")
    url = "https://www.okx.com/api/v5/market/candles?" + urlencode({"instId": instrument, "bar": "15m", "limit": "200"})
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "Montlok-Model-ClosedBars/1"})
    # Python.org macOS builds may not have a populated system CA path. Use the
    # standard certifi trust bundle when installed, never disable verification.
    try:
        import certifi
        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        context = ssl.create_default_context()
    with urlopen(request, timeout=10, context=context) as response:
        # Reject cross-host redirects; this actor has exactly one public origin.
        from urllib.parse import urlparse
        if urlparse(response.url).hostname != "www.okx.com":
            raise ValueError("Public market request redirected to an unapproved host")
        data = response.read(512001)
    if len(data) > 512000:
        raise ValueError("Oversized candle response")
    return parse_completed_candles(json.loads(data), time.time_ns())


def make_gru_market_actor(manifest, runner_root, *, loop=None, poll_seconds=30, fetcher=fetch_completed_candles):
    if manifest["runnerId"] != "gru_v1" or not 5 <= poll_seconds <= 300:
        raise ValueError("GRU actor or poll interval invalid")
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import ActorConfig
    loop = loop or asyncio.get_running_loop()

    class ClosedBarModelActor(Actor):
        def __init__(self):
            super().__init__(ActorConfig())
            self.task = None
            self.accepting = False
            self.last_asof_ns = 0
            self.last_error = None
            self.fetch_errors = 0
            self.window_ready = False

        def on_start(self):
            self.accepting = True
            self.task = loop.create_task(self.poll())

        async def poll(self):
            while self.accepting:
                try:
                    frames = await asyncio.gather(*[asyncio.to_thread(fetcher, name) for name in MARKETS])
                    window = await asyncio.to_thread(gru_window_from_markets, manifest, dict(zip(MARKETS, frames)), runner_root)
                    if time.time_ns() - window["asOfNs"] > manifest["runtime"]["maxInputAgeMs"] * 1_000_000:
                        raise ValueError("Latest aligned completed bars are stale")
                    if self.accepting and window["asOfNs"] > self.last_asof_ns:
                        self.msgbus.publish(topic=f"model.features.{manifest['releaseId']}", msg=window)
                        self.last_asof_ns = window["asOfNs"]
                    self.window_ready, self.last_error = True, None
                except asyncio.CancelledError:
                    return
                except Exception as failure:
                    self.fetch_errors += 1
                    self.window_ready, self.last_error = False, str(failure)
                await asyncio.sleep(poll_seconds)

        def on_stop(self):
            self.accepting = False
            if self.task:
                self.task.cancel()

        async def quiesce(self):
            self.on_stop()
            if self.task:
                await asyncio.gather(self.task, return_exceptions=True)

        def model_market_snapshot(self):
            return {"releaseId": manifest["releaseId"], "marketSource": "OKX public confirmed 15m candles",
                "windowReady": self.window_ready, "asOfNs": self.last_asof_ns or None,
                "fetchErrors": self.fetch_errors, "lastError": self.last_error,
                "requiredMarkets": list(MARKETS), "volumeField": "volCcyQuote"}

    return ClosedBarModelActor()
