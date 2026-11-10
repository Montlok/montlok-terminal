#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""Measure public OKX quotes through the native live runner, without an execution client."""

import argparse
import json
import os
import signal
import threading
import time
from collections import Counter
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from nautilus_trader.core import nautilus_pyo3 as pyo3


@dataclass
class ProbeConfig:
    instruments: list[str]
    execution_clients: int = 0


class QuoteProbe(pyo3.DataActor):
    def __init__(self, config: ProbeConfig) -> None:
        super().__init__()
        self.instruments = [pyo3.InstrumentId.from_str(value) for value in config.instruments]
        self.execution_clients = config.execution_clients
        self.counts = Counter()
        self.arrival_ms = deque(maxlen=100_000)
        self.dispatch_ms = deque(maxlen=100_000)

    def on_start(self) -> None:
        for instrument_id in self.instruments:
            self.subscribe_quotes(instrument_id, client_id=pyo3.ClientId("OKX"))

    def on_quote(self, quote) -> None:
        received_ns = time.time_ns()
        self.counts[str(quote.instrument_id)] += 1
        self.arrival_ms.append((received_ns - quote.ts_event) / 1_000_000)
        self.dispatch_ms.append((received_ns - quote.ts_init) / 1_000_000)

    def on_stop(self) -> None:
        print("QUOTE_PROBE=" + json.dumps(self.report()), flush=True)

    def report(self) -> dict:
        def percentiles(values):
            ordered = sorted(values)
            return {
                label: round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))], 3)
                if ordered else None
                for label, fraction in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))
            }

        return {
            "execution_clients": self.execution_clients,
            "trading_strategies": 0,
            "transport": "native_okx_websocket",
            "requested": len(self.instruments),
            "seen": len(self.counts),
            "quotes": sum(self.counts.values()),
            "counts": dict(self.counts),
            "exchange_to_callback_ms": percentiles(self.arrival_ms),
            "adapter_to_callback_ms": percentiles(self.dispatch_ms),
            "latency_sample_limit": self.arrival_ms.maxlen,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=45)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error("--seconds must be between 1 and 300")
    instruments = sorted(json.loads(args.signals.read_text())["instruments"])
    builder = pyo3.live.LiveNode.builder(
        "OKX-QUOTE-PROBE", pyo3.TraderId("QUOTE-PROBE-001"), pyo3.Environment.LIVE,
    )
    builder = builder.with_timeout_connection(30).with_reconciliation(False)
    builder = builder.with_timeout_disconnection_secs(5).with_delay_post_stop_secs(1)
    builder = builder.add_data_client(
        "OKX", pyo3.okx.OKXDataClientFactory(),
        pyo3.okx.OKXDataClientConfig(
            instrument_types=[pyo3.OKXInstrumentType.SPOT],
            environment=pyo3.OKXEnvironment.LIVE,
            api_key="", api_secret="", api_passphrase="",
        ),
    )
    node = builder.build()
    node.add_actor_from_config(pyo3.ImportableActorConfig(
        actor_path="__main__:QuoteProbe", config_path="__main__:ProbeConfig",
        config={"instruments": instruments},
    ))
    timer = threading.Timer(args.seconds, os.kill, args=(os.getpid(), signal.SIGINT))
    timer.daemon = True
    timer.start()
    try:
        node.run()
    finally:
        timer.cancel()


if __name__ == "__main__":
    main()
