#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""Verify native OKX Demo data, account, and order connections without submitting orders."""

import argparse
import asyncio
import json
import os
import signal
import stat
import threading
from pathlib import Path

from nautilus_trader.core import nautilus_pyo3 as pyo3


def load_credentials(path: Path) -> dict:
    if path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ValueError("Credential file must not be accessible to group or other users")
    values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line)
    fields = {
        "api_key": "OKX_DEMO_API_KEY",
        "api_secret": "OKX_DEMO_API_SECRET",
        "api_passphrase": "OKX_DEMO_API_PASSPHRASE",
    }
    if any(not values.get(name) for name in fields.values()):
        raise ValueError("The Demo API key, secret, and passphrase are all required")
    return {field: values[name] for field, name in fields.items()}


async def preflight(credentials: dict, requested: list[str]) -> tuple[dict, list[str]]:
    client = pyo3.OKXHttpClient(
        **credentials, environment=pyo3.OKXEnvironment.DEMO,
        timeout_secs=15, max_retries=0,
    )
    account = await client.request_account_configuration()
    instruments, _ = await client.request_instruments(pyo3.OKXInstrumentType.SPOT)
    available = {str(instrument.id) for instrument in instruments}
    supported = sorted(set(requested) & available)
    if not supported:
        raise ValueError("None of the requested instruments are available in OKX Demo")
    print("DEMO_PREFLIGHT=" + json.dumps({
        "mode": "demo", "account_level": account["acctLv"],
        "supported": supported, "unsupported": sorted(set(requested) - available),
        "order_submission_enabled": False,
    }), flush=True)
    return account, supported


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=45)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error("--seconds must be between 1 and 300")
    credentials = load_credentials(args.credentials)
    requested = sorted(json.loads(args.signals.read_text())["instruments"])
    account, supported = asyncio.run(preflight(credentials, requested))
    trader_id = pyo3.TraderId("TOKYO-DEMO-001")
    builder = pyo3.live.LiveNode.builder("TOKYO-DEMO", trader_id, pyo3.Environment.SANDBOX)
    builder = builder.with_timeout_connection(30).with_reconciliation(False)
    builder = builder.with_timeout_disconnection_secs(5).with_delay_post_stop_secs(1)
    builder = builder.add_data_client(
        "OKX", pyo3.okx.OKXDataClientFactory(),
        pyo3.okx.OKXDataClientConfig(
            instrument_types=[pyo3.OKXInstrumentType.SPOT],
            environment=pyo3.OKXEnvironment.DEMO,
            api_key="", api_secret="", api_passphrase="",
        ),
    )
    builder = builder.add_exec_client(
        "OKX", pyo3.okx.OKXExecutionClientFactory(),
        pyo3.okx.OKXExecClientConfig(
            trader_id=trader_id, account_id=pyo3.AccountId("OKX-TOKYO-DEMO"),
            **credentials, environment=pyo3.OKXEnvironment.DEMO,
            instrument_types=[pyo3.OKXInstrumentType.SPOT],
            allow_leveraged_products=False, use_spot_margin=False,
            expected_uid=account["uid"], expected_main_uid=account["mainUid"],
            expected_account_type=int(account["type"]), expected_account_level=int(account["acctLv"]),
        ),
    )
    node = builder.build()
    # A data actor can observe quotes but cannot place orders. No strategy is registered.
    node.add_actor_from_config(pyo3.ImportableActorConfig(
        actor_path="okx_stream_probe:QuoteProbe", config_path="okx_stream_probe:ProbeConfig",
        config={"instruments": supported, "execution_clients": 1},
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
