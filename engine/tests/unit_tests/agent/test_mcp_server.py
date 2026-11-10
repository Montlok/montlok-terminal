# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path


MODEL_DIGEST = "a" * 64


def test_stdio_mcp_protocol_exposes_signal_boundary_only(tmp_path) -> None:
    os.chmod(tmp_path, stat.S_IRWXU)
    server_path = (
        Path(__file__).parents[3] / "nautilus_trader" / "agent" / "mcp_server.py"
    ).resolve()
    now_ns = time.time_ns()
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "publish_alpha_signal",
                "arguments": {
                    "signal_id": "signal-stdio-01",
                    "route_id": "btc-long-only",
                    "instrument_id": "BTC-USDT.OKX",
                    "action": "LONG",
                    "confidence": 0.75,
                    "generated_at_ns": now_ns,
                    "expires_at_ns": now_ns + 10_000_000_000,
                },
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "get_alpha_signal_receipt",
                "arguments": {"signal_id": "signal-stdio-01"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "publish_alpha_signal",
                "arguments": {
                    "signal_id": "signal-spoof-01",
                    "model_id": "impostor",
                    "route_id": "btc-long-only",
                    "instrument_id": "BTC-USDT.OKX",
                    "action": "LONG",
                    "confidence": 0.75,
                    "generated_at_ns": now_ns,
                    "expires_at_ns": now_ns + 10_000_000_000,
                },
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {"name": "place_order", "arguments": {}},
        },
    ]
    input_bytes = b"".join(
        json.dumps(request, separators=(",", ":")).encode() + b"\n" for request in requests
    )
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "NAUTILUS_AGENT_INBOX": str(tmp_path / "alpha.sqlite3"),
        "NAUTILUS_AGENT_MODEL_ID": "model-01",
        "NAUTILUS_AGENT_MODEL_VERSION": "2026-08-13",
        "NAUTILUS_AGENT_MODEL_ARTIFACT_DIGEST": MODEL_DIGEST,
        "NAUTILUS_AGENT_ROUTE_IDS": "btc-long-only",
        "NAUTILUS_AGENT_INSTRUMENT_IDS": "BTC-USDT.OKX",
    }

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and repository script
        [sys.executable, str(server_path)],
        input=input_bytes,
        capture_output=True,
        check=False,
        env=environment,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr.decode()
    responses = [json.loads(line) for line in completed.stdout.splitlines()]
    assert [response["id"] for response in responses] == [1, 2, 3, 4, 5, 6]
    tools = {tool["name"] for tool in responses[1]["result"]["tools"]}
    assert tools == {
        "get_alpha_gateway_policy",
        "get_alpha_signal_receipt",
        "publish_alpha_signal",
    }
    publish_tool = next(
        tool for tool in responses[1]["result"]["tools"] if tool["name"] == "publish_alpha_signal"
    )
    assert "model_id" not in publish_tool["inputSchema"]["properties"]
    assert "model_artifact_digest" not in publish_tool["inputSchema"]["properties"]
    assert responses[2]["result"]["structuredContent"]["accepted"] is True
    assert responses[3]["result"]["structuredContent"]["receipt"]["status"] == "PENDING"
    assert responses[4]["result"]["isError"] is True
    assert "unknown alpha signal fields" in responses[4]["result"]["structuredContent"]["error"]
    assert "Unknown tool" in responses[5]["error"]["message"]
    assert completed.stderr == b""
    with sqlite3.connect(tmp_path / "alpha.sqlite3") as connection:
        payload = json.loads(
            connection.execute(
                "SELECT payload_json FROM alpha_signals WHERE signal_id = ?",
                ("signal-stdio-01",),
            ).fetchone()[0],
        )
        signal_count = connection.execute("SELECT COUNT(*) FROM alpha_signals").fetchone()[0]
    assert payload["model_id"] == "model-01"
    assert payload["model_version"] == "2026-08-13"
    assert payload["model_artifact_digest"] == MODEL_DIGEST
    assert signal_count == 1
