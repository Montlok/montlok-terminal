# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""
Minimal MCP stdio gateway for durable alpha-signal admission.

The gateway intentionally exposes no raw order, leverage, withdrawal, risk-limit mutation or
Halted-state reset tool. It does not import an MCP framework so the boundary adds no network
listener and no dependency to the trading runtime.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final


if __package__ in (None, ""):  # Allow an isolated source-tree protocol test without a built wheel.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from inbox import AlphaSignal  # type: ignore[import-not-found]
    from inbox import AlphaSignalInbox  # type: ignore[import-not-found]
    from inbox import AlphaSignalPolicy  # type: ignore[import-not-found]
    from inbox import SignalAction  # type: ignore[import-not-found]
else:
    from nautilus_trader.agent.inbox import AlphaSignal
    from nautilus_trader.agent.inbox import AlphaSignalInbox
    from nautilus_trader.agent.inbox import AlphaSignalPolicy
    from nautilus_trader.agent.inbox import SignalAction


SERVER_NAME: Final[str] = "nautilus-alpha-gateway"
SERVER_VERSION: Final[str] = "1.0.0"
SUPPORTED_PROTOCOLS: Final[tuple[str, ...]] = (
    "2025-11-25",
    "2025-06-18",
)
MAX_MESSAGE_BYTES: Final[int] = 1_048_576


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """Environment-owned stdio gateway configuration."""

    inbox_path: Path
    model_id: str
    model_version: str
    model_artifact_digest: str
    policy: AlphaSignalPolicy

    @classmethod
    def from_environment(cls) -> GatewayConfig:
        inbox = os.environ.get("NAUTILUS_AGENT_INBOX")
        if inbox is None:
            raise ValueError("NAUTILUS_AGENT_INBOX is required")
        max_ttl_ms = _environment_int("NAUTILUS_AGENT_MAX_TTL_MS", 30_000)
        max_pending = _environment_int("NAUTILUS_AGENT_MAX_PENDING", 1_024)
        model_id = _required_environment("NAUTILUS_AGENT_MODEL_ID")
        model_version = _required_environment("NAUTILUS_AGENT_MODEL_VERSION")
        model_artifact_digest = _required_environment(
            "NAUTILUS_AGENT_MODEL_ARTIFACT_DIGEST",
        )
        return cls(
            inbox_path=Path(inbox),
            model_id=model_id,
            model_version=model_version,
            model_artifact_digest=model_artifact_digest,
            policy=AlphaSignalPolicy(
                allowed_model_artifacts=frozenset({(model_id, model_artifact_digest)}),
                allowed_route_ids=frozenset(_environment_csv("NAUTILUS_AGENT_ROUTE_IDS")),
                allowed_instrument_ids=frozenset(
                    _environment_csv("NAUTILUS_AGENT_INSTRUMENT_IDS"),
                ),
                max_ttl_ns=max_ttl_ms * 1_000_000,
                max_pending_signals=max_pending,
            ),
        )


class StdioMcpServer:
    """Synchronous newline-delimited JSON-RPC MCP server."""

    def __init__(
        self,
        inbox: AlphaSignalInbox,
        model_id: str,
        model_version: str,
        model_artifact_digest: str,
    ) -> None:
        self._inbox = inbox
        self._model_id = model_id
        self._model_version = model_version
        self._model_artifact_digest = model_artifact_digest
        self._negotiated_protocol: str | None = None
        self._client_initialized = False

    def run(self) -> None:
        while line := sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1):
            if len(line) > MAX_MESSAGE_BYTES:
                self._write(_error(None, -32600, "MCP message exceeds 1 MiB"))
                return
            try:
                request = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._write(_error(None, -32700, "Invalid JSON"))
                continue
            response = self.handle(request)
            if response is not None:
                self._write(response)

    def handle(self, request: object) -> dict[str, object] | None:
        if not isinstance(request, dict) or request.get("jsonrpc") != "2.0":
            return _error(None, -32600, "Invalid JSON-RPC request")
        request_id = request.get("id")
        if request_id is None:
            return self._handle_notification(request)
        if not isinstance(request_id, (str, int)) or isinstance(request_id, bool):
            return _error(None, -32600, "Request id must be a string or integer")
        method = request.get("method")
        if not isinstance(method, str):
            return _error(request_id, -32600, "Request method must be a string")
        params = request.get("params", {})
        if not isinstance(params, dict):
            return _error(request_id, -32602, "Request params must be an object")

        if method == "initialize":
            return self._initialize(request_id, params)
        if self._negotiated_protocol is None or not self._client_initialized:
            return _error(request_id, -32002, "Server is not initialized")
        return self._handle_initialized(request_id, method, params)

    def _handle_initialized(
        self,
        request_id: str | int,
        method: str,
        params: dict[str, object],
    ) -> dict[str, object]:
        if method == "ping":
            return _result(request_id, {})
        if method == "tools/list":
            return _result(request_id, {"tools": _tools()})
        if method == "tools/call":
            return self._call_tool(request_id, params)
        return _error(request_id, -32601, f"Unknown method '{method}'")

    def _handle_notification(self, request: dict[str, object]) -> None:
        method = request.get("method")
        if method == "notifications/initialized" and self._negotiated_protocol is not None:
            self._client_initialized = True
            return None
        if method == "notifications/cancelled":
            return None
        return None

    def _initialize(self, request_id: str | int, params: dict[str, object]) -> dict[str, object]:
        requested = params.get("protocolVersion")
        if not isinstance(requested, str) or requested not in SUPPORTED_PROTOCOLS:
            return _error(
                request_id,
                -32602,
                "Unsupported protocol version",
                {"supported": list(SUPPORTED_PROTOCOLS)},
            )
        self._negotiated_protocol = requested
        return _result(
            request_id,
            {
                "protocolVersion": requested,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                "instructions": (
                    "This server accepts short-lived alpha signals only. It cannot place raw "
                    "orders, enable leverage, change risk limits or re-arm a halted trader."
                ),
            },
        )

    def _call_tool(self, request_id: str | int, params: dict[str, object]) -> dict[str, object]:
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, dict):
            return _error(request_id, -32602, "Tool name and arguments are required")
        try:
            if name == "publish_alpha_signal":
                output = self._publish(arguments)
            elif name == "get_alpha_signal_receipt":
                output = self._receipt(arguments)
            elif name == "get_alpha_gateway_policy":
                output = self._policy(arguments)
            else:
                return _error(request_id, -32602, f"Unknown tool '{name}'")
        except (KeyError, TypeError, ValueError, PermissionError, RuntimeError) as error:
            return _result(request_id, _tool_result({"accepted": False, "error": str(error)}, True))
        return _result(request_id, _tool_result(output, False))

    def _publish(self, arguments: dict[str, object]) -> dict[str, object]:
        expected = {
            "signal_id",
            "route_id",
            "instrument_id",
            "action",
            "confidence",
            "generated_at_ns",
            "expires_at_ns",
            "rationale_digest",
        }
        extra = set(arguments) - expected
        if extra:
            raise ValueError(f"unknown alpha signal fields: {sorted(extra)}")
        signal = AlphaSignal(
            signal_id=_string(arguments, "signal_id"),
            model_id=self._model_id,
            model_version=self._model_version,
            model_artifact_digest=self._model_artifact_digest,
            route_id=_string(arguments, "route_id"),
            instrument_id=_string(arguments, "instrument_id"),
            action=SignalAction(_string(arguments, "action")),
            confidence=_number(arguments, "confidence"),
            generated_at_ns=_integer(arguments, "generated_at_ns"),
            expires_at_ns=_integer(arguments, "expires_at_ns"),
            rationale_digest=_optional_string(arguments, "rationale_digest"),
        )
        receipt = self._inbox.submit(signal)
        return {"accepted": True, "receipt": receipt.to_dict()}

    def _receipt(self, arguments: dict[str, object]) -> dict[str, object]:
        if set(arguments) != {"signal_id"}:
            raise ValueError("get_alpha_signal_receipt requires only signal_id")
        receipt = self._inbox.receipt(_string(arguments, "signal_id"))
        return {
            "found": receipt is not None,
            "receipt": None if receipt is None else receipt.to_dict(),
        }

    def _policy(self, arguments: dict[str, object]) -> dict[str, object]:
        if arguments:
            raise ValueError("get_alpha_gateway_policy does not accept arguments")
        policy = self._inbox.policy
        return {
            "allowed_model_artifacts": [
                {"model_id": model_id, "artifact_digest": digest}
                for model_id, digest in sorted(policy.allowed_model_artifacts)
            ],
            "allowed_route_ids": sorted(policy.allowed_route_ids),
            "allowed_instrument_ids": sorted(policy.allowed_instrument_ids),
            "max_ttl_ns": policy.max_ttl_ns,
            "max_pending_signals": policy.max_pending_signals,
            "actions": [action.value for action in SignalAction],
            "raw_orders_exposed": False,
            "risk_mutation_exposed": False,
            "leverage_exposed": False,
        }

    @staticmethod
    def _write(response: dict[str, object]) -> None:
        encoded = json.dumps(response, separators=(",", ":"), ensure_ascii=False).encode()
        sys.stdout.buffer.write(encoded + b"\n")
        sys.stdout.buffer.flush()


def _tools() -> list[dict[str, object]]:
    return [
        {
            "name": "publish_alpha_signal",
            "title": "Publish Alpha Signal",
            "description": (
                "Admit a short-lived long-only target signal. This is not an order API; the "
                "Nautilus strategy and RiskEngine retain execution authority."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "signal_id": {"type": "string", "minLength": 1, "maxLength": 255},
                    "route_id": {"type": "string", "minLength": 1, "maxLength": 255},
                    "instrument_id": {"type": "string", "minLength": 1, "maxLength": 255},
                    "action": {"type": "string", "enum": ["LONG", "FLAT", "HOLD"]},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "generated_at_ns": {"type": "integer", "minimum": 1},
                    "expires_at_ns": {"type": "integer", "minimum": 1},
                    "rationale_digest": {
                        "type": ["string", "null"],
                        "pattern": "^[0-9a-f]{64}$",
                    },
                },
                "required": [
                    "signal_id",
                    "route_id",
                    "instrument_id",
                    "action",
                    "confidence",
                    "generated_at_ns",
                    "expires_at_ns",
                ],
            },
            "annotations": {
                "readOnlyHint": False,
                "destructiveHint": False,
                "idempotentHint": True,
            },
        },
        {
            "name": "get_alpha_signal_receipt",
            "title": "Get Alpha Signal Receipt",
            "description": "Read the durable admission/dispatch status for one signal id.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"signal_id": {"type": "string", "minLength": 1, "maxLength": 255}},
                "required": ["signal_id"],
            },
            "annotations": {"readOnlyHint": True, "idempotentHint": True},
        },
        {
            "name": "get_alpha_gateway_policy",
            "title": "Get Alpha Gateway Policy",
            "description": "Show the host-owned model, route, instrument and TTL allowlist.",
            "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}},
            "annotations": {"readOnlyHint": True, "idempotentHint": True},
        },
    ]


def _tool_result(output: dict[str, object], is_error: bool) -> dict[str, object]:
    text = json.dumps(output, sort_keys=True, separators=(",", ":"))
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": output,
        "isError": is_error,
    }


def _result(request_id: str | int, result: dict[str, object]) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(
    request_id: str | int | None,
    code: int,
    message: str,
    data: object | None = None,
) -> dict[str, object]:
    error: dict[str, object] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _environment_csv(name: str) -> list[str]:
    value = os.environ.get(name)
    if value is None:
        raise ValueError(f"{name} is required")
    result = [item.strip() for item in value.split(",") if item.strip()]
    if not result:
        raise ValueError(f"{name} must not be empty")
    return result


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _environment_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return default if value is None else int(value)


def _string(arguments: dict[str, object], name: str) -> str:
    value = arguments[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(arguments: dict[str, object], name: str) -> str | None:
    value = arguments.get(name)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    return value


def _integer(arguments: dict[str, object], name: str) -> int:
    value = arguments[name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(arguments: dict[str, object], name: str) -> float:
    value = arguments[name]
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number")
    return float(value)


def main() -> int:
    try:
        config = GatewayConfig.from_environment()
        with AlphaSignalInbox(config.inbox_path, config.policy) as inbox:
            StdioMcpServer(
                inbox,
                config.model_id,
                config.model_version,
                config.model_artifact_digest,
            ).run()
    except Exception as error:  # The stdio protocol must never be polluted with diagnostics.
        print(f"{SERVER_NAME}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
