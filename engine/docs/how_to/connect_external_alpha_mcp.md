# Connect an external alpha model through MCP

The alpha gateway is a deliberately narrow boundary between an external model and NautilusTrader.
It accepts short-lived target signals, persists an idempotent receipt, and lets a Nautilus actor
publish the signal onto an internal MessageBus topic.

It is **not** an order API. The MCP server cannot place or cancel orders, change leverage or risk
limits, or re-arm a halted trader. A reviewed Nautilus strategy remains responsible for position
sizing and translating an admitted signal into ordinary Nautilus commands. The normal execution
and RiskEngine paths remain authoritative.

## Boundary

```text
external model
    -> MCP stdio: publish_alpha_signal
    -> private, bounded SQLite inbox
    -> AlphaSignalBridge actor
    -> internal topic agent.alpha.<route_id>
    -> reviewed Nautilus strategy
    -> RiskEngine -> execution client -> venue
```

The first schema is intentionally long-only: `LONG`, `FLAT`, and `HOLD`. A signal includes an exact
model ID plus SHA-256 model-artifact digest, route and instrument allowlists, a confidence value,
and a host-bounded expiry. Reusing a signal ID with different content is rejected.

`DISPATCHED` means the bridge published the signal to the internal MessageBus. It does **not** mean
an order was accepted or filled.

## 1. Create a private inbox directory

The directory must be absolute, owned by the trading user, and inaccessible to group and world:

```bash
install -d -m 700 /var/lib/nautilus/agent-alpha
```

Use a real SHA-256 digest of the approved model artifact. Do not use an API key or a model name as
the digest.

## 2. Configure the MCP host

Configure the Agent's MCP client to spawn this server over stdio:

```json
{
  "mcpServers": {
    "nautilus-alpha": {
      "command": "/absolute/path/to/python",
      "args": ["-m", "nautilus_trader.agent.mcp_server"],
      "env": {
        "NAUTILUS_AGENT_INBOX": "/var/lib/nautilus/agent-alpha/signals.sqlite3",
        "NAUTILUS_AGENT_MODEL_ID": "btc-model-v1",
        "NAUTILUS_AGENT_MODEL_VERSION": "2026-08-13",
        "NAUTILUS_AGENT_MODEL_ARTIFACT_DIGEST": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "NAUTILUS_AGENT_ROUTE_IDS": "btc-long-only",
        "NAUTILUS_AGENT_INSTRUMENT_IDS": "BTC-USDT.OKX",
        "NAUTILUS_AGENT_MAX_TTL_MS": "30000",
        "NAUTILUS_AGENT_MAX_PENDING": "1024"
      }
    }
  }
}
```

The gateway exposes only:

- `publish_alpha_signal`
- `get_alpha_signal_receipt`
- `get_alpha_gateway_policy`

It writes only MCP JSON-RPC messages to stdout. Diagnostics go to stderr.
The MCP process stamps model identity and artifact digest from its host-owned environment; the
Agent cannot self-report or override these fields in a tool call. Run a separate MCP process and
inbox route for a different model identity.

## 3. Add the bridge actor to the same Nautilus node as the strategy

Use the same inbox path and a policy at least as strict as the gateway policy:

```python
from nautilus_trader.agent.bridge import AlphaSignalBridge
from nautilus_trader.agent.bridge import AlphaSignalBridgeConfig

bridge = AlphaSignalBridge(
    AlphaSignalBridgeConfig(
        inbox_path="/var/lib/nautilus/agent-alpha/signals.sqlite3",
        route_id="btc-long-only",
        instrument_id="BTC-USDT.OKX",
        allowed_model_artifacts=frozenset(
            {
                "btc-model-v1=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            },
        ),
    ),
)
node.trader.add_actor(bridge)
```

Start the MCP gateway once before starting the node so the inbox exists. The bridge refuses to
create a missing inbox, which prevents a path typo from silently creating a second signal stream.
The inbox also permits only one bridge process to hold the consumer lease.

## 4. Subscribe from a reviewed strategy

The strategy subscribes to one configured route. It should independently enforce the expected
instrument, minimum confidence, allowed position transition, fixed sizing policy, and signal age
before creating a normal Nautilus order:

```python
from nautilus_trader.agent.bridge import alpha_signal_topic
from nautilus_trader.agent.inbox import AlphaSignal


def on_start(self) -> None:
    self.msgbus.subscribe(alpha_signal_topic("btc-long-only"), self.on_alpha_signal)


def on_alpha_signal(self, signal: AlphaSignal) -> None:
    if signal.instrument_id != str(self.config.instrument_id):
        raise RuntimeError("alpha route delivered the wrong instrument")

    # Apply the strategy-owned confidence, state-transition and fixed-size policy here.
    # Submit only ordinary Nautilus orders so the RiskEngine remains on the execution path.
```

Do not deserialize arbitrary Python classes, accept source code/model uploads, or place an order
inside the MCP server. Model deployment and signal admission are separate controls.

## Failure semantics

- Invalid model/digest, route, instrument, TTL, confidence, or schema: rejected before persistence.
- Inbox full: fail closed; the Agent receives an error and no signal is overwritten.
- Expired before claim: terminal `EXPIRED`.
- Bridge crash after claim: terminal `ABANDONED` on restart and never redelivered. This favors no
  duplicate order over automatic retry.
- Internal subscriber failure: terminal `REJECTED`, then the bridge raises so the host can halt.
- `Halted` trader: this gateway has no re-arm tool and cannot bypass the RiskEngine.

Use Demo first. Promotion to real funds still requires a reviewed strategy, configured capital
limits, durable event-store policy, reconciliation, and the operator's existing release gates.
