# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""Nautilus actor which republishes admitted external alpha signals internally."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from nautilus_trader.agent.inbox import AlphaSignalInbox
from nautilus_trader.agent.inbox import AlphaSignalPolicy
from nautilus_trader.agent.inbox import InboxConsumerLease
from nautilus_trader.agent.inbox import SignalStatus
from nautilus_trader.common.actor import Actor
from nautilus_trader.common.config import ActorConfig
from nautilus_trader.common.config import PositiveInt


class AlphaSignalBridgeConfig(ActorConfig, frozen=True, kw_only=True):
    """
    Configuration for the isolated external-alpha bridge.

    `allowed_model_artifacts` entries use the exact `MODEL_ID=SHA256` form. The bridge repeats
    gateway admission so a permissive or misconfigured gateway cannot weaken the trading host.
    """

    inbox_path: str
    route_id: str
    instrument_id: str
    allowed_model_artifacts: frozenset[str]
    max_ttl_ms: PositiveInt = 30_000
    max_pending_signals: PositiveInt = 1_024
    poll_interval_ms: PositiveInt = 100
    max_batch_per_poll: PositiveInt = 32


class AlphaSignalBridge(Actor):
    """
    Deliver admitted alpha signals onto a non-external internal message bus topic.

    This actor has no order factory and cannot place orders. A separately reviewed strategy must
    subscribe to :func:`alpha_signal_topic` and translate signals into normal Nautilus commands.
    """

    _TIMER_NAME = "ALPHA_SIGNAL_BRIDGE_POLL"

    def __init__(self, config: AlphaSignalBridgeConfig) -> None:
        super().__init__(config)
        if config.max_ttl_ms > 300_000:
            raise ValueError("max_ttl_ms must not exceed 300000")
        if config.max_batch_per_poll > config.max_pending_signals:
            raise ValueError("max_batch_per_poll must not exceed max_pending_signals")
        self._policy = AlphaSignalPolicy(
            allowed_model_artifacts=frozenset(
                _parse_model_artifact_binding(binding) for binding in config.allowed_model_artifacts
            ),
            allowed_route_ids=frozenset({config.route_id}),
            allowed_instrument_ids=frozenset({config.instrument_id}),
            max_ttl_ns=config.max_ttl_ms * 1_000_000,
            max_pending_signals=config.max_pending_signals,
        )
        self._policy.validate()
        self._inbox: AlphaSignalInbox | None = None
        self._consumer_lease: InboxConsumerLease | None = None

    def on_start(self) -> None:
        inbox = AlphaSignalInbox(Path(self.config.inbox_path), self._policy, create=False)
        lease: InboxConsumerLease | None = None
        try:
            lease = inbox.acquire_consumer_lease()
            self._consumer_lease = lease
            self._inbox = inbox
            abandoned = inbox.abandon_claimed()
            if abandoned:
                self.log.error(
                    f"Abandoned {abandoned} alpha signal claim(s) after an unclean consumer stop",
                )
            self.clock.set_timer(
                name=self._TIMER_NAME,
                interval=timedelta(milliseconds=self.config.poll_interval_ms),
                callback=self._on_poll,
            )
        except BaseException:
            self._inbox = None
            self._consumer_lease = None
            inbox.close()
            if lease is not None:
                lease.close()
            raise

    def on_stop(self) -> None:
        if self._inbox is not None:
            self._inbox.close()
            self._inbox = None
        if self._consumer_lease is not None:
            self._consumer_lease.close()
            self._consumer_lease = None

    def _on_poll(self, _: object) -> None:
        inbox = self._inbox
        if inbox is None:
            return
        for _ in range(self.config.max_batch_per_poll):
            signal = inbox.claim_next(
                self.config.route_id,
                self.config.instrument_id,
                now_ns=self.clock.timestamp_ns(),
            )
            if signal is None:
                return
            try:
                self.msgbus.publish(
                    alpha_signal_topic(signal.route_id),
                    signal,
                    external_pub=False,
                )
            except Exception as error:
                inbox.terminalize(
                    signal.signal_id,
                    SignalStatus.REJECTED,
                    detail=f"internal alpha subscriber failed: {type(error).__name__}",
                    now_ns=self.clock.timestamp_ns(),
                )
                raise
            inbox.terminalize(
                signal.signal_id,
                SignalStatus.DISPATCHED,
                detail="published to internal Nautilus message bus",
                now_ns=self.clock.timestamp_ns(),
            )


def alpha_signal_topic(route_id: str) -> str:
    """Return the internal topic for one configured alpha route."""
    return f"agent.alpha.{route_id}"


def _parse_model_artifact_binding(binding: str) -> tuple[str, str]:
    try:
        model_id, digest = binding.rsplit("=", maxsplit=1)
    except ValueError as error:
        raise ValueError("model artifact bindings must be MODEL_ID=SHA256") from error
    return model_id, digest
