# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import os
import stat
from dataclasses import replace

import pytest

from nautilus_trader.agent.inbox import AlphaSignal
from nautilus_trader.agent.inbox import AlphaSignalInbox
from nautilus_trader.agent.inbox import AlphaSignalPolicy
from nautilus_trader.agent.inbox import SignalAction
from nautilus_trader.agent.inbox import SignalStatus


NOW_NS = 1_800_000_000_000_000_000
MODEL_DIGEST = "a" * 64


def _policy(*, capacity: int = 4) -> AlphaSignalPolicy:
    return AlphaSignalPolicy(
        allowed_model_artifacts=frozenset({("model-01", MODEL_DIGEST)}),
        allowed_route_ids=frozenset({"btc-long-only"}),
        allowed_instrument_ids=frozenset({"BTC-USDT.OKX"}),
        max_ttl_ns=30_000_000_000,
        max_pending_signals=capacity,
    )


def _signal(signal_id: str = "signal-01") -> AlphaSignal:
    return AlphaSignal(
        signal_id=signal_id,
        model_id="model-01",
        model_version="2026-08-13",
        model_artifact_digest=MODEL_DIGEST,
        route_id="btc-long-only",
        instrument_id="BTC-USDT.OKX",
        action=SignalAction.LONG,
        confidence=0.75,
        generated_at_ns=NOW_NS,
        expires_at_ns=NOW_NS + 10_000_000_000,
    )


def _private_database(tmp_path):
    os.chmod(tmp_path, stat.S_IRWXU)
    return tmp_path / "alpha.sqlite3"


def test_submit_is_idempotent_but_rejects_signal_id_content_conflict(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy()) as inbox:
        first = inbox.submit(_signal(), now_ns=NOW_NS)
        duplicate = inbox.submit(_signal(), now_ns=NOW_NS)

        assert first == duplicate
        assert first.status == SignalStatus.PENDING
        with pytest.raises(ValueError, match="reused with different content"):
            inbox.submit(replace(_signal(), action=SignalAction.FLAT), now_ns=NOW_NS)


def test_gateway_admission_binds_model_id_to_exact_artifact_digest(tmp_path) -> None:
    path = _private_database(tmp_path)
    with (
        AlphaSignalInbox(path, _policy()) as inbox,
        pytest.raises(PermissionError, match="not admitted together"),
    ):
        inbox.submit(
            replace(_signal(), model_artifact_digest="b" * 64),
            now_ns=NOW_NS,
        )


def test_pending_capacity_is_hard_bounded(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy(capacity=1)) as inbox:
        inbox.submit(_signal("signal-01"), now_ns=NOW_NS)

        with pytest.raises(RuntimeError, match="capacity exhausted"):
            inbox.submit(_signal("signal-02"), now_ns=NOW_NS)


def test_claim_and_terminal_receipt_are_durable(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy()) as inbox:
        inbox.submit(_signal(), now_ns=NOW_NS)
        claimed = inbox.claim_next("btc-long-only", "BTC-USDT.OKX", now_ns=NOW_NS + 1)
        receipt = inbox.terminalize(
            "signal-01",
            SignalStatus.DISPATCHED,
            detail="published internally",
            now_ns=NOW_NS + 2,
        )

        assert claimed == _signal()
        assert receipt.status == SignalStatus.DISPATCHED
        assert receipt.detail == "published internally"
        assert inbox.claim_next("btc-long-only", "BTC-USDT.OKX", now_ns=NOW_NS + 3) is None


def test_unclean_claim_is_abandoned_not_redelivered(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy()) as inbox:
        inbox.submit(_signal(), now_ns=NOW_NS)
        assert inbox.claim_next("btc-long-only", "BTC-USDT.OKX", now_ns=NOW_NS + 1)

    with AlphaSignalInbox(path, _policy(), create=False) as inbox:
        assert inbox.abandon_claimed() == 1
        assert inbox.receipt("signal-01").status == SignalStatus.ABANDONED
        assert inbox.claim_next("btc-long-only", "BTC-USDT.OKX", now_ns=NOW_NS + 2) is None


@pytest.mark.skipif(os.name != "posix", reason="exclusive consumer lease requires Unix")
def test_only_one_process_consumer_can_hold_the_inbox(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy()) as inbox:
        first = inbox.acquire_consumer_lease()
        with pytest.raises(RuntimeError, match="another alpha signal consumer"):
            inbox.acquire_consumer_lease()
        first.close()
        inbox.acquire_consumer_lease().close()


def test_consumer_revalidates_persisted_signal_against_its_own_policy(tmp_path) -> None:
    path = _private_database(tmp_path)
    with AlphaSignalInbox(path, _policy()) as inbox:
        inbox.submit(_signal(), now_ns=NOW_NS)

    different_policy = replace(
        _policy(),
        allowed_model_artifacts=frozenset({("model-02", "b" * 64)}),
    )
    with AlphaSignalInbox(path, different_policy, create=False) as inbox:
        assert inbox.claim_next("btc-long-only", "BTC-USDT.OKX", now_ns=NOW_NS + 1) is None
        assert inbox.receipt("signal-01").status == SignalStatus.REJECTED


def test_inbox_rejects_non_private_parent_directory(tmp_path) -> None:
    os.chmod(tmp_path, stat.S_IRWXU | stat.S_IRGRP)

    with pytest.raises(PermissionError, match="group/world"):
        AlphaSignalInbox(tmp_path / "alpha.sqlite3", _policy())
