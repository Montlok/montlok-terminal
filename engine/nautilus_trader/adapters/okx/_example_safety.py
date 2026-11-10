# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Fail-safe launch validation shared by checked-in OKX execution examples."""

from __future__ import annotations

from dataclasses import dataclass


LIVE_ORDERS_CONFIRMATION_PREFIX = "OKX-LIVE-ORDERS"


@dataclass(frozen=True)
class OKXExampleLaunchPolicy:
    """Validated connectivity and order policy for an OKX example."""

    environment: str
    run: bool
    orders_enabled: bool


def validate_okx_example_launch(
    *,
    environment: str,
    run: bool,
    live_orders: bool,
    configured_account_id: str,
    expected_account_id: str | None,
    live_confirmation: str | None,
) -> OKXExampleLaunchPolicy:
    """Validate an example launch without reading credentials or touching the network."""
    if environment not in {"demo", "live"}:
        raise ValueError(f"Unsupported OKX environment {environment!r}")
    if live_orders and not run:
        raise ValueError("Order submission requires the independent --run connection gate")

    if environment == "live" and live_orders:
        if not expected_account_id:
            raise ValueError("Live orders require --expected-account-id")
        if expected_account_id != configured_account_id:
            raise ValueError(
                "Expected account ID does not match the configured --account-id: "
                f"{expected_account_id!r} != {configured_account_id!r}",
            )
        required = f"{LIVE_ORDERS_CONFIRMATION_PREFIX}:{expected_account_id}"
        if live_confirmation != required:
            raise ValueError(f"Live order confirmation must exactly match {required!r}")

    return OKXExampleLaunchPolicy(
        environment=environment,
        run=run,
        orders_enabled=live_orders,
    )
