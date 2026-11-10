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

import pytest

from nautilus_trader.adapters.okx._example_safety import OKXExampleLaunchPolicy
from nautilus_trader.adapters.okx._example_safety import validate_okx_example_launch


def validate(
    environment: str = "demo",
    run: bool = False,
    live_orders: bool = False,
    configured_account_id: str = "OKX-001",
    expected_account_id: str | None = None,
    live_confirmation: str | None = None,
) -> OKXExampleLaunchPolicy:
    return validate_okx_example_launch(
        environment=environment,
        run=run,
        live_orders=live_orders,
        configured_account_id=configured_account_id,
        expected_account_id=expected_account_id,
        live_confirmation=live_confirmation,
    )


def test_unset_policy_builds_demo_without_connecting_or_ordering() -> None:
    policy = validate()

    assert policy.environment == "demo"
    assert not policy.run
    assert not policy.orders_enabled


def test_demo_orders_require_explicit_run_and_order_gates() -> None:
    policy = validate(run=True, live_orders=True)

    assert policy.environment == "demo"
    assert policy.run
    assert policy.orders_enabled


def test_orders_without_run_are_rejected() -> None:
    with pytest.raises(ValueError, match="--run"):
        validate(live_orders=True)


def test_live_connection_can_remain_order_disabled() -> None:
    policy = validate(environment="live", run=True)

    assert policy.environment == "live"
    assert policy.run
    assert not policy.orders_enabled


def test_live_orders_without_expected_account_are_rejected() -> None:
    with pytest.raises(ValueError, match="expected-account-id"):
        validate(environment="live", run=True, live_orders=True)


def test_live_orders_for_wrong_account_are_rejected() -> None:
    with pytest.raises(ValueError, match="does not match"):
        validate(
            environment="live",
            run=True,
            live_orders=True,
            expected_account_id="OKX-002",
            live_confirmation="OKX-LIVE-ORDERS:OKX-002",
        )


def test_live_orders_with_mismatched_confirmation_are_rejected() -> None:
    with pytest.raises(ValueError, match="exactly match"):
        validate(
            environment="live",
            run=True,
            live_orders=True,
            expected_account_id="OKX-001",
            live_confirmation="OKX-LIVE-ORDERS:OKX-002",
        )


def test_live_orders_require_all_account_bound_gates() -> None:
    policy = validate(
        environment="live",
        run=True,
        live_orders=True,
        expected_account_id="OKX-001",
        live_confirmation="OKX-LIVE-ORDERS:OKX-001",
    )

    assert policy.environment == "live"
    assert policy.run
    assert policy.orders_enabled
