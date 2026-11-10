# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""Fail-closed boundaries for external model signals."""

from nautilus_trader.agent.inbox import AlphaSignal
from nautilus_trader.agent.inbox import AlphaSignalInbox
from nautilus_trader.agent.inbox import AlphaSignalPolicy
from nautilus_trader.agent.inbox import InboxConsumerLease
from nautilus_trader.agent.inbox import SignalAction
from nautilus_trader.agent.inbox import SignalReceipt
from nautilus_trader.agent.inbox import SignalStatus


__all__ = [
    "AlphaSignal",
    "AlphaSignalInbox",
    "AlphaSignalPolicy",
    "InboxConsumerLease",
    "SignalAction",
    "SignalReceipt",
    "SignalStatus",
]
