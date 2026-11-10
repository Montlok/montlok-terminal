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

from typing import Final

from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


OKX: Final[str] = "OKX"
OKX_VENUE: Final[Venue] = Venue(OKX)
OKX_CLIENT_ID: Final[ClientId] = ClientId(OKX)

# Broker tag applied to every order and to the Cancel All After request, so the venue-side
# timer only cancels orders owned by Nautilus. Mirrors `OKX_NAUTILUS_BROKER_ID` in Rust.
OKX_CANCEL_ALL_AFTER_TAG: Final[str] = "5328c82e5542BCDE"
# Delay between retries of a failed Cancel All After renewal (OKX allows ~1 request/second).
OKX_CANCEL_ALL_AFTER_RETRY_INTERVAL_SECS: Final[float] = 1.0
