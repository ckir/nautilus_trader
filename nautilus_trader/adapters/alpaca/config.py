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

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.core.nautilus_pyo3 import AlpacaDataFeed
from nautilus_trader.model.identifiers import Venue

ALPACA_VENUE = Venue("ALPACA")

class AlpacaDataClientConfig(LiveDataClientConfig, frozen=True):
    api_key: str
    api_secret: str
    feed: AlpacaDataFeed = AlpacaDataFeed.IEX

class AlpacaExecutionClientConfig(LiveExecClientConfig, frozen=True):
    api_key: str
    api_secret: str
    paper: bool = True
