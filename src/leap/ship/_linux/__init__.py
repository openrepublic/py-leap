# py-leap: Antelope protocol framework
# Copyright 2021-eternity Guillermo Rodriguez

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from __future__ import annotations
from typing import (
    AsyncContextManager
)
from contextlib import (
    asynccontextmanager as acm
)
import tractor

from ..structs import (
    StateHistoryArgs
)

from ._utils import BlockReceiver
from ._context import (
    open_static_resources,
    open_control_stream_handlers
)
from ._decoders import open_decoder


# log = tractor.log.get_logger(__name__)
log = tractor.log.get_console_log(level='info')


@acm
async def open_state_history(

    sh_args: StateHistoryArgs

) -> AsyncContextManager[BlockReceiver]:
    '''
    Multi-process state history websocket reader.

    Using `tractor` structured concurrency actor framework for spawning,
    supervision and IPC.

    '''
    async with (
        open_static_resources(sh_args) as root_ctx,

        open_control_stream_handlers(root_ctx),

        open_decoder(root_ctx),
    ):
        yield BlockReceiver(root_ctx)

    log.info('root exit')

