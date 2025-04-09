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
from typing import AsyncContextManager
from contextlib import asynccontextmanager as acm

import tractor

from ..structs import StateHistoryArgs
from .structs import PerformanceOptions
from ._utils import BlockReceiver
from ._context import open_root_context


log = tractor.log.get_logger(__name__)


@acm
async def open_state_history(

    sh_args: StateHistoryArgs

) -> AsyncContextManager[BlockReceiver]:
    '''
    Multi-process state history websocket reader.

    Using `tractor` structured concurrency actor framework for spawning,
    supervision and IPC.

    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    async with open_root_context(sh_args) as ctx:
        for i in range(perf_args.decoders):
            ctx.add_decoder()

        yield BlockReceiver(ctx)

    log.info('root exit')

