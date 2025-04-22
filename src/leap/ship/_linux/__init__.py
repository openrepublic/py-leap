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
from typing import (
    Callable,
    AsyncContextManager
)
from contextlib import asynccontextmanager as acm

import tractor
from tractor.ipc._ringbuf._pubsub import open_ringbuf_subscriber

from leap.protocol import (
    apply_leap_codec,
    limit_leap_plds
)
from leap.ship.structs import (
    StateHistoryArgs,
    block_decoder
)

from .structs import (
    PerformanceOptions,
)
from ._ws import open_ship_supervisor
from ._utils import BlockReceiver


@acm
async def open_state_history(

    sh_args: StateHistoryArgs,
    benchmark_log_fn: Callable | None = None

) -> AsyncContextManager[BlockReceiver]:
    '''
    Multi-process state history websocket reader.

    Using `tractor` structured concurrency actor framework for spawning,
    supervision and IPC.

    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    async with (
        tractor.open_nursery(
            loglevel=perf_args.loglevels.root,
            debug_mode=perf_args.debug_mode,
            enable_modules=[
                'tractor.ipc._ringbuf._pubsub',
            ]
        ) as an,

        open_ship_supervisor(sh_args, an),

        open_ringbuf_subscriber(decoder=block_decoder) as block_stream
    ):

        yield BlockReceiver(
            block_stream,
            sh_args,
            log_fn=benchmark_log_fn
        )
