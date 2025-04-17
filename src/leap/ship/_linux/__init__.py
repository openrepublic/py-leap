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
import os
from typing import (
    Callable,
    AsyncContextManager
)
from contextlib import asynccontextmanager as acm

import tractor

from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_receiver,
)
from ..structs import StateHistoryArgs
from .structs import (
    PerformanceOptions,
)
from ._ws import ship_supervisor
from ._joiner import block_joiner
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

    pid = os.getpid()

    async with (
        tractor.open_nursery(
            loglevel=perf_args.loglevels.root,
            debug_mode=perf_args.debug_mode,
            enable_modules=[
                'tractor.linux._fdshare',
                __name__
            ]
        ) as an,

        open_ringbuf(
            f'final-blocks-for-{pid}',
            buf_size=perf_args.buf_size
        ) as joiner_token
    ):
        joiner_portal = await an.start_actor(
            'block_joiner',
            loglevel=perf_args.loglevels.joiner,
            enable_modules=[
                'leap.ship._linux._joiner',
                'tractor.ipc._ringbuf._pubsub'
            ],
        )

        ship_portal = await an.start_actor(
            'ship_supervisor',
            loglevel=perf_args.loglevels.ship_supervisor,
            enable_modules=[
                'leap.ship._linux._ws',
            ],
        )

        async with (
            joiner_portal.open_context(
                block_joiner,
                sh_args=sh_args,
                out_token=joiner_token
            ) as (joiner_ctx, _),

            ship_portal.open_context(
                ship_supervisor,
                sh_args=sh_args,
            ) as (ship_ctx, _),

            attach_to_ringbuf_receiver(joiner_token) as block_stream
        ):
            yield BlockReceiver(
                block_stream,
                sh_args,
                log_fn=benchmark_log_fn
            )

            await ship_ctx.cancel()

        await joiner_portal.cancel_actor()
        await ship_portal.cancel_actor()
