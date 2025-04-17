from typing import (
    AsyncContextManager
)
from contextlib import (
    asynccontextmanager as acm
)

import tractor

from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_receiver,
)

from ..structs import (
    StateHistoryArgs
)
from .structs import (
    PerformanceOptions,
)
from ._ws import ship_supervisor
from ._joiner import block_joiner
from ._utils import (
    get_logger,
    BlockReceiver
)


log = get_logger(__name__)


@acm
async def open_root_context(

    sh_args: StateHistoryArgs

) -> AsyncContextManager[BlockReceiver]:

    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

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
            'final',
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
            yield BlockReceiver(block_stream, sh_args)

            await ship_ctx.cancel()

        await joiner_portal.cancel_actor()
        await ship_portal.cancel_actor()
