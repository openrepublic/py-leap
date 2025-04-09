from typing import (
    AsyncContextManager
)
from contextlib import (
    asynccontextmanager as acm
)

import trio
import tractor
from tractor.trionics import gather_contexts
from tractor.ipc._ringbuf._pubsub import (
    RingBufferSubscriber,
    open_ringbuf_subscriber,
)

from ..structs import (
    StateHistoryArgs
)
from .structs import (
    PerformanceOptions,
    BatchSizeOptions,
    PerfTweakMsg,
)


import tractor.ipc._ringbuf._ringd as ringd

from ._ws import ship_reader
from ._decoders import (
    stage_0_decoder,
    stage_1_decoder
)
from ._resmon import resource_monitor

import leap.ship._linux._resmon as resmon


log = tractor.log.get_logger(__name__)


class RootContext:

    def __init__(
        self,
        sh_args: StateHistoryArgs,
        tn: trio.Nursery,
        an: tractor.ActorNursery,
        block_channel: RingBufferSubscriber,
        resmon
    ):
        self.sh_args = StateHistoryArgs.from_dict(sh_args)
        self.tn = tn
        self.an = an
        self.perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)
        self.block_channel = block_channel

        self.resmon = resmon

        self._decoder_teardown: dict[str, trio.Event] = {}

        self._decoders = 0
        self._global_teardown = trio.Event()

        self._next_decoder_index = 0

    def next_decoder_ids(self) -> tuple[str, list[str]]:
        index = self._next_decoder_index
        self._next_decoder_index += 1
        decoder_id = f'decoder-{index}'
        stage_0_id = decoder_id + '-stage-0'
        stage_1_ids = tuple(
            decoder_id + f'-stage-1.{i}'
            for i in range(int(self.perf_args.stage_ratio))
        )
        return stage_0_id, stage_1_ids

    @acm
    async def _open_stage_1_decoder(
        self,
        stage_0_id: str,
        stage_1_id: str
    ) -> AsyncContextManager[None]:

        portal = await self.an.start_actor(
            stage_1_id,
            enable_modules=[
                'leap.ship._linux._decoders',
                'leap.ship._linux._resmon',
            ]
        )

        async with portal.open_context(
            stage_1_decoder,
            stage_0_id=stage_0_id,
            stage_1_id=stage_1_id,
            sh_args=self.sh_args,
        ) as (ctx, _sent):
            yield
            await ctx.cancel()

    @acm
    async def _open_decoder(
        self,
        stage_0_id: str,
        stage_1_ids: list[str]
    ) -> AsyncContextManager[None]:
        '''
        Add a new decoder to pipeline

        '''
        stage_0_portal = await self.an.start_actor(
            stage_0_id,
            enable_modules=[
                'leap.ship._linux._resmon',
                'leap.ship._linux._decoders',
                'tractor.ipc._ringbuf._pubsub',
            ],
        )

        async with (
            # open long running stage 0 decoding ctx
            stage_0_portal.open_context(
                stage_0_decoder,
                stage_0_id=stage_0_id,
                sh_args=self.sh_args,
            ) as (stage_0_ctx, _),

            # open long running stage 1 decoding ctxs
            gather_contexts([
                self._open_stage_1_decoder(
                    stage_0_id,
                    stage_1_id
                )
                for stage_1_id in stage_1_ids
            ]),
        ):
            yield
            await stage_0_ctx.cancel()

    def add_decoder(self):
        stage_0_id, stage_1_ids = self.next_decoder_ids()
        self._decoders += 1

        teardown = trio.Event()
        self._decoder_teardown[stage_0_id] = teardown

        async def _decoder_task():
            async with self._open_decoder(stage_0_id, stage_1_ids):
                await teardown.wait()

            self._decoders -= 1
            if self._decoders == 0:
                self._global_teardown.set()

        self.tn.start_soon(_decoder_task)

    async def teardown_decoders(self):
        for teardown in self._decoder_teardown.values():
            teardown.set()

        await self._global_teardown.wait()

    async def close_control_streams(self):
        await self.ctrl_schan.aclose()
        await self.ctrl_rchan.aclose()

    async def end_is_near(self):
        flush_msg = PerfTweakMsg(
            batch_size_options=BatchSizeOptions(
                new_batch_size=1,
                must_flush=True
            )
        )
        await resmon.broadcast_tweak(flush_msg)
        await self.resmon.cancel()


    async def reached_end(self):
        await self.teardown_decoders()


@acm
async def open_root_context(

    sh_args: StateHistoryArgs

) -> AsyncContextManager[RootContext]:
    global _ctx

    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    async with (
        # open root nursery, enable pubsub to manage the
        # subscriber channels
        tractor.open_nursery(
            loglevel=perf_args.loglevel,
            debug_mode=perf_args.debug_mode,
            enable_modules=[
                'tractor.ipc._ringbuf._pubsub'
            ]
        ) as an,

        ringd.open_ringd(loglevel=perf_args.loglevel),

        open_ringbuf_subscriber(
            buf_size=perf_args.buf_size,
        ) as block_channel
    ):
        res_monitor_portal = await an.start_actor(
            'resource_monitor',
            enable_modules=['leap.ship._linux._resmon']
        )

        ship_portal = await an.start_actor(
            'ship_reader',
            enable_modules=[
                'leap.ship._linux._ws',
                'leap.ship._linux._resmon',
                'tractor.ipc._ringbuf._pubsub'
            ],
        )

        async with (
            res_monitor_portal.open_context(
                resource_monitor,
                sh_args=sh_args,
            ) as (res_ctx, _),

            ship_portal.open_context(
                ship_reader,
                sh_args=sh_args,
            ) as (ship_ctx, _),

            trio.open_nursery() as tn,
        ):
            await resmon.register_actor()
            ctx = RootContext(
                sh_args=sh_args,
                tn=tn,
                an=an,
                resmon=res_ctx,
                block_channel=block_channel
            )
            _ctx = ctx
            yield ctx
            await res_ctx.cancel()
            await ship_ctx.cancel()

        # TODO: wtf why hard_kill needed for resource_monitor?
        await an.cancel(hard_kill=True)
