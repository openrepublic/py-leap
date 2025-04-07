from typing import (
    AsyncContextManager
)
from contextlib import (
    asynccontextmanager as acm
)
from dataclasses import dataclass

import trio
import tractor
from tractor import Context
from tractor.ipc._ringbuf._pubsub import (
    RingBufferSubscriber,
    open_ringbuf_subscriber
)

from ..structs import (
    StateHistoryArgs
)
from .structs import (
    PerformanceOptions,
    EndIsNearMsg,
    ReachedEndMsg,
    BatchSizeOptions,
    PerfTweakMsg,
    InputConnectMsg,
    InputDisconnectMsg,
)


import tractor.ipc._ringbuf._ringd as ringd

from ._ws import ship_reader
from ._resmon import resource_monitor

import leap.ship._linux._resmon as resmon


log = tractor.log.get_logger(__name__)


@dataclass
class ChildContext:
    ctx: Context
    stream: tractor.MsgStream


class RootContext:

    def __init__(
        self,
        sh_args: StateHistoryArgs,
        an: tractor.ActorNursery,
        ringd_portal: tractor.Portal,
        block_channel: RingBufferSubscriber,
        ship: ChildContext,
        resmon: ChildContext,
    ):
        self.sh_args = StateHistoryArgs.from_dict(sh_args)
        self.an = an
        self.perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)
        self.ringd_portal = ringd_portal
        self.block_channel = block_channel
        self.ship = ship
        self.resmon = resmon

        self.ctrl_schan, self.ctrl_rchan = trio.open_memory_channel(0)

        self.stage_0: dict[str, ChildContext] = {}
        self.stage_1: dict[str, ChildContext] = {}

        self._next_decoder_index = 0

        self.decoders_lock = trio.StrictFIFOLock()

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

    def add_stage_0_decoder(
        self,
        ctx: ChildContext,
        id: str
    ):
        self.stage_0[id] = ctx

    def add_stage_1_decoder(
        self,
        ctx: ChildContext,
        id: str
    ):
        self.stage_1[id] = ctx

    def get_ctx(self, actor_name: str) -> Context:
        match actor_name:
            case 'ship_reader':
                return self.ship

            case 'resource_monitor':
                return self.resmon

            case _:
                stage_0_ctx = self.stage_0.get(actor_name, None)
                if stage_0_ctx:
                    return stage_0_ctx

                stage_1_ctx = self.stage_1.get(actor_name, None)
                if stage_1_ctx:
                    return stage_1_ctx

                raise ValueError(f'Unknown actor context {actor_name}')

    async def multi_send(
        self,
        msg: bytes,
        destinations: list[str]
    ):
        for dst in destinations:
            await self.get_ctx(dst).stream.send(msg)

    async def close_control_streams(self):
        await self.ship.stream.aclose()

        for cctx in list(self.stage_0.values()) + list(self.stage_1.values()):
            await cctx.stream.aclose()

        await self.ctrl_schan.aclose()
        await self.ctrl_rchan.aclose()


@acm
async def open_static_resources(

    sh_args: StateHistoryArgs

) -> AsyncContextManager[RootContext]:

    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    async with (
        tractor.open_nursery(
            loglevel=perf_args.loglevel,
            debug_mode=perf_args.debug_mode,
        ) as an,

        ringd.open_ringd(loglevel=perf_args.loglevel) as ringd_portal,

        open_ringbuf_subscriber() as block_channel
    ):
        res_monitor_portal = await an.start_actor(
            'resource_monitor',
            enable_modules=['leap.ship._linux._resmon']
        )

        ship_portal = await an.start_actor(
            'ship_reader',
            enable_modules=['leap.ship._linux._ws'],
        )

        async with (
            res_monitor_portal.open_context(
                resource_monitor,
                sh_args=sh_args,
            ) as (res_ctx, _),

            res_ctx.open_stream() as res_stream,

            ship_portal.open_context(
                ship_reader,
                sh_args=sh_args,
            ) as (ship_ctx, _),

            ship_ctx.open_stream() as ship_stream
        ):
            resmon = ChildContext(
                ctx=res_ctx,
                stream=res_stream
            )

            ship = ChildContext(
                ctx=ship_ctx,
                stream=ship_stream
            )

            yield RootContext(
                sh_args=sh_args,
                an=an,
                ringd_portal=ringd_portal,
                ship=ship,
                resmon=resmon,
                block_channel=block_channel
            )

            await res_ctx.wait_for_result()
            await ship_ctx.wait_for_result()

        # TODO: wtf why hard_kill needed for resource_monitor?
        await an.cancel(hard_kill=True)


@acm
async def open_control_stream_handlers(

    root_ctx: RootContext

) -> AsyncContextManager[None]:
    '''
    Root actor listener to all child control streams.

    All tasks should be blocked on `async for` receiver loops so they will
    close automatically when `root_ctx.close_control_streams()` is called,
    which does `aclose()` on all context streams.

    '''
    async def _block_receiver_listener():
        '''
            - EndIsNearMsg: We are close to ending, switch all rings
              to non batched mode & flush.

            - ReachedEndMsg: We reached the end block, teardown pipeline.

        '''
        async with root_ctx.ctrl_rchan:
            async for msg in root_ctx.ctrl_rchan:
                match msg:
                    case InputConnectMsg():
                        await root_ctx.block_channel.add_channel(msg.ring_name)

                    case InputDisconnectMsg():
                        await root_ctx.block_channel.remove_channel(msg.ring_name)

                    case EndIsNearMsg():
                        flush_msg = PerfTweakMsg(
                            batch_size_options=BatchSizeOptions(
                                new_batch_size=1,
                                must_flush=True
                            )
                        )

                        await resmon.broadcast_tweak(flush_msg)

                        # await root_ctx.multi_send(flush_msg.encode(), [
                        #     *list(root_ctx.stage_0.keys()),
                        #     *list(root_ctx.stage_1.keys())
                        # ])

                        await root_ctx.resmon.stream.aclose()

                    case ReachedEndMsg():
                        # close all context control streams
                        await root_ctx.close_control_streams()
                        break

    # spawn listeners in nursery and yield
    async with trio.open_nursery() as n:
        n.start_soon(_block_receiver_listener)
        yield
