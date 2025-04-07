from typing import (
    AsyncContextManager
)
from contextlib import (
    asynccontextmanager as acm
)
from dataclasses import dataclass

import trio
import msgspec
import tractor
from tractor import Context
from tractor.ipc._ringbuf._pubsub import (
    RingBufferSubscriber,
    open_ringbuf_subscriber
)
from tractor.trionics import gather_contexts

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
    ForwardMsg,
    ControlMessages
)


import tractor.ipc._ringbuf._ringd as ringd

from ._ws import ship_reader
from ._resmon import resource_monitor


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
        resmon: ChildContext | None,
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

        self._decoders_lock = trio.StrictFIFOLock()

    async def next_decoder_ids(self) -> tuple[str, list[str]]:
        async with self._decoders_lock:
            decoder_id = f'decoder-{len(self.stage_0)}'
            stage_0_id = decoder_id + '-stage-0'
            stage_1_ids = tuple(
                decoder_id + f'-stage-1.{i}'
                for i in range(int(self.perf_args.stage_ratio))
            )
            return stage_0_id, stage_1_ids

    async def add_stage_0_decoder(
        self,
        ctx: ChildContext,
        id: str
    ):
        async with self._decoders_lock:
            self.stage_0[id] = ctx

    async def add_stage_1_decoder(
        self,
        ctx: ChildContext,
        id: str
    ):
        async with self._decoders_lock:
            self.stage_1[id] = ctx

    async def get_ctx(self, actor_name: str) -> Context:
        match actor_name:
            case 'ship_reader':
                return self.ship

            case 'resource_monitor':
                if not self.resmon:
                    raise ValueError(
                        'Tried to get resource monitor context but not enabled on config'
                    )
                return self.resmon

            case _:
                async with self._decoders_lock:
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
            await (
                await self.get_ctx(dst)
            ).stream.send(msg)

    async def close_control_streams(self):
        await self.ship.stream.aclose()
        if self.resmon:
            await self.resmon.stream.aclose()
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
        ringd.open_ringd(loglevel=perf_args.loglevel) as ringd_portal,

        tractor.open_nursery(
            loglevel=perf_args.loglevel,
            debug_mode=perf_args.debug_mode,
        ) as an,

        open_ringbuf_subscriber() as block_channel
    ):
        acms = []

        # maybe spawn resource monitor
        res_monitor_portal: tractor.Portal | None = None
        if perf_args.res_monitor:
            res_monitor_portal = await an.start_actor(
                'resource_monitor',
                enable_modules=['leap.ship._linux._resmon']
            )
            acms += [res_monitor_portal.open_context(
                resource_monitor,
                sh_args=sh_args,
            )]

        ship_portal = await an.start_actor(
            'ship_reader',
            enable_modules=['leap.ship._linux._ws'],
        )

        acms = [
            ship_portal.open_context(
                ship_reader,
                sh_args=sh_args,
            ),
        ]

        async with gather_contexts(acms) as ctxs:
            if perf_args.res_monitor:
                res_ctx, ship_ctx = ctxs

            else:
                ship_ctx, _ = ctxs[0]

            contexts = [ship_ctx]

            res_ctx = None
            if perf_args.res_monitor:
                res_ctx, _ = res_ctx
                contexts.append[res_ctx]

            async with gather_contexts([
                ctx.open_stream()
                for ctx in contexts
            ]) as streams:
                resmon = None
                res_stream = None
                if perf_args.res_monitor:
                    res_stream, ship_stream = streams
                    resmon = ChildContext(
                        ctx=res_ctx,
                        stream=res_stream
                    )

                else:
                    ship_stream = streams[0]

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

            for c in contexts:
                await c.wait_for_result()

        await an.cancel()



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
                        # use perf tweak msg to signal flush
                        flush_msg = PerfTweakMsg(
                            batch_size_options=BatchSizeOptions(
                                new_batch_size=1,
                                must_flush=True
                            )
                        ).encode()

                        # send to all other pipeline actors but joiner
                        await root_ctx.multi_send(flush_msg, [
                            'ship_reader',
                            *list(root_ctx.stage_0.keys()),
                            *list(root_ctx.stage_1.keys())
                        ])

                        if root_ctx.resmon:
                            # resmon uses EndIsNearMsg as its termination msg
                            await root_ctx.resmon.stream.send(msg.encode())

                    case ReachedEndMsg():
                        # close all context control streams
                        await root_ctx.close_control_streams()
                        break

    async def _resource_monitor_listener():
        '''
        Forward messages from resource monitor to destination actors

        '''
        async with root_ctx.resmon.stream as stream:
            async for msg in stream:
                msg = msgspec.msgpack.decode(msg, type=ControlMessages)
                match msg:
                    case ForwardMsg():
                        dst_ctx = root_ctx.get_ctx(msg.dst_ctx)
                        await dst_ctx.send(msg.payload)

    # spawn listeners in nursery and yield
    async with trio.open_nursery() as n:
        n.start_soon(_block_receiver_listener)
        if root_ctx.resmon:
            n.start_soon(_resource_monitor_listener)

        yield
