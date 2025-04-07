from typing import AsyncContextManager
from contextlib import asynccontextmanager as acm

import trio
import tractor
import msgspec
import antelope_rs

from tractor.ipc import (
    attach_to_ringbuf_receiver,
    attach_to_ringbuf_channel,
)
from tractor.trionics import gather_contexts
import tractor.ipc._ringbuf._ringd as ringd
from tractor.ipc._ringbuf._pubsub import open_ringbuf_publisher

from ..structs import (
    StateHistoryArgs,
    GetBlocksResultV0,
)
from ..decoder import BlockDecoder

from ._utils import control_listener_task
from ._context import (
    ChildContext,
    RootContext
)
from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    InputConnectMsg,
    OutputConnectMsg,
)


# log = tractor.log.get_logger(__name__)
log = tractor.log.get_console_log(level='info')


@tractor.context
async def stage_0_decoder(
    ctx: tractor.Context,
    id: str,
    sh_args: StateHistoryArgs,
    input_ring: str
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_0_batch_size)

    async with (
        ringd.open_ringbuf(
            name=input_ring,
            must_exist=True
        ) as in_token,

        attach_to_ringbuf_receiver(
            in_token
        ) as rchan,

        open_ringbuf_publisher(
            batch_size=batch_size,
            force_cancel=True
        ) as outputs,
    ):
        await ctx.started()
        async with control_listener_task(
            ctx,
            inputs=rchan,
            output=outputs
        ):
            async for msg in rchan:
                payload = msgspec.msgpack.decode(msg, type=IndexedPayloadMsg)

                result = antelope_rs.abi_unpack_msgpack(
                    'std',
                    'get_blocks_result_v0',
                    payload.decode_data()
                )

                await outputs.send(
                    IndexedPayloadMsg(
                        index=payload.index,
                        data=result
                    ).encode()
                )

    log.info(f'{id} exit')


@tractor.context
async def stage_1_decoder(
    ctx: tractor.Context,
    id: str,
    sh_args: StateHistoryArgs,
    input_ring: str,
    output_ring: str,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_1_batch_size)

    decoder = BlockDecoder(sh_args)

    async with (
        ringd.open_ringbuf(
            name=input_ring,
            must_exist=True
        ) as in_token,

        ringd.open_ringbuf(
            name=output_ring,
            must_exist=True
        ) as out_token,

        attach_to_ringbuf_channel(
            in_token,
            out_token,
            batch_size=batch_size
        ) as chan,
    ):
        await ctx.started()
        async with control_listener_task(
            ctx,
            output=chan
        ):
            async for msg in chan:
                payload = msgspec.msgpack.decode(msg, type=IndexedPayloadMsg)

                result = payload.decode(type=GetBlocksResultV0)
                block = decoder.decode_block_result(result)

                await chan.send(
                    IndexedPayloadMsg(
                        index=payload.index,
                        data=block
                    ).encode()
                )

    log.info(f'{id} exit')


@acm
async def open_stage_1_decoder(

    root_ctx: RootContext,
    sid: str

) -> AsyncContextManager[tuple[ChildContext, str, str]]:
    '''
    Called by `open_decoder`.

    Spawn a single stage 1 decoder actor.

        0. allocate an input & output ringbuf pair

        1. start stage_1_decoder context and stream

        2. register on RootContext and yield ctx, stream & rings
    '''

    portal = await root_ctx.an.start_actor(
        sid,
        enable_modules=['leap.ship._linux._decoders']
    )

    input_ring = sid + '.input'
    output_ring = sid + '.output'

    async with (
        ringd.open_ringbuf(
            input_ring,
            buf_size=root_ctx.perf_args.buf_size,
            must_exist=False
        ),

        ringd.open_ringbuf(
            output_ring,
            buf_size=root_ctx.perf_args.buf_size,
            must_exist=False
        ),

        portal.open_context(
            stage_1_decoder,
            id=sid,
            sh_args=root_ctx.sh_args,
            input_ring=input_ring,
            output_ring=output_ring
        ) as (ctx, _sent),

        ctx.open_stream() as stream
    ):
        cctx = ChildContext(
            ctx=ctx,
            stream=stream
        )
        await root_ctx.add_stage_1_decoder(cctx, sid)
        yield cctx, input_ring, output_ring


@acm
async def open_decoder(

    root_ctx: RootContext

) -> AsyncContextManager[None]:
    '''
    Create and connect a new decoder pipeline:

      0. start stage 0 decoder actor.

      1. open variable amount of state 1 actors based on `PerformanceOptions.stage_ratio` config
        (see open_stage_1_decoder), this will allocate all rings necesary for stage 1.

      2. allocate a new ringbuf to be input for stage 0 decoder.

      3. open stage_0_decoder ctx & control stream.

      4. register new stage 0 decoder on RootContext.

      5. connect all stage 1 outputs to joiner actor.

      6. connect all stage 0 outputs to stage 0 actor.

      7. finally connect stage 0 input to ship actor.


    '''

    perf_args = root_ctx.perf_args

    stage_0_id, stage_1_ids = await root_ctx.next_decoder_ids()

    stage_0_portal = await root_ctx.an.start_actor(
        stage_0_id,
        enable_modules=['leap.ship._linux._decoders'],
    )

    stage_0_input = stage_0_id + '.input'

    async with (
        gather_contexts([
            open_stage_1_decoder(
                root_ctx,
                stage_1_id
            )
            for stage_1_id in stage_1_ids
        ]) as stage_1_ctxs,

        # ship -> stage 0
        ringd.open_ringbuf(
            stage_0_input,
            buf_size=perf_args.buf_size,
            must_exist=False
        ),

        stage_0_portal.open_context(
            stage_0_decoder,
            id=stage_0_id,
            sh_args=root_ctx.sh_args,
            input_ring=stage_0_input
        ) as (stage_0_ctx, _),

        stage_0_ctx.open_stream() as stage_0_stream,
    ):
        await root_ctx.add_stage_0_decoder(
            ChildContext(
                ctx=stage_0_ctx,
                stream=stage_0_stream
            ),
            stage_0_id
        )

        # connect stage 1 outputs to joiner
        for _, _, ring_name in stage_1_ctxs:
            await root_ctx.ctrl_schan.send(
                InputConnectMsg(
                    ring_name=ring_name
                )
            )

        # connect stage 0 outputs
        for _, ring_name, _ in stage_1_ctxs:
            await stage_0_stream.send(
                OutputConnectMsg(
                    ring_name=ring_name
                ).encode()
            )

        # connect ship output
        await root_ctx.ship.stream.send(
            OutputConnectMsg(
                ring_name=stage_0_input
            ).encode()
        )

        yield
