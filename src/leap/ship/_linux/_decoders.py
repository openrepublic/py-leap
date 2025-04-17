import os
from contextlib import (
    asynccontextmanager as acm
)

import tractor
import msgspec
import antelope_rs

from tractor.trionics import (
    gather_contexts,
    maybe_open_nursery
)
from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_receiver,
    attach_to_ringbuf_channel,
)
from tractor.ipc._ringbuf._pubsub import (
    open_ringbuf_publisher,
    open_pub_channel_at,
    open_sub_channel_at,
)

from ..structs import (
    StateHistoryArgs,
    GetBlocksResultV0,
)
from ..decoder import BlockDecoder

from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    EndReachedMsg,
    PipelineMessages
)
from ._utils import get_logger


log = get_logger(__name__)


@tractor.context
async def stage_0_decoder(
    ctx: tractor.Context,
    reader_id: str,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_0_batch_size)

    pid = os.getpid()

    stage_0_id: str = tractor.current_actor().name

    stage_1_ids = [
        f'{stage_0_id}.stage-1.{i}'
        for i in range(perf_args.stage_ratio)
    ]

    input_ring = f'{stage_0_id}-{pid}.input'

    log.info(f'opening {stage_0_id}')

    try:
        async with tractor.open_nursery() as an:
            stage_1_portals = [
                await an.start_actor(
                    stage_1_id,
                    loglevel=perf_args.loglevels.decoder_stage_1,
                    enable_modules=[
                        'leap.ship._linux._decoders',
                        'tractor.ipc._ringbuf._pubsub',
                        'tractor.linux._fdshare'
                    ]
                )
                for stage_1_id in stage_1_ids
            ]
            async with (
                open_ringbuf(
                    input_ring,
                    buf_size=perf_args.buf_size
                ) as in_token,

                open_pub_channel_at(
                    reader_id,
                    in_token,
                ),

                attach_to_ringbuf_receiver(
                    in_token
                ) as rchan,

                open_ringbuf_publisher(
                    batch_size=batch_size,
                    msgs_per_turn=perf_args.stage_0_msgs_per_turn,
                ) as outputs,

                gather_contexts([
                    portal.open_context(
                        stage_1_decoder,
                        stage_0_id=stage_0_id,
                        sh_args=sh_args
                    )
                    for portal in stage_1_portals
                ])
            ):
                log.info(f'channels ready on {stage_0_id}')

                await ctx.started(input_ring)

                async for raw_msg in rchan:
                    msg = msgspec.msgpack.decode(raw_msg, type=PipelineMessages)

                    match msg:
                        case EndReachedMsg():
                            await outputs.broadcast(raw_msg)
                            await outputs.flush()
                            log.info('flushed')
                            break

                        case IndexedPayloadMsg():
                            result = antelope_rs.abi_unpack_msgpack(
                                'std',
                                'get_blocks_result_v0',
                                msg.decode_data()
                            )

                            await outputs.send(
                                msg.new_from_data(result).encode()
                            )

            await an.cancel()

    finally:
        log.info('exit')


@tractor.context
async def stage_1_decoder(
    ctx: tractor.Context,
    stage_0_id: str,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_1_batch_size)
    pid = os.getpid()

    decoder = BlockDecoder(sh_args)

    stage_1_id: str = tractor.current_actor().name

    input_ring = f'{stage_1_id}-{pid}.input'
    output_ring = f'{stage_1_id}-{pid}.output'

    log.info(f'opening {stage_1_id}')

    try:
        async with (
            open_ringbuf(
                input_ring,
                buf_size=perf_args.buf_size
            ) as in_token,

            open_pub_channel_at(stage_0_id, in_token),

            open_ringbuf(
                output_ring,
                buf_size=perf_args.buf_size
            ) as out_token,

            open_sub_channel_at('block_joiner', out_token),

            attach_to_ringbuf_channel(
                in_token,
                out_token,
                batch_size=batch_size
            ) as chan,
        ):
            await ctx.started()

            async for msg in chan:
                msg = msgspec.msgpack.decode(msg, type=PipelineMessages)
                match msg:
                    case EndReachedMsg():
                        await chan.flush()
                        log.info('flushed')
                        break

                    case IndexedPayloadMsg():
                        result = msg.decode(type=GetBlocksResultV0)
                        block = decoder.decode_block_result(result)

                        block['ws_index'] = msg.index
                        block['ws_size'] = msg.ws_size

                        await chan.send(
                            msg.new_from_data(block).encode()
                        )

    finally:
        log.info('exit')


@acm
async def open_decoder(
    name: str,
    reader_id: str,
    sh_args: StateHistoryArgs,
    nursery: tractor.ActorNursery | None = None
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    async with maybe_open_nursery(
        nursery=nursery,
        lib=tractor,
    ) as an:
        stage_0_portal = await an.start_actor(
            name,
            loglevel=perf_args.loglevels.decoder_stage_0,
            enable_modules=[
                'leap.ship._linux._decoders',
                'tractor.ipc._ringbuf._pubsub',
                'tractor.linux._fdshare'
            ],
        )

        async with stage_0_portal.open_context(
            stage_0_decoder,
            reader_id=reader_id,
            sh_args=sh_args,
        ) as (stage_0_ctx, _):
            yield

        await an.cancel()

