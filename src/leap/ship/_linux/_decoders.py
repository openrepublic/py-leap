from contextlib import asynccontextmanager as acm

import tractor

from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_channel,
)
from tractor.ipc._ringbuf._pubsub import (
    open_pub_channel_at,
    open_sub_channel_at,
)

from leap.abis import standard
from leap.ship.structs import StateHistoryArgs
from leap.ship.decoder import BlockDecoder

from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    StartRangeMsg,
    pipeline_encoder,
    pipeline_decoder
)
from ._utils import get_logger


log = get_logger(__name__)


@tractor.context
async def empty_block_decoder(
    ctx: tractor.Context,
    reader_pid: int,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    actor_name: str = tractor.current_actor().name
    filter_name: str = f'block_filter-{reader_pid}'
    joiner_name: str = f'block_joiner-{reader_pid}'

    input_ring = f'{actor_name}-{reader_pid}.input'
    output_ring = f'{actor_name}-{reader_pid}.output'

    await ctx.started()
    log.info('ctx started')

    try:
        async with (
            open_ringbuf(
                input_ring,
                buf_size=perf_args.buf_size
            ) as in_token,

            open_pub_channel_at(
                filter_name,
                in_token,
                topic='empty_blocks'
            ),

            open_ringbuf(
                output_ring,
                buf_size=perf_args.buf_size
            ) as empty_token,

            open_sub_channel_at(joiner_name, empty_token),

            attach_to_ringbuf_channel(
                in_token,
                empty_token,
                batch_size=perf_args.empty_blocks_batch_size,
                encoder=pipeline_encoder,
                decoder=pipeline_decoder
            ) as chan,
        ):

            async for msg in chan:
                match msg:
                    case StartRangeMsg():
                        await chan.flush()

                    case IndexedPayloadMsg():
                        block = standard.unpack(
                            'get_blocks_result_v0_header',
                            msg.unwrap()
                        )
                        await chan.send(
                            msg.new_from_data(block)
                        )

            await chan.flush()
            log.info('exit main msg loop')

    finally:
        log.info('exit')


@tractor.context
async def full_block_decoder(
    ctx: tractor.Context,
    reader_pid: int,
    decoder_index: int,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    decoder = BlockDecoder(sh_args)

    actor_name: str = tractor.current_actor().name
    filter_name: str = f'block_filter-{reader_pid}'
    joiner_name: str = f'block_joiner-{reader_pid}'

    input_ring = f'{actor_name}-{reader_pid}.input'
    output_ring = f'{actor_name}-{reader_pid}.output'

    await ctx.started()
    log.info('ctx started')

    try:
        async with (
            open_ringbuf(
                input_ring,
                buf_size=perf_args.small_buf_size
            ) as in_token,

            open_pub_channel_at(
                filter_name,
                in_token,
                topic='full_blocks'
            ),

            open_ringbuf(
                output_ring,
                buf_size=perf_args.small_buf_size
            ) as out_token,

            open_sub_channel_at(joiner_name, out_token),

            attach_to_ringbuf_channel(
                in_token,
                out_token,
                batch_size=perf_args.full_blocks_batch_size,
                encoder=pipeline_encoder,
                decoder=pipeline_decoder
            ) as chan,
        ):
            async for msg in chan:
                match msg:
                    case StartRangeMsg():
                        if decoder_index == 0:
                            await chan.send(msg)

                        await chan.flush()

                    case IndexedPayloadMsg():
                        result = standard.unpack(
                            'get_blocks_result_v0',
                            (msg.unwrap())[1:]
                        )
                        og_traces, og_deltas = decoder.decode_result_dict(result)

                        await chan.send(
                            msg.new_from_data(
                                result,
                                add_meta=(
                                    {
                                        'traces_len': len(result['traces']),
                                        'traces_og_len': og_traces,
                                        'deltas_len': len(result['deltas']),
                                        'deltas_og_len': og_deltas
                                    }
                                    if sh_args.block_meta
                                    else None
                                )
                            )
                        )

            await chan.flush()

    finally:
        log.info('exit')


@acm
async def open_full_block_decoder(
    reader_pid: int,
    decoder_index: int,
    sh_args: StateHistoryArgs,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    portal = await an.start_actor(
        f'full_decoder-{reader_pid}-{decoder_index}',
        loglevel=perf_args.loglevels.full_decoders,
        enable_modules=[
            __name__,
            'tractor.linux._fdshare'
        ],
    )

    async with portal.open_context(
        full_block_decoder,
        reader_pid=reader_pid,
        decoder_index=decoder_index,
        sh_args=sh_args
    ) as (ctx, _):
        yield

    await portal.cancel_actor()


@acm
async def open_empty_block_decoder(
    reader_pid: int,
    decoder_index: int,
    sh_args: StateHistoryArgs,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    portal = await an.start_actor(
        f'empty_decoder-{reader_pid}-{decoder_index}',
        loglevel=perf_args.loglevels.empty_decoders,
        enable_modules=[
            __name__,
            'tractor.linux._fdshare'
        ],
    )

    async with portal.open_context(
        empty_block_decoder,
        reader_pid=reader_pid,
        sh_args=sh_args,
    ) as (ctx, _):
        yield

    await portal.cancel_actor()
