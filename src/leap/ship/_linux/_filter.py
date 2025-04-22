from contextlib import asynccontextmanager as acm

import trio
import tractor

from tractor.trionics import gather_contexts
from tractor.ipc._ringbuf._pubsub import (
    open_ringbuf_publisher,
)
from tractor.ipc._ringbuf import (
    RBToken,
    attach_to_ringbuf_receiver,
)

from leap.protocol import apply_leap_codec
from leap.ship.structs import (
    StateHistoryArgs,
)
from leap.ship._utils import (
    ResultFilter,
)

from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    StartRangeMsg,
    pipeline_encoder,
    pipeline_decoder
)
from ._decoders import (
    open_empty_block_decoder,
    open_full_block_decoder
)
from ._utils import get_logger


log = get_logger()


@tractor.context
async def block_filter(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    in_token: RBToken,
    reader_pid: int
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    rfilter = ResultFilter(sh_args)
    inspector = rfilter.inspector

    await ctx.started()

    try:
        async with (
            apply_leap_codec(),

            tractor.open_nursery() as an,

            gather_contexts([
                open_empty_block_decoder(
                    reader_pid,
                    i,
                    sh_args,
                    an
                )
                for i in range(perf_args.empty_decoders)
            ]),

            gather_contexts([
                open_full_block_decoder(
                    reader_pid,
                    i,
                    sh_args,
                    an
                )
                for i in range(perf_args.full_decoders)
            ]),

            open_ringbuf_publisher(
                batch_size=perf_args.empty_blocks_batch_size,
                msgs_per_turn=perf_args.empty_blocks_msgs_per_turn,
                topic='empty_blocks',
                encoder=pipeline_encoder
            ) as empty_blocks_topic,

            open_ringbuf_publisher(
                batch_size=perf_args.full_blocks_batch_size,
                msgs_per_turn=perf_args.full_blocks_msgs_per_turn,
                topic='full_blocks',
                encoder=pipeline_encoder
            ) as full_blocks_topic,

            attach_to_ringbuf_receiver(
                in_token,
                decoder=pipeline_decoder
            ) as rchan,
        ):
            async def parallel_topic_await(attr: str, *args):
                async with trio.open_nursery() as tn:
                    for topic in (
                        empty_blocks_topic,
                        full_blocks_topic
                    ):
                        tn.start_soon(getattr(topic, attr), *args)

            async for raw_msg, msg in rchan.iter_raw_pairs():
                match msg:
                    case StartRangeMsg():
                        await parallel_topic_await('broadcast', raw_msg)
                        await parallel_topic_await('flush')

                    case IndexedPayloadMsg():
                        result = msg.unwrap()

                        if rfilter.is_relevant(result):
                            await full_blocks_topic.send(raw_msg)

                        else:
                            header = inspector.header_of(result)

                            await empty_blocks_topic.send(
                                msg.new_from_data(header)
                            )

            await empty_blocks_topic.flush()
            await full_blocks_topic.flush()
            log.info('flushed')

    finally:
        log.info('exit')


@acm
async def open_block_filter(
    sh_args: StateHistoryArgs,
    in_token: RBToken,
    reader_pid: int,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    portal = await an.start_actor(
        f'block_filter-{reader_pid}',
        loglevel=perf_args.loglevels.filters,
        enable_modules=[
            __name__,
            'tractor.ipc._ringbuf._pubsub',
            'tractor.linux._fdshare'
        ],
    )

    async with portal.open_context(
        block_filter,
        in_token=in_token,
        reader_pid=reader_pid,
        sh_args=sh_args,
    ) as (ctx, _):
        yield

    await portal.cancel_actor()
