from heapq import (
    heappush,
    heappop
)
from contextlib import asynccontextmanager as acm

import msgspec
import tractor
from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_sender,
)
from tractor.ipc._ringbuf._pubsub import (
    open_ringbuf_subscriber,
    open_sub_channel_at
)

from leap.ship.structs import (
    StateHistoryArgs,
    block_encoder
)
from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    StartRangeMsg,
    pipeline_decoder
)
from ._utils import get_logger


log = get_logger(__name__)


@tractor.context
async def block_joiner(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    reader_pid: int,
    reader_index: int
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    actor_name: str = tractor.current_actor().name

    output_ring = f'{actor_name}-{reader_pid}.output'

    next_index = reader_index * perf_args.ws_range_stride
    next_range = None

    # calculate correct final block offset based on ws stride

    # reader 0 will get final range
    target_end = sh_args.end_block_num

    reader_delta = perf_args.ws_readers - reader_index

    if reader_delta >= 1:
        # remove last segment, after this we are aligned with ws stride
        target_end -= - (
            sh_args.end_block_num % perf_args.ws_range_stride
        )
        reader_delta -= 1

    while reader_delta > 0:
        # for ws_readers > 2 each will finish at prev ws stride bound
        target_end -= perf_args.ws_range_stride
        reader_delta -= 1

    pq: list[tuple[int, dict]] = []

    def push(payload: IndexedPayloadMsg):
        block = payload.unwrap()
        block['meta'] = {
            'ws_index': payload.index,
        }
        if payload.meta:
            block['meta'].update(payload.meta)

        heappush(pq, (payload.index, block))

    def can_pop_next() -> bool:
        '''
        Predicate to check if we have next in order block on pqueue.

        '''
        return (
            len(pq) > 0
            and
            pq[0][0] == next_index
        )

    def pop_next() -> dict | None:
        '''
        Pop first block from pqueue.

        '''
        nonlocal next_index
        index, msg = heappop(pq)
        next_index += 1
        return msg

    try:
        async with (
            open_ringbuf(
                output_ring,
                buf_size=perf_args.buf_size
            ) as out_token,

            open_sub_channel_at('root', out_token),

            attach_to_ringbuf_sender(
                out_token,
                batch_size=perf_args.final_batch_size,
                encoder=block_encoder
            ) as schan,

            open_ringbuf_subscriber(
                decoder=pipeline_decoder
            ) as inputs,
        ):
            await ctx.started()
            log.info('ctx started')

            async for msg in inputs:
                match msg:
                    case StartRangeMsg():
                        if next_index != msg.start_msg_index:
                            next_range = msg

                    case IndexedPayloadMsg():
                        push(msg)

                        # remove all in-order blocks from ready map
                        block_batch = []
                        while can_pop_next():
                            maybe_block = pop_next()
                            if maybe_block:
                                block_batch.append(maybe_block)

                        if next_range and next_index % perf_args.ws_range_stride == 0:
                            next_index = next_range.start_msg_index
                            next_range = None

                        # no ready blocks
                        if len(block_batch) == 0:
                            continue

                        # send all ready blocks as a single batch
                        await schan.send(block_batch)

                        last_block_num = block_batch[-1]['this_block']['block_num']
                        if last_block_num == target_end - 1:
                            log.info('joiner reached end')
                            break

            await schan.flush()
            log.info('flushed')

    finally:
        log.info('exit')


@acm
async def open_block_joiner(
    sh_args: StateHistoryArgs,
    reader_pid: int,
    reader_index: int,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    portal = await an.start_actor(
        f'block_joiner-{reader_pid}',
        loglevel=perf_args.loglevels.joiners,
        enable_modules=[
            __name__,
            'tractor.ipc._ringbuf._pubsub',
            'tractor.linux._fdshare'
        ],
    )

    async with (
        portal.open_context(
            block_joiner,
            sh_args=sh_args,
            reader_pid=reader_pid,
            reader_index=reader_index
        ) as (ctx, _),
    ):
        yield

    await portal.cancel_actor()
