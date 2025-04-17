from heapq import (
    heappush,
    heappop
)

import msgspec
import tractor
from tractor.ipc._ringbuf import (
    RBToken,
    attach_to_ringbuf_sender,
)
from tractor.ipc._ringbuf._pubsub import open_ringbuf_subscriber

from ..structs import StateHistoryArgs
from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
)
from ._utils import get_logger


log = get_logger(__name__)


@tractor.context
async def block_joiner(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    out_token: RBToken
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    next_index = 0
    pq: list[tuple[int, dict]] = []

    def push(msg: bytes):
        payload = msgspec.msgpack.decode(msg, type=IndexedPayloadMsg)
        block = payload.decode_data(type=dict)
        # log.info(f'pushed {block["this_block"]["block_num"]} {payload.index} expect {next_index}')
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

    def pop_next() -> dict:
        '''
        Pop first block from pqueue.

        '''
        nonlocal next_index
        _, msg = heappop(pq)
        next_index += 1
        return msg

    async with (
        attach_to_ringbuf_sender(
            out_token,
            batch_size=min(sh_args.block_range, perf_args.final_batch_size)
        ) as schan,

        open_ringbuf_subscriber() as inputs,
    ):
        await ctx.started()
        async for msg in inputs:
            push(msg)

            # remove all in-order blocks from ready map
            block_batch = []
            while can_pop_next():
                block_batch.append(pop_next())

            # no ready blocks
            if len(block_batch) == 0:
                continue

            # send all ready blocks as a single batch
            payload = msgspec.msgpack.encode(block_batch)
            await schan.send(payload)

            last_block_num = block_batch[-1]['this_block']['block_num']

            if last_block_num == sh_args.end_block_num - 1:
                await schan.flush()
                log.info('joiner reached end')
                break
