import tractor
import msgspec
import antelope_rs

from tractor.ipc import (
    attach_to_ringbuf_receiver,
    attach_to_ringbuf_channel,
)
import tractor.ipc._ringbuf._ringd as ringd
from tractor.ipc._ringbuf._pubsub import (
    open_ringbuf_publisher,
    open_pub_channel_at,
    open_sub_channel_at
)

from ..structs import (
    StateHistoryArgs,
    GetBlocksResultV0,
)
from ..decoder import BlockDecoder

from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
)

import leap.ship._linux._resmon as resmon


log = tractor.log.get_logger(__name__)


@tractor.context
async def stage_0_decoder(
    ctx: tractor.Context,
    stage_0_id: str,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_0_batch_size)

    input_ring = stage_0_id + '.input'

    async with (
        open_pub_channel_at(
            'ship_reader',
            input_ring,
            must_exist=False
        ),

        ringd.attach_ringbuf(name=input_ring) as in_token,

        attach_to_ringbuf_receiver(
            in_token
        ) as rchan,

        open_ringbuf_publisher(
            buf_size=perf_args.buf_size,
            batch_size=batch_size,
            msgs_per_turn=perf_args.stage_0_msgs_per_turn,
        ) as outputs,
    ):
        await resmon.register_actor(
            output=outputs
        )
        await ctx.started()

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

    log.info(f'{stage_0_id} exit')


@tractor.context
async def stage_1_decoder(
    ctx: tractor.Context,
    stage_0_id: str,
    stage_1_id: str,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_1_batch_size)

    decoder = BlockDecoder(sh_args)

    input_ring = stage_1_id + '.input'
    output_ring = stage_1_id + '.output'

    async with (
        open_pub_channel_at(stage_0_id, input_ring),

        ringd.attach_ringbuf(name=input_ring) as in_token,

        open_sub_channel_at('root', output_ring),

        ringd.attach_ringbuf(name=output_ring) as out_token,

        attach_to_ringbuf_channel(
            in_token,
            out_token,
            batch_size=batch_size
        ) as chan,
    ):

        await resmon.register_actor(
            output=chan
        )
        await ctx.started()

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

    log.info(f'{stage_1_id} exit')
