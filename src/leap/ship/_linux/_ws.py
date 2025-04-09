import trio
import tractor
import antelope_rs
import trio_websocket

from trio_websocket import open_websocket_url

from tractor.ipc._ringbuf._pubsub import (
    open_ringbuf_publisher,
)

from ..structs import (
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksAckRequestV0
)
from .._exceptions import NodeosConnectionError
from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
)

import leap.ship._linux._resmon as resmon


log = tractor.log.get_logger(__name__)


@tractor.context
async def ship_reader(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.ws_batch_size)

    log.info(f'connecting to ws {sh_args.endpoint}...')

    try:
        async with (
            open_websocket_url(
                sh_args.endpoint,
                max_message_size=sh_args.max_message_size,
                message_queue_size=sh_args.max_messages_in_flight
            ) as ws,

            open_ringbuf_publisher(
                buf_size=perf_args.buf_size,
                batch_size=batch_size,
                msgs_per_turn=perf_args.ws_msgs_per_turn,
            ) as outputs,
        ):
            await resmon.register_actor(
                output=outputs
            )
            # first message is ABI
            _ = await ws.get_message()

            # send get_status_request
            await ws.send_message(
                antelope_rs.abi_pack(
                    'std',
                    'request',
                    GetStatusRequestV0().to_dict()
                )
            )

            # receive get_status_result
            status_result_bytes = await ws.get_message()
            status = antelope_rs.abi_unpack('std', 'result', status_result_bytes)
            log.info(f'node status: {status}')

            # send get_blocks_request
            get_blocks_msg = antelope_rs.abi_pack(
                'std',
                'request',
                GetBlocksRequestV0(
                    start_block_num=sh_args.start_block_num,
                    end_block_num=sh_args.end_block_num,
                    max_messages_in_flight=sh_args.max_messages_in_flight,
                    have_positions=[],
                    irreversible_only=sh_args.irreversible_only,
                    fetch_block=sh_args.fetch_block,
                    fetch_traces=sh_args.fetch_traces,
                    fetch_deltas=sh_args.fetch_deltas
                ).to_dict()
            )
            await ws.send_message(get_blocks_msg)
            log.info(
                f'sent get blocks request, '
                f'range: {sh_args.start_block_num}-{sh_args.end_block_num}, '
                f'max_msgs_in_flight: {sh_args.max_messages_in_flight}'
            )

            await ctx.started()

            msg_index = 0
            unacked_msgs = 0
            while True:
                msg = await ws.get_message()
                unacked_msgs += 1

                payload = IndexedPayloadMsg(
                    index=msg_index,
                    data=msg[1:]
                )

                await outputs.send(payload.encode())
                msg_index += 1

                if unacked_msgs >= sh_args.max_messages_in_flight:
                    await ws.send_message(
                        antelope_rs.abi_pack(
                            'std',
                            'request',
                            GetBlocksAckRequestV0(
                                num_messages=sh_args.max_messages_in_flight
                            ).to_dict()
                        )
                    )
                    unacked_msgs = 0

    except trio_websocket._impl.HandshakeError as e:
        raise NodeosConnectionError(
            f'Could not connect to state history endpoint: {sh_args.endpoint}\n'
            'is nodeos running?'
        ) from e
