import os
from contextlib import asynccontextmanager as acm

import trio
import tractor
import msgspec
import trio_websocket

from tractor.ipc._ringbuf import (
    open_ringbuf,
    attach_to_ringbuf_sender
)
from trio_websocket import open_websocket_url

from leap.abis import standard
from leap.protocol import (
    apply_leap_codec
)
from leap.ship.structs import (
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksAckRequestV0
)
from leap.ship._exceptions import NodeosConnectionError

from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    StartRangeMsg,
    ReaderReadyMsg,
    ReaderControlMessages,
    pipeline_encoder,
)
from ._joiner import open_block_joiner
from ._filter import open_block_filter
from ._utils import get_logger


log = get_logger(__name__)


@tractor.context
async def ship_range_reader(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    reader_index: int,
    endpoint: str
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    log.info(f'connecting to ws {endpoint}...')

    reader_id: str = tractor.current_actor().name
    reader_pid: int = os.getpid()

    range_end: int = 0
    current_block: int = 0
    unacked_max: int = 0

    msg_index: int = 0
    start_msg_index: int = 0

    try:
        async with (
            open_websocket_url(
                endpoint,
                max_message_size=sh_args.max_message_size,
                message_queue_size=sh_args.max_messages_in_flight
            ) as ws,

            apply_leap_codec(),

            tractor.open_nursery() as an,

            open_ringbuf(
                f'filter-{reader_pid}.input',
                buf_size=perf_args.buf_size
            ) as filter_token,

            open_block_filter(
                sh_args,
                filter_token,
                reader_pid,
                an=an
            ),

            open_block_joiner(
                sh_args,
                reader_pid,
                reader_index,
                an=an
            ),

            attach_to_ringbuf_sender(
                filter_token,
                batch_size=perf_args.ws_batch_size,
                encoder=pipeline_encoder
            ) as filter_chan,

            trio.open_nursery() as tn
        ):

            # first message is ABI
            _ = await ws.get_message()

            # send get_status_request
            await ws.send_message(
                standard.pack(
                    'request',
                    GetStatusRequestV0().to_dict()
                )
            )

            # receive get_status_result
            status_result_bytes = await ws.get_message()
            status = standard.unpack('result', status_result_bytes)
            log.info(f'node status: {status}')

            await ctx.started(validate_pld_spec=False)

            async with ctx.open_stream() as stream:

                cancel_scope = trio.CancelScope()

                async def control_listener_task():
                    nonlocal msg_index, range_end, current_block, unacked_max, start_msg_index
                    async for raw_msg in stream:
                        msg = msgspec.msgpack.decode(raw_msg, type=ReaderControlMessages)
                        match msg:
                            case StartRangeMsg():
                                start_msg_index = msg.start_msg_index
                                msg_index = msg.start_msg_index

                                range_end = msg.end_block_num
                                current_block = msg.start_block_num
                                unacked_max = min(msg.range_size, sh_args.max_messages_in_flight)

                                await filter_chan.send(raw_msg)

                                # send get_blocks_request
                                get_blocks_msg = standard.pack(
                                    'request',
                                    GetBlocksRequestV0(
                                        start_block_num=msg.start_block_num,
                                        end_block_num=msg.end_block_num,
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
                                    f'{reader_id} sent get blocks request, '
                                    f'range: {msg.start_block_num}-{msg.end_block_num}, '
                                    f'max_msgs_in_flight: {sh_args.max_messages_in_flight}'
                                )

                    cancel_scope.cancel()

                tn.start_soon(control_listener_task)

                await stream.send(ReaderReadyMsg().encode())

                with cancel_scope:
                    unacked_msgs = 0
                    while True:
                        msg = await ws.get_message()
                        unacked_msgs += 1

                        payload = IndexedPayloadMsg(
                            index=msg_index,
                            data=msg,
                            meta=(
                                {
                                    'ws_size': len(msg)
                                }
                                if sh_args.block_meta
                                else None
                            )
                        )

                        await filter_chan.send(payload)
                        msg_index += 1
                        current_block += 1

                        if unacked_msgs >= unacked_max:
                            await ws.send_message(
                                standard.pack(
                                    'request',
                                    GetBlocksAckRequestV0(
                                        num_messages=sh_args.max_messages_in_flight
                                    ).to_dict()
                                )
                            )
                            unacked_msgs = 0

                        if current_block == range_end:
                            log.info(f'finished range {current_block}, {start_msg_index}-{msg_index}')
                            await stream.send(ReaderReadyMsg().encode())

                await filter_chan.flush()
                log.info('exit main msg loop')

    except trio_websocket._impl.HandshakeError as e:
        raise NodeosConnectionError(
            f'Could not connect to state history endpoint: {endpoint}\n'
            'is nodeos running?'
        ) from e

    finally:
        log.info('exit')


@acm
async def open_range_reader(
    sh_args: StateHistoryArgs,
    endpoints: list[str],
    reader_index: int,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    reader_id = f'ship-reader-{reader_index}'
    portal = await an.start_actor(
        reader_id,
        loglevel=perf_args.loglevels.ship_readers,
        enable_modules=[
            __name__,
            'tractor.ipc._ringbuf._pubsub',
            'tractor.linux._fdshare'
        ],
    )
    async with (
        portal.open_context(
            ship_range_reader,
            sh_args=sh_args,
            reader_index=reader_index,
            endpoint=(
                sh_args.endpoint
                if len(endpoints) == 1
                else
                endpoints[reader_index]
            )
        ) as (ctx, _),

        ctx.open_stream() as stream
    ):
        yield stream

    await portal.cancel_actor()
