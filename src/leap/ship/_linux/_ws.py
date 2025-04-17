from typing import Iterator

import trio
import tractor
import msgspec
import antelope_rs
import trio_websocket

from trio_websocket import open_websocket_url

from tractor.ipc._ringbuf._pubsub import open_ringbuf_publisher

from ..structs import (
    Struct,
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksAckRequestV0
)
from .._exceptions import NodeosConnectionError
from .structs import (
    PerformanceOptions,
    IndexedPayloadMsg,
    EndReachedMsg
)
from ._decoders import open_decoder
from ._utils import get_logger


log = get_logger(__name__)


class StartRangeMsg(Struct, frozen=True, tag=True):
    start_msg_index: int
    start_block_num: int
    end_block_num: int
    is_last: bool

    @property
    def range_size(self) -> int:
        return self.end_block_num - self.start_block_num


class CloseReaderMsg(Struct, frozen=True, tag=True):
    ...


ReaderControlMessages = StartRangeMsg | CloseReaderMsg


class ReaderReadyMsg(Struct, frozen=True, tag=True):
    ...


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
    batch_size = min(sh_args.block_range, perf_args.ws_batch_size)

    log.info(f'connecting to ws {endpoint}...')

    reader_id: str = tractor.current_actor().name

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

            open_ringbuf_publisher(
                batch_size=batch_size,
                msgs_per_turn=perf_args.ws_msgs_per_turn,
            ) as outputs,

            open_decoder(
                f'decoder-{reader_index}',
                reader_id,
                sh_args,
            ),

            trio.open_nursery() as tn
        ):

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

            await ctx.started()

            async with ctx.open_stream() as stream:

                cancel_scope = trio.CancelScope()

                async def control_listener_task():
                    nonlocal msg_index, range_end, current_block, unacked_max, start_msg_index
                    async for msg in stream:
                        msg = msgspec.msgpack.decode(msg, type=ReaderControlMessages)
                        match msg:
                            case CloseReaderMsg():
                                await outputs.broadcast(EndReachedMsg().encode())
                                cancel_scope.cancel()
                                break

                            case StartRangeMsg():
                                start_msg_index = msg.start_msg_index
                                msg_index = msg.start_msg_index

                                range_end = msg.end_block_num
                                current_block = msg.start_block_num
                                unacked_max = min(msg.range_size, sh_args.max_messages_in_flight)

                                # send get_blocks_request
                                get_blocks_msg = antelope_rs.abi_pack(
                                    'std',
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

                tn.start_soon(control_listener_task)

                await stream.send(ReaderReadyMsg().encode())

                with cancel_scope:
                    unacked_msgs = 0
                    while True:
                        msg = await ws.get_message()
                        unacked_msgs += 1

                        payload = IndexedPayloadMsg(
                            index=msg_index,
                            ws_size=len(msg),
                            data=msg[1:]
                        )

                        await outputs.send(payload.encode())
                        msg_index += 1
                        current_block += 1

                        if unacked_msgs >= unacked_max:
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

                        if current_block == range_end:
                            log.info(f'finished range {current_block}, {start_msg_index}-{msg_index}')
                            await stream.send(ReaderReadyMsg().encode())

                await outputs.flush()
                log.info('exit main msg loop')

    except trio_websocket._impl.HandshakeError as e:
        raise NodeosConnectionError(
            f'Could not connect to state history endpoint: {endpoint}\n'
            'is nodeos running?'
        ) from e

    finally:
        log.info('exit')


class ReachedEndError(Exception):
    ...


class RangeMsgIterator:

    def __init__(self, sh_args: StateHistoryArgs) -> None:
        self.sh_args: StateHistoryArgs = StateHistoryArgs.from_dict(sh_args)
        self.perf_args: PerformanceOptions = PerformanceOptions.from_dict(sh_args.backend_kwargs)

        self._start_num: int = sh_args.start_block_num
        self._end_num: int = sh_args.end_block_num  # non-inclusive
        self._stride: int = self.perf_args.ws_range_stride

        self._current: int = self._start_num

    def __iter__(self) -> Iterator[StartRangeMsg]:
        return self

    def __next__(self) -> StartRangeMsg:
        if self._current >= self._end_num:
            raise StopIteration

        start: int = self._current

        if start == self._start_num:
            end: int = min(((start // self._stride) + 1) * self._stride, self._end_num)
        else:
            end: int = min(start + self._stride, self._end_num)

        is_last: bool = end >= self._end_num
        msg_index: int = start - self._start_num

        msg: StartRangeMsg = StartRangeMsg(
            start_msg_index=msg_index,
            start_block_num=start,
            end_block_num=end,
            is_last=is_last
        )

        self._current = end

        return msg


@tractor.context
async def ship_supervisor(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    endpoints = perf_args.extra_endpoints

    if sh_args.endpoint not in endpoints:
        endpoints = [sh_args.endpoint, *endpoints]

    if (
        len(endpoints) != 1
        and
        len(endpoints) < perf_args.ws_readers
    ):
        raise ValueError(
            'If user extra_endpoints its expected to have the '
            'same amount of total endpoints as perf_args.ws_readers'
        )

    block_range = RangeMsgIterator(sh_args)
    range_lock = trio.StrictFIFOLock()

    async def next_range_msg() -> StartRangeMsg:
        async with range_lock:
            try:
                return next(block_range)

            except StopIteration:
                raise ReachedEndError

    async with tractor.open_nursery() as an:
        async def open_reader(
            reader_index: int
        ):
            reader_id = f'ship-reader-{reader_index}'
            portal = await an.start_actor(
                reader_id,
                loglevel=perf_args.loglevels.ship_reader,
                enable_modules=[
                    'leap.ship._linux._ws',
                    'tractor.ipc._ringbuf._pubsub',
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
                log.info(f'started range reader {reader_index}')
                async for msg in stream:
                    msgspec.msgpack.decode(msg, type=ReaderReadyMsg)
                    try:
                        msg = await next_range_msg()
                        await stream.send(msg.encode())
                        log.info(
                            f'reader {reader_index} start range: '
                            f'{msg.start_block_num}-{msg.end_block_num}, '
                            f'start index: {msg.start_msg_index}'
                        )

                    except ReachedEndError:
                        log.info(f'closing reader {reader_index}')
                        await stream.send(CloseReaderMsg().encode())
                        return

        await ctx.started()

        async with trio.open_nursery() as tn:
            for i in range(perf_args.ws_readers):
                tn.start_soon(open_reader, i)

        await an.cancel()
