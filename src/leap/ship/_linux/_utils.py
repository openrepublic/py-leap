import json
import importlib
from heapq import (
    heappush,
    heappop
)
from contextlib import asynccontextmanager as acm

import trio
import tractor
import msgspec

from tractor.ipc import (
    RingBufferSendChannel,
    RingBufferReceiveChannel
)
from tractor.ipc._ringbuf._pubsub import (
    RingBufferPublisher,
    RingBufferSubscriber
)

from .structs import (
    IndexedPayloadMsg,
    EndIsNearMsg,
    ReachedEndMsg,
    OutputConnectMsg,
    OutputDisconnectMsg,
    InputConnectMsg,
    InputDisconnectMsg,
    ControlMessages,
)

from leap.sugar import LeapJSONEncoder
from ..structs import Block
from .._benchmark import BenchmarkedBlockReceiver


# log = tractor.log.get_logger(__name__)
log = tractor.log.get_console_log(level='info')


@acm
async def control_listener_task(
    ctx: tractor.Context,
    inputs: RingBufferSubscriber | RingBufferReceiveChannel | None = None,
    output: RingBufferPublisher | RingBufferSendChannel | None = None,
):
    async def _listener(stream):
        async for msg in stream:
            msg = msgspec.msgpack.decode(msg, type=ControlMessages)
            match msg:
                case OutputConnectMsg():
                    if not isinstance(output, RingBufferPublisher):
                        raise RuntimeError(
                            f'Got OutputConnectMsg but output is of type {type(output)}'
                        )

                    await output.add_channel(
                        name=msg.ring_name
                    )

                case OutputDisconnectMsg():
                    if not isinstance(output, RingBufferPublisher):
                        raise RuntimeError(
                            f'Got OutputDisconnectMsg but output is of type {type(output)}'
                        )

                    await output.remove_channel(
                        name=msg.ring_name
                    )

                case InputConnectMsg():
                    if not isinstance(inputs, RingBufferSubscriber):
                        raise RuntimeError(
                            f'Got InputConnectMsg but output is of type {type(output)}'
                        )

                    await inputs.add_channel(
                        name=msg.ring_name
                    )

                case InputDisconnectMsg():
                    if not isinstance(inputs, RingBufferSubscriber):
                        raise RuntimeError(
                            f'Got InputDisconnectMsg but output is of type {type(output)}'
                        )

                    await inputs.remove_channel(
                        name=msg.ring_name
                    )

        log.info('control_listener_task exit')

    async with (
        ctx.open_stream() as control_stream,
        trio.open_nursery() as n
    ):
        n.start_soon(_listener, control_stream)
        yield control_stream


class BlockReceiver(BenchmarkedBlockReceiver):

    def __init__(
        self,
        root_ctx
    ):
        super().__init__(
            root_ctx.block_channel,
            root_ctx.sh_args
        )

        self.root_ctx = root_ctx

        self._next_index = 0
        self._near_end = False
        self._pqueue: list[tuple[int, dict]] = []

    def _can_pop_next(self) -> bool:
        '''
        Predicate to check if we have next in order block on pqueue.

        '''
        return (
            len(self._pqueue) > 0
            and
            self._pqueue[0][0] == self._next_index
        )

    def _pop_next(self) -> dict:
        '''
        Pop first block from pqueue.

        '''
        _, msg = heappop(self._pqueue)
        self._next_index += 1
        return msg

    async def _drain_to_heap(self):
        '''
        While we dont have next in order block on pqueue, receive from
        ring subscriber, after return its implied next in order block
        is ready and present on pqueue.

        '''
        new_blocks = 0
        while not self._can_pop_next():
            msg = await self._rchan.receive()
            msg = msgspec.msgpack.decode(msg, type=IndexedPayloadMsg)
            block = msg.decode_data(type=dict)
            heappush(self._pqueue, (msg.index, block))
            new_blocks += 1

        if new_blocks > 0:
            self._maybe_benchmark(new_blocks)

    async def _iterator(self):
        '''
        Until ring subscriber is closed, await next in order message, check for
        end conditions and optionally convert output to desired format.

        '''
        while True:
            try:
                await self._drain_to_heap()

            except trio.ClosedResourceError:
                break

            blocks: list[dict] = []
            while self._can_pop_next():
                blocks.append(self._pop_next())

            if len(blocks) == 0:
                raise RuntimeError('Expected batch size to be always > 0')

            last_block_num = blocks[-1]['this_block']['block_num']

            # maybe signal near end
            if (
                not self._near_end
                and
                self._args.end_block_num - last_block_num
                <=
                self._args.max_messages_in_flight * 3
            ):
                self._near_end = True
                await self.root_ctx.ctrl_schan.send(EndIsNearMsg())

            # maybe we reached end?
            if last_block_num == self._args.end_block_num - 1:
                print('reached end')
                await self.root_ctx.ctrl_schan.send(ReachedEndMsg())
                await self._rchan.aclose()

            if self._args.output_convert:
                try:
                    blocks = msgspec.convert(blocks, type=list[Block])

                except msgspec.ValidationError as e:
                    try:
                        e.add_note(
                            'Msgspec error while decoding batch:\n' +
                            json.dumps(blocks, indent=4, cls=LeapJSONEncoder)
                        )

                    except Exception as inner_e:
                        inner_e.add_note('could not decode without type either!')
                        raise inner_e from e

                    raise e

            if self._args.output_batched:
                yield blocks

            else:
                for block in blocks:
                    yield block


def import_module_path(path: str):
    module_path, func_name = path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
