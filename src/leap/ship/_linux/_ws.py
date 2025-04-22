import os
from typing import Iterator
from contextlib import asynccontextmanager as acm

import trio
import tractor
import msgspec

from leap.protocol import (
    apply_leap_codec,
    limit_leap_plds
)
from leap.ship.structs import StateHistoryArgs

from .structs import (
    PerformanceOptions,
    StartRangeMsg,
    ReaderReadyMsg,
    ReaderControlMessages
)
from ._utils import get_logger

from ._ws_reader import open_range_reader


log = get_logger(__name__)


class ReachedEndError(Exception):
    ...


class RangeMsgIterator:

    def __init__(self, sh_args: StateHistoryArgs) -> None:
        self.sh_args: StateHistoryArgs = StateHistoryArgs.from_dict(sh_args)
        self.perf_args: PerformanceOptions = PerformanceOptions.from_dict(sh_args.backend_kwargs)

        self._start_num: int = sh_args.start_block_num
        self._end_num: int = sh_args.end_block_num
        self._stride: int = self.perf_args.ws_range_stride

        self._current: int = self._start_num

    @property
    def ranges(self) -> list[StartRangeMsg]:
        return [
            msg
            for msg in RangeMsgIterator(self.sh_args)
        ]

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


class ShipSupervisor:

    def __init__(
        self,
        sh_args: StateHistoryArgs,
    ):
        sh_args = StateHistoryArgs.from_dict(sh_args)
        perf_args = PerformanceOptions.from_dict(
            sh_args.backend_kwargs
        )

        self.sh_args = sh_args
        self.perf_args = perf_args

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

        self._endpoints = endpoints

        self._range_iter = RangeMsgIterator(sh_args)

        range_msgs = [msg for msg in self._range_iter.ranges]
        log.info(
            'range information:\n'
            + '\n'.join([
                str(msg)
                for msg in range_msgs
            ])
        )

        self._range_lock = trio.StrictFIFOLock()

        self._initial_readers = min(
            len(range_msgs),
            perf_args.ws_readers
        )

        self._reader_lock = trio.StrictFIFOLock()
        self._readers: list[tractor.MsgStream] = []

    async def broadcast(self, msg: ReaderControlMessages):
        async with (
            self._reader_lock,
            trio.open_nursery() as n,
        ):
            for stream in self._reader_streams:
                n.start_soon(
                    stream.send,
                    msg.encode()
                )

    async def next_range_msg(self) -> StartRangeMsg:
        async with self._range_lock:
            try:
                return next(self._range_iter)

            except StopIteration:
                raise ReachedEndError

    async def run_range_reader(
        self,
        an: tractor.ActorNursery,
    ):
        await self._reader_lock.acquire()
        reader_index = len(self._readers)
        async with open_range_reader(
            self.sh_args,
            self._endpoints,
            reader_index,
            an
        ) as stream:
            self._readers.append(stream)
            self._reader_lock.release()
            log.info(f'started range reader {reader_index}')
            async for msg in stream:
                msgspec.msgpack.decode(msg, type=ReaderReadyMsg)
                try:
                    msg = await self.next_range_msg()
                    await stream.send(msg.encode())
                    log.info(
                        f'reader {reader_index} start range: '
                        f'{msg.start_block_num}-{msg.end_block_num}, '
                        f'start index: {msg.start_msg_index}'
                    )

                except ReachedEndError:
                    break

        log.info(f'reader {reader_index} closed')

    @acm
    async def run(self):
        async with (
            tractor.open_nursery() as an,
            trio.open_nursery() as tn,

            apply_leap_codec(),
            limit_leap_plds(),
        ):
            for i in range(self._initial_readers):
                tn.start_soon(self.run_range_reader, an)

            yield


_supervisor: ShipSupervisor | None = None


@tractor.context
async def ship_supervisor(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs
):
    global _supervisor
    _supervisor = ShipSupervisor(sh_args)
    try:
        async with _supervisor.run():
            await ctx.started()

    finally:
        log.info('exit')


@acm
async def open_ship_supervisor(
    sh_args: StateHistoryArgs,
    an: tractor.ActorNursery
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(sh_args.backend_kwargs)

    pid = os.getpid()

    portal = await an.start_actor(
        f'ship_supervisor-{pid}',
        loglevel=perf_args.loglevels.ship_supervisor,
        enable_modules=[
            __name__,
        ],
    )

    async with (
        apply_leap_codec(),

        portal.open_context(
            ship_supervisor,
            sh_args=sh_args
        ) as (ctx, _),

        limit_leap_plds(),
    ):
        yield

    await portal.cancel_actor()
