import time
import json

from typing import Callable

import trio
import msgspec

from leap.sugar import LeapJSONEncoder
from leap.ship.structs import (
    Block,
    Struct,
    StateHistoryArgs,
)


class Sample(Struct):
    delta: float = 0
    blocks: int = 0
    txs: int = 0
    tables: int = 0


class BenchmarkedBlockReceiver(trio.abc.ReceiveChannel):
    '''
    Generic class to benchmark receiving blocks from a channel.

    Users need to implement `BenchmarkedBlockReceiver._receiver` async
    iterator, which consumes from the `rchan` passed and must yield dict lists
    which follow the block format.

    '''
    def __init__(
        self,
        rchan: trio.abc.ReceiveChannel,
        sh_args: StateHistoryArgs,
        log_fn: Callable | None = None
    ):
        self._rchan = rchan
        self._aiter = self._iterator()
        self._args = StateHistoryArgs.from_dict(sh_args)
        self.log_fn = log_fn

        self._samples: list[Sample] = []
        self._total_blocks_sampled: int = 0

        self._current_sample = Sample()
        self._last_sample_time = time.time()
        self._last_block_num: int = self._args.start_block_num - 1
        self._last_index: int = -1

        self._avg_time = int(
            self._args.benchmark_sample_time * self._args.benchmark_max_samples
        )

        self._start_time = None
        self._avg_block_speed: int = 0
        self._avg_tx_speed: int = 0
        self._avg_table_speed: int = 0
        self._avg_delta: float = 0

    def _maybe_benchmark(self, batch: list[dict]):
        if self._args.benchmark:
            now = time.time()
            if not self._start_time:
                self._start_time = now

            batch_size = len(batch)
            txs = 0
            tables = 0
            prev_block_num = self._last_block_num
            prev_index = self._last_index
            for block in batch:
                txs += len(block['traces']) if 'traces' in block else 0
                tables += len(block['deltas']) if 'deltas' in block else 0

                if self._args.output_validate:
                    block_num = block['this_block']['block_num']
                    ws_index = block['meta']['ws_index']

                    try:
                        assert block_num == prev_block_num + 1
                        assert ws_index == prev_index + 1
                        prev_block_num = block_num
                        prev_index = ws_index

                    except AssertionError as e:
                        e.add_note(
                            f'sequence mismatch!:\n'
                            f'block nums -> prev: {prev_block_num}, current: {block_num}'
                            f'ws_index -> prev: {prev_index}, current: {ws_index}'
                        )
                        raise

            self._current_sample.blocks += batch_size
            self._current_sample.txs += txs
            self._current_sample.tables += tables

            self._total_blocks_sampled += batch_size

            last_block = batch[-1]
            self._last_block_num = last_block['this_block']['block_num']

            self._last_index = last_block['meta']['ws_index']

            delta = now - self._last_sample_time
            if delta >= self._args.benchmark_sample_time:
                self._current_sample.delta = delta
                self._samples.append(self._current_sample)

                if len(self._samples) > self._args.benchmark_max_samples:
                    self._samples.pop(0)

                self._last_sample_time = now
                self._current_sample = Sample()

                total_blocks = 0
                total_txs = 0
                total_tables = 0
                total_sample_time = 0
                for sample in self._samples:
                    total_blocks += sample.blocks
                    total_txs += sample.txs
                    total_tables += sample.tables
                    total_sample_time += sample.delta

                self._avg_delta = total_sample_time / len(self._samples)
                self._avg_block_speed = total_blocks // total_sample_time
                self._avg_tx_speed = total_txs // total_sample_time
                self._avg_table_speed = total_tables // total_sample_time

                if self.log_fn:
                    self.log_fn(
                        f'[{self._last_block_num:,}] '
                        f'{self._avg_delta:,.2f} sec avg sample delta, '
                        f'{self._avg_block_speed:,} b/s '
                        f'{self._avg_tx_speed:,} tx/s '
                        f'{self._avg_table_speed:,} deltas/s '
                        f'{self._avg_time} sec avg, '
                        f'{self.average_speed_since_start:,} b/s all time'
                    )



    @property
    def average_sample_delta(self) -> float:
        return self._avg_delta

    @property
    def average_speed(self) -> tuple[int, int, int]:
        return self._avg_block_speed, self._avg_tx_speed, self._avg_table_speed

    @property
    def average_speed_since_start(self):
        time_delta = self._last_sample_time - self._start_time

        if time_delta <= 0:
            return 0

        return (
            self._total_blocks_sampled
            //
            time_delta
        )

    async def _receiver(self):
        raise NotImplementedError

    async def _iterator(self):
        _prev_block_num: int = self._args.start_block_num - 1
        _prev_index: int = -1

        async for batch in self._receiver():
            self._maybe_benchmark(batch)

            last_block = batch[-1]

            last_block_num = last_block['this_block']['block_num']

            if self._args.output_convert:
                try:
                    batch = msgspec.convert(batch, type=list[Block])

                except msgspec.ValidationError as e:
                    try:
                        e.add_note(
                            'Msgspec error while decoding batch:\n' +
                            json.dumps(batch, indent=4, cls=LeapJSONEncoder)
                        )

                    except Exception as inner_e:
                        inner_e.add_note('could not decode without type either!')
                        raise inner_e from e

                    raise e

            if self._args.output_batched:
                yield batch

            else:
                for block in batch:
                    yield block

            # maybe we reached end?
            if last_block_num == self._args.end_block_num - 1:
                if self.log_fn:
                    if self._args.output_convert:
                        last_block = batch[-1].to_dict()

                    else:
                        last_block = batch[-1]

                    self.log_fn('last block: \n')
                    self.log_fn(json.dumps(last_block, indent=4))

                break

    async def receive(self):
        return await self._aiter.asend(None)

    async def aclose(self):
        await self._rchan.aclose()
