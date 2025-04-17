import time

import trio

from leap.ship.structs import (
    Struct,
    StateHistoryArgs,
)


class Sample(Struct):
    delta: float = 0
    blocks: int = 0
    txs: int = 0


class BenchmarkedBlockReceiver(trio.abc.ReceiveChannel):
    '''
    Decode a stream of msgspack encoded `Block` structs

    '''
    def __init__(
        self,
        rchan: trio.abc.ReceiveChannel,
        sh_args: StateHistoryArgs
    ):
        self._rchan = rchan
        self._aiter = self._iterator()
        self._args = StateHistoryArgs.from_dict(sh_args)

        self._samples: list[Sample] = []
        self._total_blocks_sampled: int = 0

        self._current_sample = Sample()
        self._last_sample_time = time.time()

        self._start_time = None
        self._avg_block_speed: int = 0
        self._avg_tx_speed: int = 0
        self._avg_delta: float = 0

    def _maybe_benchmark(self, batch_size: int, txs: int):
        if self._args.benchmark:
            now = time.time()
            if not self._start_time:
                self._start_time = now

            self._current_sample.blocks += batch_size
            self._current_sample.txs += txs
            self._total_blocks_sampled += batch_size

            delta = now - self._last_sample_time
            if delta >= self._args.benchmark_sample_time:
                self._current_sample.delta = delta
                self._samples.append(self._current_sample)
                self._last_sample_time = now
                self._current_sample = Sample()

                total_blocks = 0
                total_txs = 0
                total_sample_time = 0
                for sample in self._samples:
                    total_blocks += sample.blocks
                    total_txs += sample.txs
                    total_sample_time += sample.delta

                self._avg_delta = total_sample_time / len(self._samples)
                self._avg_block_speed = total_blocks // total_sample_time
                self._avg_tx_speed = total_txs // total_sample_time

                if len(self._samples) > self._args.benchmark_max_samples:
                    self._samples.pop(0)

    @property
    def average_sample_delta(self) -> float:
        return self._avg_delta

    @property
    def average_speed(self) -> tuple[int, int]:
        return self._avg_block_speed, self._avg_tx_speed

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

    async def _iterator(self):
        raise NotImplementedError

    async def receive(self):
        return await self._aiter.asend(None)

    async def aclose(self):
        await self._rchan.aclose()
