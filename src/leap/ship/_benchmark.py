import time

import trio

from leap.ship.structs import (
    StateHistoryArgs,
)


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

        self._samples: list[int] = []
        self._current_sample: int = 0
        self._last_sample_time = time.time()
        self._top_speed: int = 0
        self._avg_speed: int = 0

    def _maybe_benchmark(self, batch_size: int):
        if self._args.benchmark:
            self._current_sample += batch_size
            now = time.time()

            if now - self._last_sample_time > self._args.benchmark_sample_time:
                self._last_sample_time = now
                self._samples.append(self._current_sample)
                self._current_sample = 0

                self._avg_speed = int(sum(self._samples) / len(self._samples))
                self._top_speed = max(self._samples)

                if len(self._samples) > self._args.benchmark_max_samples:
                    self._samples.pop(0)

    @property
    def average_speed(self):
        if not self._args.benchmark:
            raise ValueError(
                'StateHistoryArgs.benchmark must be set to True to calculate speed'
            )

        return self._avg_speed

    @property
    def top_speed(self):
        if not self._args.benchmark:
            raise ValueError(
                'StateHistoryArgs.benchmark must be set to True to calculate speed'
            )

        return self._top_speed

    async def _iterator(self):
        raise NotImplementedError

    async def receive(self):
        return await self._aiter.asend(None)

    async def aclose(self):
        await self._rchan.aclose()
