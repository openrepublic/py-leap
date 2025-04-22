import importlib
from heapq import (
    heappush,
    heappop
)
from typing import Callable

import trio
import tractor
import msgspec

from ..structs import StateHistoryArgs
from .._benchmark import BenchmarkedBlockReceiver


get_logger = tractor.log.get_logger
get_console_log = tractor.log.get_console_log
# _proj_name: str = 'py-leap'
#
#
# def get_logger(
#     name: str = None,
# 
# ) -> logging.Logger:
#     '''
#     Return the package log or a sub-log for `name` if provided.
# 
#     '''
#     return tractor.log.get_logger(
#         name=name,
#         _root_name=_proj_name,
#     )
# 
# def get_console_log(
#     level: str | None = None,
#     name: str | None = None,
# 
# ) -> logging.Logger:
#     '''
#     Get the package logger and enable a handler which writes to stderr.
# 
#     Yeah yeah, i know we can use ``DictConfig``. You do it...
# 
#     '''
#     return tractor.log.get_console_log(
#         level,
#         name=name,
#         _root_name=_proj_name,
#     )  # our root logger


class BlockReceiver(BenchmarkedBlockReceiver):
    def __init__(
        self,
        rchan: trio.abc.ReceiveChannel,
        sh_args: StateHistoryArgs,
        log_fn: Callable | None = None
    ):
        super().__init__(rchan, sh_args, log_fn=log_fn)

        self.next_index = 0
        self.pq: list[tuple[int, dict]] = []

    def push_all(self, blocks: list[dict]):
        for block in blocks:
            ws_index = block['meta']['ws_index']
            heappush(
                self.pq,
                (
                    ws_index,
                    block
                )
            )
            # print(f'got {ws_index} need {self.next_index}')

    def can_pop_next(self) -> bool:
        '''
        Predicate to check if we have next in order block on pqueue.

        '''
        return (
            len(self.pq) > 0
            and
            self.pq[0][0] == self.next_index
        )

    def pop_next(self) -> dict:
        '''
        Pop first block from pqueue.

        '''
        _, msg = heappop(self.pq)
        self.next_index += 1
        return msg

    async def _receiver(self):
        async for blocks in self._rchan:
            self.push_all(blocks)

            block_batch = []
            while self.can_pop_next():
                block_batch.append(self.pop_next())

            # no ready blocks
            if len(block_batch) == 0:
                continue

            yield block_batch


def import_module_path(path: str):
    module_path, func_name = path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
