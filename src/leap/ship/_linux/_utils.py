import json
import logging
import importlib

import tractor
import msgspec
from tractor.ipc._ringbuf import RingBufferReceiveChannel

from leap.sugar import LeapJSONEncoder
from ..structs import (
    StateHistoryArgs,
    Block
)
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
        rchan: RingBufferReceiveChannel,
        sh_args: StateHistoryArgs
    ):
        super().__init__(rchan, sh_args)
        self._near_end = False

    async def _iterator(self):
        async for blocks in self._rchan:
            blocks = msgspec.msgpack.decode(blocks)

            txs = sum((
                len(block['traces'])
                for block in blocks
            ))

            self._maybe_benchmark(len(blocks), txs)

            last_block_num = blocks[-1]['this_block']['block_num']

            # maybe near end
            if (
                not self._near_end
                and
                self._args.end_block_num - last_block_num
                <=
                self._args.max_messages_in_flight * 3
            ):
                self._near_end = True

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

            # maybe we reached end?
            if last_block_num == self._args.end_block_num - 1:
                break


def import_module_path(path: str):
    module_path, func_name = path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
