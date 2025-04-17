import importlib

import tractor
import msgspec

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
    async def _receiver(self):
        async for batch in self._rchan:
            yield msgspec.msgpack.decode(batch)


def import_module_path(path: str):
    module_path, func_name = path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
