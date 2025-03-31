
import trio
import tractor

from tractor.log import get_console_log

from leap.ship.structs import StateHistoryArgs
from leap.indexer.parquet import parquet_indexer



async def _main():
    _log = get_console_log(level='info')
    start_block_num = 135_764_267
    end_block_num = start_block_num + 1000
    ship_endpoint = 'ws://127.0.0.1:29999'

    sh_args = StateHistoryArgs.from_dict(
        {
            'endpoint': ship_endpoint,
            'start_block_num': start_block_num,
            'end_block_num': end_block_num,
        }
    )

    data_dir = 'data'

    async with tractor.open_nursery() as an:

        portal = await an.start_actor(
            'indexer',
            enable_modules=['leap.indexer.parquet']
        )

        async with portal.open_context(
            parquet_indexer,
            sh_args=sh_args.to_dict(),
            data_dir=data_dir
        ) as (ctx, _sent):
            await ctx.result()


if __name__ == '__main__':

    try:
        trio.run(_main)

    except KeyboardInterrupt:
        ...
