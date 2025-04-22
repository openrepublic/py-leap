import trio

from tractor.log import get_console_log

from leap.abis import token
from leap.ship import open_state_history
from leap.ship.structs import StateHistoryArgs, OutputFormats


'''
OS: NixOS 25.05pre773904.698214a32beb (Warbler) x86_64
Kernel: 6.14.0
CPU: AMD Ryzen 9 6900HX with Radeon Graphics (16) @ 4.936GHz
Memory: 27798MiB

average speed on these settings:

 - single node: 10k
 - two nodes: 16k

'''


async def _main():
    _log = get_console_log(level='info')
    start_block_num = 137_000_000
    end_block_num = 175_000_000
    # end_block_num = start_block_num + 10_000_000
    ship_endpoint = 'ws://127.0.0.1:19420'

    sh_args = StateHistoryArgs.from_dict(
        {
            'endpoint': ship_endpoint,
            'start_block_num': start_block_num,
            'end_block_num': end_block_num,
            'fetch_traces': True,
            'fetch_deltas': True,
            'start_contracts': {'eosio.token': token},
            'action_whitelist': {'eosio.token': ['transfer', 'issue', 'retire']},
            'delta_whitelist': {'eosio.token': ['accounts', 'stat']},
            'output_batched': True,
            'output_format': OutputFormats.OPTIMIZED,
            'output_convert': False,
            'output_validate': False,
            'block_meta': False,
            'max_message_size': 10 * 1024 * 1024,
            'max_messages_in_flight': 25_000,
            'benchmark': True,
            'backend_kwargs': {
                'extra_endpoints': [
                    'ws://127.0.0.1:19421'
                ],

                'buf_size': 512 * 1024 * 1024,
                'small_buf_size': 64 * 1024 * 1024,

                'ws_range_stride': 25_000,

                'ws_batch_size': 2000,
                'ws_msgs_per_turn': 1,

                'empty_blocks_batch_size': 1000,
                'empty_blocks_msgs_per_turn': 1,
                'full_blocks_batch_size': 1000,
                'full_blocks_msgs_per_turn': 1000,

                'final_batch_size': 1000,

                'ws_readers': 2,
                'empty_decoders': 3,
                'full_decoders': 1,

                'debug_mode': False,

                'loglevels': {
                    # 'root': 'info',
                    # 'filters': 'info',
                    # 'joiners': 'info',
                    # 'ship_supervisor': 'info',
                    # 'ship_readers': 'info',
                    # 'full_decoders': 'info',
                    # 'empty_decoders': 'info',
                }

            }
        }
    )

    async with open_state_history(
        sh_args=sh_args,
        benchmark_log_fn=print
    ) as block_chan:
        try:
            async for batch in block_chan:
                ...

        except KeyboardInterrupt:
            ...


if __name__ == '__main__':

    try:
        trio.run(_main)

    except KeyboardInterrupt:
        ...
