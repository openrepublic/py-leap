import time

import trio

from tractor.log import get_console_log

from leap import CLEOS
from leap.ship import open_state_history
from leap.ship.structs import StateHistoryArgs, OutputFormats


'''
OS: NixOS 25.05pre773904.698214a32beb (Warbler) x86_64
Kernel: 6.14.0
CPU: AMD Ryzen 9 6900HX with Radeon Graphics (16) @ 4.936GHz
GPU: AMD ATI Radeon 680M
Memory: 27798MiB

average speed on these settings: 7.9k
'''


async def _main():
    _log = get_console_log(level='info')
    start_block_num = 137_000_000
    end_block_num = start_block_num + 1_000_000
    http_endpoint = 'https://testnet.telos.net'
    ship_endpoint = 'ws://127.0.0.1:19420'

    cleos = CLEOS(endpoint=http_endpoint)

    token_abi = cleos.get_abi('eosio.token', encode=True)

    last_speed_log_time = time.time()

    sh_args = StateHistoryArgs.from_dict(
        {
            'endpoint': ship_endpoint,
            'start_block_num': start_block_num,
            'end_block_num': end_block_num,
            'fetch_traces': True,
            'fetch_deltas': True,
            'start_contracts': {'eosio.token': token_abi},
            'action_whitelist': {'eosio.token': ['*']},
            'delta_whitelist': {'eosio.token': ['*']},
            'output_batched': True,
            'output_format': OutputFormats.OPTIMIZED,
            'output_convert': False,
            'max_message_size': 10 * 1024 * 1024,
            'max_messages_in_flight': 24_000,
            'benchmark': True,
            'backend_kwargs': {
                'buf_size': 512 * 1024 * 1024,

                'ws_batch_size': 2000,
                'ws_msgs_per_turn': 1,

                'stage_0_batch_size': 1000,
                'stage_0_msgs_per_turn': 1000,

                'stage_1_batch_size': 100,

                'decoders': 1,
                'stage_ratio': 5,

                'resmon': {
                    'recommend_batch_size_updates': False
                },

                'debug_mode': False,
                'loglevel': 'warning'
            }
        }
    )

    async with open_state_history(
        sh_args=sh_args
    ) as block_chan:
        try:
            avg_time = int(
                sh_args.benchmark_sample_time * sh_args.benchmark_max_samples
            )
            async for batch in block_chan:
                block = batch[-1]
                now = time.time()
                if now - last_speed_log_time >= sh_args.benchmark_sample_time:
                    last_speed_log_time = now

                    if sh_args.output_convert:
                        block_num = block.this_block.block_num
                    else:
                        block_num = block['this_block']['block_num']

                    print(
                        f'[{block_num:,}] '
                        f'{block_chan.average_sample_delta:,.2f} sec avg sample delta, '
                        f'{block_chan.average_speed:,} b/s {avg_time} sec avg, '
                        f'{block_chan.average_speed_since_start:,} b/s all time'
                    )

        except KeyboardInterrupt:
            ...


if __name__ == '__main__':

    try:
        trio.run(_main)

    except KeyboardInterrupt:
        ...
