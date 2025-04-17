import time
import json

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

average speed on these settings:

 - single node: 7.7-7.9k
 - two nodes: 8.8-8.9k

'''


async def _main():
    _log = get_console_log(level='info')
    start_block_num = 137_000_000
    end_block_num = start_block_num + 200_000
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
            'output_convert': True,
            'max_message_size': 10 * 1024 * 1024,
            'max_messages_in_flight': 25_000,
            'benchmark': True,
            'backend_kwargs': {
                'extra_endpoints': [
                    'ws://127.0.0.1:19421'
                ],

                'buf_size': 512 * 1024 * 1024,

                'ws_range_stride': 25_000,
                'ws_batch_size': 1000,
                'ws_msgs_per_turn': 1,

                'stage_0_batch_size': 250,
                'stage_0_msgs_per_turn': 250,

                'stage_1_batch_size': 100,

                'final_batch_size': 10,

                'ws_readers': 2,
                'decoders': 1,
                'stage_ratio': 5,

                'debug_mode': False,

                'loglevels': {
                    # 'root': 'info',
                    # 'joiner': 'info',
                    # 'ship_supervisor': 'info',
                    # 'ship_reader': 'info',
                    # 'decoder_stage_0': 'info',
                    # 'decoder_stage_1': 'info',
                }

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
            batch = None
            block_num = None
            prev_block_num = start_block_num - 1
            prev_index = -1
            async for batch in block_chan:
                for block in batch:
                    if sh_args.output_convert:
                        block_num = block.this_block.block_num
                        index = block.ws_index
                    else:
                        block_num = block['this_block']['block_num']
                        index = block['ws_index']

                    try:
                        assert block_num == prev_block_num + 1
                        prev_block_num = block_num

                        assert index == prev_index + 1
                        prev_index = index

                    except AssertionError as e:
                        e.add_note(f'block nums: prev: {prev_block_num}, : {block_num}')
                        e.add_note(f'index: prev: {prev_index}, : {index}')
                        raise

                now = time.time()
                if now - last_speed_log_time >= sh_args.benchmark_sample_time:
                    last_speed_log_time = now

                    avg_block_speed, avg_tx_speed = block_chan.average_speed

                    print(
                        f'[{block_num:,}] '
                        f'{block_chan.average_sample_delta:,.2f} sec avg sample delta, '
                        f'{avg_block_speed:,} b/s '
                        f'{avg_tx_speed:,} tx/s '
                        f'{avg_time} sec avg, '
                        f'{block_chan.average_speed_since_start:,} b/s all time'
                    )

            if batch:
                if sh_args.output_convert:
                    last_block = batch[-1].to_dict()

                else:
                    last_block = batch[-1]

                print('last block: \n')
                print(json.dumps(last_block, indent=4))

                assert block_num == sh_args.end_block_num - 1

        except KeyboardInterrupt:
            ...


if __name__ == '__main__':

    try:
        trio.run(_main)

    except KeyboardInterrupt:
        ...
