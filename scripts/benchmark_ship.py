import time

import trio

from leap import CLEOS
from leap.ship import open_state_history

async def _main():
    start_block_num = 143_000_000
    http_endpoint = 'https://testnet.telos.net'
    ship_endpoint = 'ws://127.0.0.1:19420'

    cleos = CLEOS(endpoint=http_endpoint)

    token_abi = cleos.get_abi('eosio.token')

    buckets: list[int] = []
    current_bucket = 0

    last_bucket_time = time.time()

    async with open_state_history(
        sh_args={
            'endpoint': ship_endpoint,
            'start_block_num': start_block_num,
            'fetch_traces': True,
            'fetch_deltas': True,
            'start_contracts': {'eosio.token': token_abi},
            'action_whitelist': {'eosio.token': ['transfer']},
            'delta_whitelist': {'eosio.token': ['accounts']}
        }
    ) as block_chan:
        async for block in block_chan:
            current_bucket += 1
            now = time.time()

            if now - last_bucket_time > 1.0:
                last_bucket_time = now
                buckets.append(current_bucket)
                current_bucket = 0

                block_num = block.this_block.block_num
                speed_avg = int(sum(buckets) / len(buckets))
                print(f'[{block_num:,}] {speed_avg} b/s')

                if len(buckets) > 10:
                    buckets = buckets[-10:]


if __name__ == '__main__':

#    from cProfile import Profile
#    from pstats import SortKey, Stats
#    with Profile() as profile:
    try:
        trio.run(_main)

    except KeyboardInterrupt:
        ...

#        (
#            Stats(profile)
#            .strip_dirs()
#            .sort_stats(SortKey.CALLS)
#            .print_stats()
#        )
