import time
from cProfile import Profile
from pstats import SortKey, Stats

import trio
from leap.ship import open_state_history

async def _main():
    start_block_num = 135764267
    ship_endpoint = 'ws://127.0.0.1:29999'

    buckets: list[int] = []
    current_bucket = 0

    last_bucket_time = time.time()

    async for block in open_state_history(
        ship_endpoint,
        start_block_num,
        max_messages_in_flight=20,
        action_whitelist={'_': []},
        delta_whitelist={'_': []}
    ):
        current_bucket += 1
        now = time.time()

        if now - last_bucket_time > 1.0:
            last_bucket_time = now
            buckets.append(current_bucket)
            current_bucket = 0

            speed_avg = int(sum(buckets) / len(buckets))
            print(f'[{block.block_num:,}] {speed_avg} b/s')

            if len(buckets) > 10:
                buckets = buckets[-10:]


if __name__ == '__main__':
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
