import time

import trio

from leap.ship import open_state_history


async def test_ship(cleos_bs):
    cleos = cleos_bs

    acc = cleos.new_account()
    receipt = cleos.transfer_token('eosio', acc, '10.0000 TLOS')
    tx_block_num = receipt['processed']['block_num']

    blocks = []
    async with open_state_history(
        endpoint=cleos.ship_endpoint,
        sh_args={
            'start_block_num': tx_block_num,
            'end_block_num': tx_block_num + 1,
        }
    ) as rchan:
        async for block in rchan:
            blocks.append(block)

    block = blocks[0]

    assert block['header']['this_block']['block_num'] == receipt['processed']['block_num']


async def test_manual():
    # http_endpoint = 'http://127.0.0.1:9999'
    ship_endpoint = 'ws://127.0.0.1:29999'

    samples = []
    sample_time = 1.0
    max_samples = 10
    last_stime = time.time()
    block_delta = 0

    with trio.CancelScope() as cscope:
        async with open_state_history(
            endpoint=ship_endpoint,
            sh_args={'start_block_num': 135_764_267},
            # debug_mode=True
        ) as rchan:
            try:
                async for block in rchan:
                    block_num = block['header']['this_block']['block_num']
                    block_delta += 1
                    now = time.time()
                    if now - last_stime >= sample_time:
                        samples.append(block_delta)
                        block_delta = 0
                        last_stime = now
                        if len(samples) > max_samples:
                            samples.pop(0)

                        speed_avg = int(sum(samples) / len(samples))
                        print(f'[{block_num}] speed avg: {speed_avg:,} blocks/sec')


            except KeyboardInterrupt:
                cscope.cancel()

