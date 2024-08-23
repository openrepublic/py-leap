#!/usr/bin/env python3

from contextlib import aclosing
from leap.ship import open_state_history


async def test_ship():
    '''Requires testcontainer-evm to be running:
    docker run -itd --network=host --rm ghcr.io/telosnetwork/testcontainer-nodeos-evm:v0.1.4
    '''
    stream = open_state_history(
        'ws://127.0.0.1:18999',
        2,
        end_block_num=60
    )

    async with aclosing(stream):
        async for block in stream:
            block_num = block['this_block']['block_num']
            print(block_num)

