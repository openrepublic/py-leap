import json

from leap.ship import open_state_history
from leap.sugar import LeapJSONEncoder


async def test_ship(cleos_bs):
    cleos = cleos_bs

    acc = cleos.new_account()
    receipt = cleos.transfer_token('eosio', acc, '10.0000 TLOS')
    tx_block_num = receipt['processed']['block_num']

    token_abi = cleos.get_abi('eosio.token', encode=True)

    blocks = []
    async with open_state_history(
        sh_args={
            'endpoint': cleos.ship_endpoint,
            'start_block_num': tx_block_num,
            'end_block_num': tx_block_num + 1,
            'fetch_traces': True,
            'fetch_deltas': True,
            'start_contracts': {
                'eosio.token': token_abi
            },
            'action_whitelist': {
                'eosio.token': ['transfer']
            },
            'delta_whitelist': {
                'eosio.token': ['accounts']
            },
        }
    ) as rchan:
        async for block in rchan:
            print(json.dumps(block.as_dict(), indent=4, cls=LeapJSONEncoder))
            blocks.append(block)

    block = blocks[0]

    assert block.this_block.block_num == receipt['processed']['block_num']
