import json
import platform

import trio
import pytest

from leap.ship import (
    open_state_history
)
from leap.sugar import LeapJSONEncoder
from leap.ship.structs import OutputFormats


@pytest.mark.parametrize(
    'fetch_block,fetch_traces,fetch_deltas,start_contracts,action_whitelist,delta_whitelist,decode_meta,output_format,backend',
    [
        (False, False, False, {}, None, None, False, OutputFormats.OPTIMIZED, 'generic'),

        (True, True, True, {}, None, None, True, OutputFormats.OPTIMIZED, 'generic'),

        (True, True, True, {}, None, None, True, OutputFormats.STANDARD, 'generic'),

        (False, False, False, {}, None, None, False, OutputFormats.OPTIMIZED, 'linux'),

        (True, True, True, {}, None, None, True, OutputFormats.OPTIMIZED, 'linux'),

        (True, True, True, {}, None, None, True, OutputFormats.STANDARD, 'linux'),
    ],
    ids=[
        'generic_headers',
        'generic_opt',
        'generic_std',
        'linux_headers',
        'linux_opt',
        'linux_std'
    ]
)
def test_one_tx(
    cleos_bs,
    fetch_block: bool,
    fetch_traces: bool,
    fetch_deltas: bool,
    start_contracts: dict,
    action_whitelist: dict,
    delta_whitelist: dict,
    decode_meta: bool,
    output_format: OutputFormats,
    backend: str
):
    if platform.system() != 'Linux' and backend != 'generic':
        pytest.skip('Linux only')

    cleos = cleos_bs

    acc = cleos.new_account()
    receipt = cleos.transfer_token('eosio', acc, '10.0000 TLOS')
    tx_block_num = receipt['processed']['block_num']

    blocks = []
    async def main():
        async with open_state_history(
            sh_args={
                'endpoint': cleos.ship_endpoint,
                'start_block_num': tx_block_num,
                'end_block_num': tx_block_num + 1,
                'fetch_block': fetch_block,
                'fetch_traces': fetch_traces,
                'fetch_deltas': fetch_deltas,
                'start_contracts': start_contracts,
                'action_whitelist': action_whitelist,
                'delta_whitelist': delta_whitelist,
                'decode_meta': decode_meta,
                'output_format': output_format,
                'backend': backend,
                'backend_kwargs': {
                    # 'debug_mode': True
                }
            }
        ) as rchan:
            async for block in rchan:
                # breakpoint()
                print(json.dumps(
                    block.to_dict(),
                    indent=4,
                    cls=LeapJSONEncoder
                ))
                blocks.append(block)

    trio.run(main)

    block = blocks[0]

    assert block.this_block.block_num == receipt['processed']['block_num']
