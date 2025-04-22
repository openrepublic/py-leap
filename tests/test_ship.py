import json
import platform

import trio
import pytest

from leap.ship import (
    open_state_history
)
from leap.sugar import LeapJSONEncoder
from leap.ship.structs import (
    StateHistoryArgs,
    OutputFormats
)
from leap.ship._utils import ResultFilter


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
                    'res_monitor': False,
                    'debug_mode': True
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


def test_action_trace_filtering():

    # action trace bytes that contain an eosio.token transfer
    raw = bytes.fromhex(
        '01002d08fa62b779ef596a7758b1c2563f160f0dfe53b5b8d23aaa206c4a0cb39d0b006'
        '40000000026010000000000000000000000000000000a01010001000000000000ea3055'
        '80a33e82007b8e5ac058b9648beaa988bcc6ac5ac0e99c117530801a49ef81256c36db0'
        '900000000ac59450800000000010000000000ea3055537d5e08000000001e0c00000000'
        '00ea30550000000000ea305500000000221acfa4010000000000ea305500000000a8ed3'
        '2327420bc105280a96a28eba432ddf000082a77c150a491a5c2f538daab0423a4a939dd'
        '7d2858531352fc6c92d05b77a1000000000000000000000000000000000000000000000'
        '0000000000000000000ccdd8f17c23ab20156f8f39397acee024cc081f5dc3d07669dca'
        'c021e4e3e266754a0000000000cf0000000000000000010000000000ea3055880000000'
        '0000000000000010201010000a6823403ea30559ff27ef3877bc18d820b3c1e65a07f6b'
        '341d8f7377772f145b0d1dc0d004c6816d36db090000000049861500000000000100005'
        '819ec8b6f570d2d020000000000040500a6823403ea305500a6823403ea305500000057'
        '2d3ccdcd0100005819ec8b6f5700000000a8ed32323700005819ec8b6f570000000000e'
        'a3055c5ed5b000000000004544c4f5300000016544544503a20496e666c6174696f6e20'
        '6f66667365740010000000000000000000000000010301010000a6823403ea3055efdb2'
        '7f417854af52863a2e365f3b90fd36fdc3d416874bb6b7726d3471a75b07036db090000'
        '00004a86150000000000010000000000ea3055547d5e0800000000040500a6823403ea3'
        '05500a6823403ea3055000000572d3ccdcd010000000000ea305500000000a8ed323257'
        '0000000000ea3055a092432a010c2fe506ae24000000000004544c4f530000003654726'
        '16e7366657220776f726b65722070726f706f73616c20736861726520746f20776f726b'
        '732e646563696465206163636f756e74001000000000000000000000000001040101000'
        '0a6823403ea305534007998d24ef83ac44ac32bc0a00c53bdb744138817d074475efabf'
        '7eaa74797336db09000000004b86150000000000010000000000ea3055577d5e0800000'
        '000040500a6823403ea305500a6823403ea3055000000572d3ccdcd010000000000ea30'
        '5500000000a8ed32324c0000000000ea3055008037f500ea3055bf3f370000000000045'
        '44c4f530000002b5472616e736665722070726f647563657220736861726520746f2070'
        '65722d626c6f636b206275636b6574000a0000000000000000000000000105020100000'
        '05819ec8b6f579ff27ef3877bc18d820b3c1e65a07f6b341d8f7377772f145b0d1dc0d0'
        '04c6816e36db0900000000c4950000000000000100005819ec8b6f570e2d02000000000'
        '0040500005819ec8b6f5700a6823403ea3055000000572d3ccdcd0100005819ec8b6f57'
        '00000000a8ed32323700005819ec8b6f570000000000ea3055c5ed5b000000000004544'
        'c4f5300000016544544503a20496e666c6174696f6e206f666673657400040000000000'
        '0000000000000001060201000000000000ea30559ff27ef3877bc18d820b3c1e65a07f6'
        'b341d8f7377772f145b0d1dc0d004c6816f36db0900000000ad59450800000000010000'
        '5819ec8b6f570f2d02000000000004050000000000ea305500a6823403ea30550000005'
        '72d3ccdcd0100005819ec8b6f5700000000a8ed32323700005819ec8b6f570000000000'
        'ea3055c5ed5b000000000004544c4f5300000016544544503a20496e666c6174696f6e2'
        '06f6666736574000300000000000000000000000001070301000000000000ea3055efdb'
        '27f417854af52863a2e365f3b90fd36fdc3d416874bb6b7726d3471a75b07136db09000'
        '00000ae59450800000000010000000000ea3055557d5e080000000004050000000000ea'
        '305500a6823403ea3055000000572d3ccdcd010000000000ea305500000000a8ed32325'
        '70000000000ea3055a092432a010c2fe506ae24000000000004544c4f53000000365472'
        '616e7366657220776f726b65722070726f706f73616c20736861726520746f20776f726'
        'b732e646563696465206163636f756e7400030000000000000000000000000108030100'
        'a092432a010c2fe5efdb27f417854af52863a2e365f3b90fd36fdc3d416874bb6b7726d'
        '3471a75b07236db0900000000f279000000000000010000000000ea3055567d5e080000'
        '00000405a092432a010c2fe500a6823403ea3055000000572d3ccdcd010000000000ea3'
        '05500000000a8ed3232570000000000ea3055a092432a010c2fe506ae24000000000004'
        '544c4f53000000365472616e7366657220776f726b65722070726f706f73616c2073686'
        '1726520746f20776f726b732e646563696465206163636f756e74000f00000000000000'
        '000000000001090401000000000000ea305534007998d24ef83ac44ac32bc0a00c53bdb'
        '744138817d074475efabf7eaa74797436db0900000000af594508000000000100000000'
        '00ea3055587d5e080000000004050000000000ea305500a6823403ea3055000000572d3'
        'ccdcd010000000000ea305500000000a8ed32324c0000000000ea3055008037f500ea30'
        '55bf3f37000000000004544c4f530000002b5472616e736665722070726f64756365722'
        '0736861726520746f207065722d626c6f636b206275636b657400020000000000000000'
        '00000000010a040100008037f500ea305534007998d24ef83ac44ac32bc0a00c53bdb74'
        '4138817d074475efabf7eaa74797536db090000000023c3000000000000010000000000'
        'ea3055597d5e08000000000405008037f500ea305500a6823403ea3055000000572d3cc'
        'dcd010000000000ea305500000000a8ed32324c0000000000ea3055008037f500ea3055'
        'bf3f37000000000004544c4f530000002b5472616e736665722070726f6475636572207'
        '36861726520746f207065722d626c6f636b206275636b65740001000000000000000000'
        '00000000000000010000000000000000000000000000000000'
    )

    sh_args = StateHistoryArgs.from_dict({
        'endpoint': '',
        'start_block_num': 1,
        'action_whitelist': {'eosio.token': ['transfer', 'issue', 'retire']},
    })

    rfilter = ResultFilter(sh_args)

    result = {
        'traces': raw
    }

    assert rfilter.is_relevant(result)
