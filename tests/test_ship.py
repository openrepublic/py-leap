import trio
import tractor
from leap.ship import open_state_history
import logging

async def test_ship(cleos_bs):
    cleos = cleos_bs

    acc = cleos.new_account()
    receipt = cleos.transfer_token('eosio', acc, '10.0000 TLOS')
    tx_block_num = receipt['processed']['block_num']

    blocks = []
    async with open_state_history(
        endpoint=cleos.ship_endpoint,
        start_block_num=tx_block_num,
        end_block_num=tx_block_num + 1000,
    ) as stream:
        try:
            async for block in stream:
                # logging.info(block)
                blocks.append(block)
                # await trio.sleep(0)
        except BaseException:
            log = tractor.log.get_console_log()
            log.exception('yo top leve test is fucked')
            raise

    block = blocks[0]

    assert block.block_num == receipt['processed']['block_num']
