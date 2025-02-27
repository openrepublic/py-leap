from leap.ship import open_state_history

async def test_ship(cleos_bs):
    cleos = cleos_bs

    acc = cleos.new_account()
    receipt = cleos.transfer_token('eosio', acc, '10.0000 TLOS')
    tx_block_num = receipt['processed']['block_num']

    blocks = []
    async for block in open_state_history(
        cleos.ship_endpoint,
        tx_block_num, end_block_num=tx_block_num,
    ):
        blocks.append(block)

    block = blocks[0]

    assert block.block_num == receipt['processed']['block_num']
