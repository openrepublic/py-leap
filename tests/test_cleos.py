

def test_wait_blocks(cleos):

    start = cleos.head_block_num

    wait = 10

    cleos.wait_blocks(wait)

    assert (cleos.head_block_num - start) - wait <= 1
