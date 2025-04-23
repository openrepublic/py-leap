import trio

from leap.protocol import Asset, Symbol
from leap.tokens import tlos_token


def test_abi(cleos_w_testcontract):
    assert cleos_w_testcontract.get_abi('testcontract'), "failed to fetch abi"

def test_asset(cleos_w_testcontract):
    cleos = cleos_w_testcontract
    cleos.push_action(
        'testcontract',
        'checkasset',
        ['1000.0000 TLOS', 1000 * (10 ** 4)],
        'testcontract'
    )

    cleos.push_action(
        'testcontract',
        'checkasset',
        ['-1000.0000 TLOS', -1000 * (10 ** 4)],
        'testcontract'
    )

    max_supply = Asset((1 << 62) - 1, Symbol.from_str(tlos_token))
    cleos.push_action(
        'testcontract',
        'checkasset',
        [max_supply, max_supply.amount],
        'testcontract'
    )

    # since using antelope_rs.Asset we can't even build an invalid 1 << 62 amount asset
    # with pytest.raises(ValueError) as err:
    #     max_supply = Asset((1 << 62), Symbol.from_str(tlos_token))
    #     cleos.push_action(
    #         'testcontract',
    #         'checkasset',
    #         [max_supply, max_supply.amount],
    #         'testcontract',
    #         retries=1
    #     )

    # assert 'asset amount must be less than 2^62' in repr(err.value)

    cleos.wait_blocks(2)

    trio.run(
        cleos.a_push_action,
        'testcontract',
        'checkasset',
        ['1000.0000 TLOS', 1000 * (10 ** 4)],
        'testcontract'
    )


def test_ripmd160(cleos_w_testcontract):
    cleos = cleos_w_testcontract
    test_hash = 'd80744e16d62c62c5fa2a04b92da3fe6b9efb523'

    cleos.push_action(
        'testcontract',
        'checkripmd',
        [test_hash, test_hash],
        'testcontract'
    )


def test_extended_asset(cleos_w_testcontract):
    cleos = cleos_w_testcontract
    cleos.push_action(
        'testcontract',
        'checkexasset',
        ['1000.000000000 PUSDT@swap.libre', 'swap.libre', 1000 * (10 ** 9)],
        'testcontract'
    )


def test_deploy_update(cleos_w_testcontract):
    cleos = cleos_w_testcontract

    abi = cleos.get_abi('testcontract')
    assert abi

    result = cleos.push_action(
        'testcontract',
        'testversion',
        [420],
        'testcontract'
    )
    output = result['processed']['action_traces'][0]['console']
    assert output == 'v1\n420'

    cleos.deploy_contract_from_path(
        'testcontract',
        'tests/contracts/testcontract/v2',
        contract_name='testcontract',
        create_account=False
    )

    cleos.wait_blocks(1)

    abi = cleos.get_abi('testcontract')
    assert abi

    result = cleos.push_action(
        'testcontract',
        'testversion',
        ['chungus', 420],
        'testcontract'
    )
    output = result['processed']['action_traces'][0]['console']
    assert output == 'v2\n420\nchungus'
