#!/usr/bin/env python3

import pytest

from leap.protocol import Asset, Symbol
from leap.tokens import tlos_token
from leap.errors import TransactionPushError


async def test_asset(cleos_w_testcontract):
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

    with pytest.raises(TransactionPushError) as err:
        max_supply = Asset((1 << 62), Symbol.from_str(tlos_token))
        cleos.push_action(
            'testcontract',
            'checkasset',
            [max_supply, max_supply.amount],
            'testcontract',
            retries=1
        )

    assert 'assertion failure with message: magnitude of asset amount must be less than 2^62' in str(err)

    cleos.wait_blocks(2)

    await cleos.a_push_action(
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

    cleos.push_action(
        'testcontract',
        'checkexasset',
        [
            {'quantity': '420.000000000 PUSDT', 'contract': 'swap.libre'},
            'swap.libre',
            420 * (10 ** 9)
        ],
        'testcontract'
    )
