#!/usr/bin/env python3

import pytest

from leap.sugar import Asset, Checksum160, Int64, LeapOptional, asset_from_str
from leap.tokens import tlos_token


def test_asset(cleos_w_testcontract):
    cleos = cleos_w_testcontract
    ec, res = cleos.push_action(
        'testcontract',
        'checkasset',
        [asset_from_str('1000.0000 TLOS'),Int64(1000 * (10 ** 4))],
        'testcontract'
    )
    assert ec == 0

    ec, res = cleos.push_action(
        'testcontract',
        'checkasset',
        [asset_from_str('-1000.0000 TLOS'), Int64(-1000 * (10 ** 4))],
        'testcontract'
    )
    assert ec == 0

    max_supply = Asset((1 << 62) - 1, tlos_token)
    ec, res = cleos.push_action(
        'testcontract',
        'checkasset',
        [max_supply, Int64(max_supply.amount)],
        'testcontract'
    )
    assert ec == 0

    max_supply = Asset((1 << 62), tlos_token)
    ec, res = cleos.push_action(
        'testcontract',
        'checkasset',
        [max_supply, Int64(max_supply.amount)],
        'testcontract'
    )
    assert ec == 1
    assert res['error']['details'][0]['message'] == 'assertion failure with message: magnitude of asset amount must be less than 2^62'


def test_ripmd160(cleos_w_testcontract):
    cleos = cleos_w_testcontract
    test_hash = 'd80744e16d62c62c5fa2a04b92da3fe6b9efb523'

    ec, _ = cleos.push_action(
        'testcontract',
        'checkripmd',
        [LeapOptional(test_hash, 'rd160'), test_hash],
        'testcontract'
    )
    assert ec == 0
