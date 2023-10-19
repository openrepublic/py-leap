#!/usr/bin/env python3

import json

from leap.sugar import Asset, Abi, Int64, asset_from_str
from leap.tokens import tlos_token


def test_asset(cleos):
    with open('tests/contracts/testcontract/testcontract.wasm', 'rb') as wasm_file:
        wasm = wasm_file.read()

    with open('tests/contracts/testcontract/testcontract.abi', 'r') as abi_file:
        abi = Abi(json.loads(abi_file.read()))

    cleos.deploy_contract(
        'testcontract',
        wasm, abi, verify_hash=False)

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
