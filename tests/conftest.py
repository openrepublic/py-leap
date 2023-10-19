#!/usr/bin/env python3

import pytest
from leap.fixtures import cleos, bootstrap_test_nodeos


@pytest.fixture(scope='module')
def cleos_w_token(request, tmp_path_factory):
    request.applymarker(
        pytest.mark.contracts(
            **{'eosio.token': (
                'tests/contracts/eosio.token/eosio.token.wasm',
                'tests/contracts/eosio.token/eosio.token.abi')}))

    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos


@pytest.fixture(scope='module')
def cleos_w_testcontract(request, tmp_path_factory):
    request.applymarker(
        pytest.mark.contracts(
            testcontract=(
                'tests/contracts/testcontract/testcontract.wasm',
                'tests/contracts/testcontract/testcontract.abi'
            )
        )
    )

    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos
