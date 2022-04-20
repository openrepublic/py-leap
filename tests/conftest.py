#!/usr/bin/env python3

import pytest

from py_eosio.fixtures import single_node_chain as cleos


@pytest.fixture(scope='session')
def msig_contract(cleos):
    cleos.deploy_contract_from_host(
        'testcontract',
        'tests/contracts/testcontract',
    )
    yield cleos
