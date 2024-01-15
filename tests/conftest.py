#!/usr/bin/env python3

import os

from leap.sugar import is_module_installed


if 'MANAGED_LEAP' in os.environ:
    if not is_module_installed('docker'):
        raise ImportError(
            f'MANAGED_LEAP present but package docker not installed, '
            'try reinstalling like this \"poetry install --with=nodemngr\"'
        )

    import pytest
    from leap.fixtures import bootstrap_test_nodeos


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
