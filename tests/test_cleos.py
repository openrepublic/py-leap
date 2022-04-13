#!/usr/bin/env python3

from py_eosio.fixtures import cleos_full_boot as cleos


def test_cleos_get_info(cleos):
    assert 'server_version' in cleos.get_info()
