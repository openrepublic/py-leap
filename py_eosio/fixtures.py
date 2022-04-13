#!/usr/bin/env python3

import docker
import pytest

from .cleos import CLEOS
from .sugar import get_container


DEFAULT_NODEOS_REPO = 'guilledk/py-eosio'
DEFAULT_NODEOS_IMAGE = 'eosio-2.0.13'


@pytest.fixture(scope='session')
def cleos_full_boot():
    dclient = docker.from_env()
    vtestnet = get_container(
        dclient,
        f'{DEFAULT_NODEOS_REPO}:{DEFAULT_NODEOS_IMAGE}',
        force_unique=True,
        detach=True,
        network='host')

    try:
        cleos = CLEOS(dclient, vtestnet)
        cleos.start_keosd()

        cleos.start_nodeos_from_config(
            '/root/nodeos/config.ini',
            data_dir='/root/nodeos/data',
            state_plugin=True)

        cleos.wait_blocks(1)
        cleos.setup_wallet('5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3')
        cleos.boot_sequence()

        yield cleos

    finally:
        vtestnet.stop()
        vtestnet.remove()
