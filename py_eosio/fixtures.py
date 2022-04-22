#!/usr/bin/env python3

import json

import docker
import pytest

from .cleos import CLEOS
from .sugar import (
    get_container,
    random_eosio_name
)


DEFAULT_NODEOS_REPO = 'guilledk/py-eosio'
DEFAULT_NODEOS_IMAGE = 'eosio-2.0.13'


@pytest.fixture(scope='session')
def single_node_chain():
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

        # prod_count = 21 
        # cleos.producers = [
        #     random_eosio_name()
        #     for i in range(prod_count)
        # ]
        # cleos.producers.sort()
        # cleos.producer_keys = cleos.create_key_pairs(prod_count)

        # private_keys = [pkeys[0] for pkeys in cleos.producer_keys]
        # public_keys = [pkeys[1] for pkeys in cleos.producer_keys]

        # cleos.import_keys(private_keys)

        # cleos.create_accounts_staked(
        #     'eosio',
        #     cleos.producers,
        #     public_keys)

        # results = cleos.parallel_push_action((
        #     ('eosio' for i in range(prod_count)),
        #     ('regproducer' for i in range(prod_count)),
        #     ([cleos.producers[i], public_keys[i], '', 0]
        #         for i in range(prod_count)),
        #     (f'{cleos.producers[i]}@active' for i in range(prod_count))
        # ))

        # for res in results:
        #     assert res[0] == 0

        # shdl = cleos.get_schedule()
        # cleos.logger.info(shdl)    

        # rexed = '100000.0000 TLOS'
        # ec, _ = cleos.rex_deposit('eosio', rexed)
        # assert ec == 0
        # ec, _ = cleos.rex_buy('eosio', rexed)
        # assert ec == 0
        # ec, _ = cleos.vote_producers(
        #     'eosio', '', cleos.producers)
        # assert ec == 0
        # 
        # cleos.wait_blocks(600, sleep_time=30)

        # producers = cleos.get_producers()
        # cleos.logger.info(json.dumps(producers, indent=4))

        # shdl = cleos.get_schedule()
        # cleos.logger.info(shdl)    

        yield cleos

    finally:
        vtestnet.stop()
        vtestnet.remove()
