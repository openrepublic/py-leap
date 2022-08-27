#!/usr/bin/env python3

import time
import json
import string
import logging

import docker
import pytest
import requests

from .cleos import CLEOS, default_nodeos_image
from .sugar import (
    get_container,
    get_free_port,
    random_eosio_name
)


@pytest.fixture(scope='session')
def single_node_chain():
    dclient = docker.from_env()
    vtestnet = get_container(
        dclient,
        default_nodeos_image(),
        force_unique=True,
        detach=True,
        network='host')

    try:
        cleos = CLEOS(dclient, vtestnet)

        cleos.start_keosd()

        cleos.start_nodeos_from_config(
            '/root/nodeos/config.ini',
            data_dir='/root/nodeos/data',
            genesis='/root/nodeos/genesis/local.json',
            state_plugin=True)

        time.sleep(0.5)

        cleos.setup_wallet('5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL')
        cleos.wait_blocks(1)
        cleos.boot_sequence()

        yield cleos

    finally:
        vtestnet.stop()
        vtestnet.remove()




@pytest.fixture(scope='session')
def multi_node_chain():

    logger = logging.getLogger('cleos')

    dclient = docker.from_env()
    node_amount = 3

    container = dclient.containers.run(
        default_nodeos_image(),
        detach=True,
        network='host',
        remove=True)
    logger.info('Container launched')

    try:
        cleos = CLEOS(dclient, container, logger=logger)

        keosd_port = get_free_port()

        cleos.start_keosd()

        logger.info('Nodeos start')
        cleos.start_nodeos_from_config(
            '/root/nodeos/config.ini',
            data_dir='/root/nodeos/data',
            state_plugin=True)

        time.sleep(0.5)

        cleos.setup_wallet('5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3')
        cleos.boot_sequence()

        # create producer accounts
        producers = [
            f'producer{string.ascii_lowercase[i]}'
            for i in range(node_amount)
        ]

        for producer in producers:
            cleos.create_account_staked('eosio', producer)
            ec, _ = cleos.register_producer(producer)
            assert ec == 0

        # vote for producers
        voter = cleos.new_account()
        cleos.give_token(voter, '400000000.0000 TLOS')
        cleos.give_token('exrsrv.tf', '400000000.0000 TLOS')

        ec, _ = cleos.delegate_bandwidth(
            voter, voter, '100000000.0000 TLOS', '100000000.0000 TLOS')
        assert ec == 0

        ec, _ = cleos.vote_producers(
            voter, '', producers)

        assert ec == 0

        # pause block production on 'eosio' producer
        cleos.pause_block_production()

        logger.info(f'Is block production paused: {cleos.is_block_production_paused()}') 

        # init producer nodes
        apis = []
        ports = []
        for producer in producers:
            http_port = get_free_port()
            p2p_port = get_free_port()

            api = CLEOS(
                dclient, container, url=f'http://127.0.0.1:{http_port}')

            api.start_nodeos(
                http_addr=f'127.0.0.1:{http_port}',
                p2p_addr=f'127.0.0.1:{p2p_port}',
                sig_provider=f'{cleos.keys[producer]}=KEY:{cleos.private_keys[producer]}',
                producer_name=producer,
                data_dir=f'/root/nodeos-{producer}/data',
                paused=True)

            ports.append({
                'http': http_port,
                'p2p': p2p_port
            })
            apis.append(api)

        time.sleep(1)

        # perform peer connections
        for i in range(node_amount):
            api = apis[i]

            status = api.connect_node('127.0.0.1:9876')
            assert status == 'added connection'

            peer_idxs = [j for j in range(node_amount) if j != i]
            for j in peer_idxs:
                status = api.connect_node(f'127.0.0.1:{ports[j]["p2p"]}')
                assert status == 'added connection'

        # perform eosio producer handover
        cleos.resume_block_production()
        for api in apis:
            api.resume_block_production()

        state = cleos.get_global_state()
        cleos.wait_blocks(1000 - (state['block_num'] + 1), sleep_time=10)

        state = cleos.get_global_state()
        last_claim_time = float(state['last_claimrewards'])

        cleos.wait_blocks(3588, sleep_time=10)

        init_state = cleos.get_global_state()
        schedule = cleos.get_schedule()

        assert schedule['active']['version'] == 1
        assert last_claim_time == float(
            cleos.get_global_state()['last_claimrewards'])

        total_unpaid_pinfo = 0
        for producer in producers:
            pinfo = cleos.get_producer(producer)
            total_unpaid_pinfo += pinfo['unpaid_blocks']

        pinfo = cleos.get_producer(producers[0])

        assert pinfo['is_active']
        assert 1 < pinfo['unpaid_blocks']

        assert total_unpaid_pinfo == init_state['total_unpaid_blocks']

        ec, out = cleos.claim_rewards(producers[0])
        assert ec != 0
        assert 'No payment exists for account' in out 

        cleos.wait_blocks(200)
   
        pinfo = cleos.get_producer(producers[0])
        state = cleos.get_global_state()

        yield cleos

    finally:
        container.stop()
