#!/usr/bin/env python3

import time

from pathlib import Path

import pytest
import docker

from leap.cleos import CLEOS, default_nodeos_image
from leap.sugar import (
    get_container,
    docker_move_out,
    docker_move_into
)


@pytest.mark.manual
def test_fork(multi_cleos):

    logger = logging.getLogger('cleos')

    dclient = docker.from_env()
    node_amount = 3

    eosio_container = dclient.containers.run(
        default_nodeos_image(),
        detach=True,
        network='host',
        remove=True)

    node_containers = [
        dclient.containers.run(
            default_nodeos_image(),
            detach=True,
            network='host',
            remove=True)
        for i in range(node_amount)
    ]

    logger.info('Container launched')

    try:
        cleos = CLEOS(dclient, eosio_container, logger=logger)

        keosd_port = get_free_port()

        cleos.start_keosd()

        logger.info('Nodeos start')
        cleos.start_nodeos_from_config(
            '/root/nodeos/config.ini',
            data_dir='/root/nodeos/data',
            state_plugin=True,
            genesis='/root/nodeos/genesis/local.json',
            not_shutdown_thresh_exeded=True)

        time.sleep(0.5)

        cleos.setup_wallet('5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL')
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

        # create snapshot
        snap_info = cleos.create_snapshot('localhost:8888')

        docker_move_out(
            dclient,
            cleos.vtestnet,
            snap_info['snapshot_name'], '/tmp/'
        )

        snap_name = Path(snap_info['snapshot_name']).name

        logger.info(f'Is block production paused: {cleos.is_block_production_paused()}') 

        # init producer nodes
        apis = []
        ports = []
        for i, producer in enumerate(producers):
            http_port = get_free_port()
            p2p_port = get_free_port()

            docker_move_into(
                dclient,
                node_containers[i],
                f'/tmp/{snap_name}', '/root'
            )

            api = CLEOS(
                dclient,
                node_containers[i],
                url=f'http://127.0.0.1:{http_port}')

            opt_params = {}
            if i == len(producers) - 1:
                opt_params['extra_params'] = [
                    '--last-block-time-offset=0',
                    '--last-block-cpu-effort-percent=100'
                ]
                opt_params['hist_addr'] = '127.0.0.1:19999'
                opt_params['plugins'] = [
                    'producer_plugin',
                    'producer_api_plugin',
                    'chain_api_plugin',
                    'net_plugin',
                    'net_api_plugin',
                    'http_plugin',
                    'state_history_plugin'
                ]

            api.start_nodeos(
                http_addr=f'127.0.0.1:{http_port}',
                p2p_addr=f'127.0.0.1:{p2p_port}',
                snapshot=f'/root/tmp/{snap_name}',
                sig_provider=f'{cleos.keys[producer]}=KEY:{cleos.private_keys[producer]}',
                producer_name=producer,
                data_dir=f'/root/nodeos-{producer}/data',
                paused=True,
                not_shutdown_thresh_exeded=True,
                **opt_params)

            api.wait_for_phrase_in_nodeos_logs('start listening for http requests')

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

        # eosio_container.stop()

        api = apis[0]

        state = api.get_global_state()

        api.wait_blocks(3550, sleep_time=10)

        init_state = api.get_global_state()
        schedule = api.get_schedule()

        assert schedule['active']['version'] == 1

        total_unpaid_pinfo = 0
        for producer in producers:
            pinfo = api.get_producer(producer)
            total_unpaid_pinfo += pinfo['unpaid_blocks']

        pinfo = api.get_producer(producers[0])

        assert pinfo['is_active']
        assert 1 < pinfo['unpaid_blocks']

        ec, _ = cleos.unlock_wallet()
        assert ec == 0

        cleos.wait_for_phrase_in_nodeos_logs('switching forks from')

    finally:
        eosio_container.stop()
        for container in node_containers:
            container.stop()
