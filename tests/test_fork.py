#!/usr/bin/env python3

import time

from pathlib import Path

import docker

from py_eosio.cleos import CLEOS, default_nodeos_image
from py_eosio.sugar import (
    get_container,
    docker_move_out,
    docker_move_into
)


def test_fork(cleos):
    try:
        dclient = docker.from_env()
        vtestnet = get_container(
            dclient,
            default_nodeos_image(),
            force_unique=True,
            detach=True,
            network='host')

        snap_info = cleos.create_snapshot('localhost:8888')

        docker_move_out(
            dclient,
            cleos.vtestnet,
            snap_info['snapshot_name'], '/tmp/'
        )

        snap_name = Path(snap_info['snapshot_name']).name

        docker_move_into(
            dclient,
            vtestnet,
            f'/tmp/{snap_name}', '/root'
        )

        cleos = CLEOS(dclient, vtestnet)

        cleos.start_keosd()

        cleos.start_nodeos(
            plugins = [
                'producer_plugin',
                'producer_api_plugin',
                'chain_api_plugin',
                'net_plugin',
                'net_api_plugin',
                'http_plugin',
                'state_history_plugin'
            ],
            http_addr='0.0.0.0:8889',
            p2p_addr='0.0.0.0:9877',
            hist_addr='0.0.0.0:19999',
            snapshot=f'/root/tmp/{snap_name}',
            sig_provider='EOS5GnobZ231eekYUJHGTcmy2qve1K23r5jSFQbMfwWTtPB7mFZ1L=KEY:5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL',
            peers=['localhost:9876'],
            extra_params=[
                '--last-block-time-offset=-200',
                '--last-block-cpu-effort-percent=100'
            ])

        time.sleep(0.5)

        cleos.setup_wallet('5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL')
        cleos.wait_blocks(1)

        cleos.wait_for_phrase_in_nodeos_logs('FORK!')

    finally:
        vtestnet.stop()
        vtestnet.remove()

