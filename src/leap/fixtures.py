# py-leap: Antelope protocol framework
# Copyright 2021-eternity Guillermo Rodriguez

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import sys
import json
import logging
import subprocess

from pathlib import Path
from contextlib import contextmanager

import docker
import pytest
import antelope_rs

from docker.types import Mount

from leap.cleos import CLEOS
from leap.sugar import (
    get_container,
    get_free_port,
)


DEFAULT_NODEOS_REPO = 'guilledk/py-leap'
DEFAULT_NODEOS_IMAGE = 'leap-5.0.3'


def default_nodeos_image():
    return f'{DEFAULT_NODEOS_REPO}:{DEFAULT_NODEOS_IMAGE}'


def maybe_get_marker(request, mark_name: str, field: str, default):
    mark = request.node.get_closest_marker(mark_name)
    if mark is None:
        return default
    else:
        return getattr(mark, field)


@contextmanager
def open_test_nodeos(request, tmp_path_factory):
    if sys.platform != 'linux':
        pytest.skip('Linux only')

    tmp_path = tmp_path_factory.getbasetemp() / request.node.name
    leap_path = tmp_path / 'leap'
    leap_path.mkdir(parents=True, exist_ok=True)
    leap_path = leap_path.resolve()

    logging.info(f'created tmp path at {leap_path}')

    dclient = docker.from_env()

    container_img = default_nodeos_image() + '-bs'
    logging.info(f'launching {container_img} container...')

    http_port = get_free_port()
    ship_port = get_free_port()
    cmd = ['nodeos', '-e', '-p', 'eosio', '--config-dir', '/', '--data-dir', '/data']
    cmd += [
        '>>', '/root/nodeos.log', '2>&1'
    ]

    container_cmd = ['/bin/bash', '-c', ' '.join(cmd)]

    vtestnet = get_container(
        dclient,
        container_img,
        force_unique=True,
        name=f'{tmp_path.name}-leap',
        detach=True,
        remove=True,
        ports={
            '8888/tcp': http_port,
            '18999/tcp': ship_port
        },
        mounts=[Mount('/root', str(leap_path), 'bind')],
        command=container_cmd
    )
    cleos = CLEOS(
        endpoint=f'http://127.0.0.1:{http_port}',
        ship_endpoint=f'ws://127.0.0.1:{ship_port}',
        node_dir=leap_path
    )

    # load keys
    ec, out = vtestnet.exec_run('cat /keys.json')
    keys = json.loads(out.decode('utf-8'))

    for account, keys in keys.items():
        priv, _pub = keys
        cleos.import_key(account, priv)

    did_nodeos_launch = False

    try:
        cleos.import_key('eosio', '5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL')
        cleos.wait_blocks(1)

        did_nodeos_launch = True

        yield cleos

    finally:
        if did_nodeos_launch:
            logging.info(f'to see nodeos logs: \"less {leap_path}/nodeos.log\"')

        else:
            process = subprocess.run(
                ['cat', str(leap_path / 'nodeos.log')],
                text=True, capture_output=True
            )
            logging.error('seems nodeos didn\'t launch? showing logs...')
            logging.error(process.stdout)

        if vtestnet is not None:
            try:
                vtestnet.exec_run('pkill -f nodeos')
                vtestnet.wait(timeout=120)
                vtestnet.kill(signal='SIGTERM')
                vtestnet.wait(timeout=20)

            except docker.errors.NotFound:
                ...

            except docker.errors.APIError:
                ...


@contextmanager
def bootstrap_test_nodeos(request, tmp_path_factory):
    if sys.platform != 'linux':
        pytest.skip('Linux only')

    tmp_path = tmp_path_factory.getbasetemp() / request.node.name
    leap_path = tmp_path / 'leap'
    leap_path.mkdir(parents=True, exist_ok=True)
    leap_path = leap_path.resolve()

    bootstrap: bool = maybe_get_marker(
        request, 'bootstrap', 'args', [False])[0]

    randomize: bool = maybe_get_marker(
        request, 'randomize', 'args', [True])[0]

    contracts = maybe_get_marker(
        request, 'contracts', 'kwargs', {})

    logging.info(f'created tmp path at {leap_path}')

    dclient = docker.from_env()

    container_img = default_nodeos_image()
    logging.info(f'launching {container_img} container...')

    cmd = ['nodeos', '-e', '-p', 'eosio', '--config-dir', '/root', '--data-dir', '/root/data']

    for plugin in [
        'net_plugin',
        'http_plugin',
        'chain_plugin',
        'producer_plugin',
        'chain_api_plugin',
        'producer_api_plugin'
    ]:
        cmd += ['--plugin', f'eosio::{plugin}']

    http_port = get_free_port() if randomize else 8888
    cmd += ['--http-server-address', '0.0.0.0:8888']
    cmd += ['--http-validate-host', '0']

    if randomize:
        priv, pub = antelope_rs.gen_key_pair(0)
    else:
        priv, pub = ('5Jr65kdYmn33C3UabzhmWDm2PuqbRfPuDStts3ZFNSBLM7TqaiL', 'EOS5GnobZ231eekYUJHGTcmy2qve1K23r5jSFQbMfwWTtPB7mFZ1L')

    cmd += ['--signature-provider', f'{pub}=KEY:{priv}']

    genesis_info = json.dumps({
        'initial_timestamp': '2019-04-15T11:00:00.000',
        'initial_key': pub,
        'initial_configuration': {
            'max_block_net_usage': 1048576,
            'target_block_net_usage_pct': 1000,
            'max_transaction_net_usage': 1048575,
            'base_per_transaction_net_usage': 12,
            'net_usage_leeway': 500,
            'context_free_discount_net_usage_num': 20,
            'context_free_discount_net_usage_den': 100,
            'max_block_cpu_usage': 200 * 1000,
            'target_block_cpu_usage_pct': 1000,
            'max_transaction_cpu_usage': 150 * 1000,
            'min_transaction_cpu_usage': 100,
            'max_transaction_lifetime': 3600,
            'deferred_trx_expiration_window': 600,
            'max_transaction_delay': 3888000,
            'max_inline_action_size': 4096,
            'max_inline_action_depth': 4,
            'max_authority_depth': 6
        }
    }, indent=4)

    with open(leap_path / 'genesis.json', 'w+') as genesis_file:
        genesis_file.write(genesis_info)

    logging.info(f'using genesis info: \n{genesis_info}')

    cmd += [
        '--genesis-json', '/root/genesis.json',
        '--contracts-console',
        '>>', '/root/nodeos.log', '2>&1'
    ]

    container_cmd = ['/bin/bash', '-c', ' '.join(cmd)]

    logging.info(f'starting nodeos container with cmd: {json.dumps(container_cmd, indent=4)}')

    did_nodeos_launch = False

    vtestnet = get_container(
        dclient,
        container_img,
        force_unique=True,
        name=f'{tmp_path.name}-leap',
        detach=True,
        remove=True,
        ports={'8888/tcp': http_port},
        mounts=[Mount('/root', str(leap_path), 'bind')],
        command=container_cmd
    )

    # maybe init contract cache dir
    download_location = Path('tests/contracts')
    download_location.mkdir(exist_ok=True, parents=True)


    cleos = CLEOS(f'http://127.0.0.1:{http_port}', node_dir=leap_path)
    rcleos = CLEOS('https://testnet.telos.net')

    def maybe_download_contract(
        account_name: str,
        local_name: str | None = None
    ):
        if not local_name:
            local_name = account_name

        logging.info(f'maybe download {local_name}')

        abi = rcleos.get_abi(account_name)
        cleos.load_abi(account_name, abi)

        contract_loc = download_location / local_name
        if contract_loc.is_dir():
            logging.info('...skip already downloaded.')
            return

        else:
            logging.info('downloading...')
            contract_loc.mkdir()

        rcleos.download_contract(
            account_name, contract_loc,
            local_name=local_name,
            abi=abi
        )
        logging.info('done.')

    try:
        cleos.import_key('eosio', priv)

        maybe_download_contract('eosio', local_name='eosio.system')
        maybe_download_contract('eosio.token')

        if bootstrap:
            maybe_download_contract('eosio.msig')
            maybe_download_contract('eosio.wrap')
            maybe_download_contract('telos.decide')

            cleos.wait_blocks(1)
            cleos.boot_sequence(
                contracts=download_location, remote_node=rcleos, extras=['telos'])
        else:
            cleos.wait_blocks(1)

        for account_name, location in contracts.items():
            cleos.deploy_contract_from_path(
                account_name,
                location,
                contract_name=account_name
            )

        did_nodeos_launch = True

        cleos.wait_blocks(1)

        yield cleos

    finally:
        if did_nodeos_launch:
            logging.info(f'to see nodeos logs: \"less {leap_path}/nodeos.log\"')

        else:
            process = subprocess.run(
                ['cat', str(leap_path / 'nodeos.log')],
                text=True, capture_output=True
            )
            logging.error('seems nodeos didn\'t launch? showing logs...')
            logging.error(process.stdout)

        vtestnet.exec_run('chmod 777 /root')

        if vtestnet is not None:
            try:
                vtestnet.exec_run('pkill -f nodeos')
                vtestnet.wait(timeout=120)
                vtestnet.kill(signal='SIGTERM')
                vtestnet.wait(timeout=20)

            except docker.errors.NotFound:
                ...

            except docker.errors.APIError:
                ...


@pytest.fixture(scope='module')
def cleos(request, tmp_path_factory):
    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos

@pytest.fixture(scope='module')
def cleos_bs(request, tmp_path_factory):
    with open_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos
