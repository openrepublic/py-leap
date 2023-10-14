#!/usr/bin/env python3

import sys
import time
import json
import logging
import requests
import binascii

from copy import deepcopy
from typing import (
    Dict,
    List,
    Union,
    Tuple,
    Optional,
    Iterator
)
from urllib3.util.retry import Retry
from pathlib import Path
from difflib import SequenceMatcher

from datetime import datetime, timedelta

import asks

from docker.client import DockerClient
from docker.models.containers import Container

from ueosio import (
    sign_tx,
    DataStream,
    get_expiration, get_tapos_info, build_push_transaction_body
)

from requests.adapters import HTTPAdapter

from .sugar import *
from .errors import ContractDeployError
from .tokens import DEFAULT_SYS_TOKEN_SYM
from .typing import (
    ExecutionResult,
    ExecutionStream,
    ActionResult
)


DEFAULT_NODEOS_REPO = 'guilledk/py-leap'
DEFAULT_NODEOS_IMAGE = 'leap-4.0.4'


def default_nodeos_image():
    return f'{DEFAULT_NODEOS_REPO}:{DEFAULT_NODEOS_IMAGE}'


class CLEOS:

    def __init__(
        self,
        docker_client: DockerClient,
        vtestnet: Container,
        url: str = 'http://127.0.0.1:8888',
        remote: str = 'https://mainnet.telos.net',
        logger = None
    ):

        self.client = docker_client
        self.vtestnet = vtestnet
        self.url = url

        if logger is None:
            self.logger = logging.getLogger('cleos')
        else:
            self.logger = logger

        self.endpoint = url
        self.remote_endpoint = remote

        self.keys = {}
        self.private_keys = {}

        self._sys_token_init = False
        self.sys_token_supply = Asset(0, DEFAULT_SYS_TOKEN_SYM)

        self._session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=10,
            backoff_factor=0.1,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        if 'asks' in sys.modules:
            self._asession = asks.Session(connections=200)

    def _get(self, *args, **kwargs):
        return self._session.get(*args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._session.post(*args, **kwargs)

    async def _aget(self, *args, **kwargs):
        return await self._asession.get(*args, **kwargs)

    async def _apost(self, *args, **kwargs):
        return await self._asession.post(*args, **kwargs)

    async def _create_and_push_tx(self, actions: list[dict], key: str):
        chain_info = await self.a_get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id = chain_info['chain_id']

        res = None
        retries = 3
        while retries > 0:
            tx = {
                'delay_sec': 0,
                'max_cpu_usage_ms': 0,
                'actions': deepcopy(actions)
            }

            # package transation
            for i, action in enumerate(tx['actions']):
                ds = DataStream()
                data = action['data']

                for val in data.values():
                    if isinstance(val, str):
                        ds.pack_string(val)

                    elif isinstance(val, Name):
                        ds.pack_name(str(val))

                    elif isinstance(val, Asset):
                        ds.pack_asset(str(val))

                    elif isinstance(val, Symbol):
                        ds.pack_symbol(str(val))

                    elif isinstance(val, Checksum256):
                        ds.pack_checksum256(str(val))

                    elif isinstance(val, int):
                        ds.pack_uint64(val)

                    elif isinstance(val, ListArgument):
                        ds.pack_array(val.type, val.list)

                    else:
                        raise ValueError(
                            f'datastream packing not implemented for {type(val)}')

                tx['actions'][i]['data'] = binascii.hexlify(
                    ds.getvalue()).decode('utf-8')

            tx.update({
                'expiration': get_expiration(
                    datetime.utcnow(), timedelta(minutes=15).total_seconds()),
                'ref_block_num': ref_block_num,
                'ref_block_prefix': ref_block_prefix,
                'max_net_usage_words': 0,
                'max_cpu_usage_ms': 0,
                'delay_sec': 0,
                'context_free_actions': [],
                'transaction_extensions': [],
                'context_free_data': []
            })

            # Sign transaction
            _, signed_tx = sign_tx(chain_id, tx, key)

            # Pack
            ds = DataStream()
            ds.pack_transaction(signed_tx)
            packed_trx = binascii.hexlify(ds.getvalue()).decode('utf-8')
            final_tx = build_push_transaction_body(signed_tx['signatures'][0], packed_trx)

            # Push transaction
            logging.info(f'pushing tx to: {self.endpoint}')
            res = (await self._apost(f'{self.endpoint}/v1/chain/push_transaction', json=final_tx)).json()
            res_json = json.dumps(res, indent=4)

            logging.info(res_json)

            retries -= 1

            if 'error' in res:
                continue

            else:
                break

        if not res:
            ValueError('res is None')

        return res


    async def a_push_action(
        self,
        account: str,
        action: str,
        data: dict,
        actor: str,
        key: str,
        permission: str = 'active'
    ):
        return await self._create_and_push_tx([{
            'account': str(account),
            'name': str(action),
            'data': data,
            'authorization': [{
                'actor': str(actor),
                'permission': str(permission)
            }]
        }], key)

    async def a_push_actions(
        self,
        actions: list[dict],
        key: str,
    ):
        return await self._create_and_push_tx(actions, key)

    def run(
        self,
        cmd: List[str],
        retry: int = 0,
        *args, **kwargs
    ) -> ExecutionResult:
        """Run command inside the virtual testnet docker container.

        :param cmd: Command and parameters separated in chunks in a list.
        :param retry: It's normal for blockchain interactions to timeout so by default
            this method retries commands 3 times. ``retry=0`` should be passed
            to avoid retry.
        :param args: This method calls ``container.exec_run`` docker APIs under the
            hood so you can pass extra arguments to it according to this (`docs
            <https://docker-py.readthedocs.io/en/stable/containers.html#docker.m
            odels.containers.Container.exec_run>`_).
        :param kwargs: Same as ``args`` but for keyword arguments.

        :return: A tuple with the process exitcode and output.
        :rtype: :ref:`typing_exe_result`
        """

        # stringify command
        cmd = [
            json.dumps(chunk)
            if isinstance(chunk, Dict) or isinstance(chunk, List)
            else str(chunk)
            for chunk in cmd
        ]

        self.logger.info(f'exec run: {cmd}')
        for i in range(1, 2 + retry):
            ec, out = self.vtestnet.exec_run(cmd, *args, **kwargs)

            if ec == 0:
                break

            if b'Error 3120003: Locked wallet' in out:
                ec, _ = self.run(['cleos', 'wallet', 'unlock', f'--password={self.wallet_key}'])
                assert ec == 0

            self.logger.warning(f'cmd run retry num {i}...')

        out = out.decode('utf-8')

        if ec != 0:
            self.logger.error('command exited with non zero status:')
            self.logger.error(out)

        return ec, out

    def open_process(
        self,
        cmd: List[str],
        **kwargs
    ) -> ExecutionStream:
        """Begin running the command inside the virtual container, return the
        internal docker process id, and a stream for the standard output.

        :param cmd: List of individual string forming the command to execute in
            the testnet container shell.
        :param kwargs: A variable number of key word arguments can be
            provided, as this function uses `exec_create & exec_start docker APIs
            <https://docker-py.readthedocs.io/en/stable/api.html#module-dock
            er.api.exec_api>`_.

        :return: A tuple with the process execution id and the output stream to
            be consumed.
        :rtype: :ref:`typing_exe_stream`
        """
        # stringify command
        cmd = [str(chunk) for chunk in cmd]
        return docker_open_process(self.client, self.vtestnet, cmd, **kwargs)

    def wait_process(
        self,
        exec_id: str,
        exec_stream: Iterator[str]
    ) -> ExecutionResult:
        """Collect output from process stream, then inspect process and return
        exitcode.

        :param exec_id: Process execution id provided by docker engine.
        :param exec_stream: Process output stream to be consumed.

        :return: Exitcode and process output.
        :rtype: :ref:`typing_exe_result`
        """

        return docker_wait_process(self.client, exec_id, exec_stream) 

    def start_keosd(self, *args, **kwargs):
        exec_id, exec_stream = self.open_process(
            ['keosd', *args], **kwargs)
        self.__keosd_exec_id = exec_id
        self.__keosd_exec_stream = exec_stream

        self.logger.info('Streaming keosd...')

        for msg in exec_stream:
            msg = msg.decode('utf-8')
            self.logger.info(msg.rstrip())
            if 'add api url: /v1/node/get_supported_apis' in msg:
                break

    def stream_keosd(self):
        assert self.is_keosd_running()
        for msg in self.__keosd_exec_stream:
            yield msg.decode('utf-8')

    def start_nodeos(
        self,
        plugins: List[str] = [
            'producer_plugin',
            'producer_api_plugin',
            'chain_api_plugin',
            'net_plugin',
            'net_api_plugin',
            'http_plugin'
        ],
        http_addr: str = '0.0.0.0:8888',
        p2p_addr: str = '0.0.0.0:9876',
        hist_addr: Optional[str] = None,
        genesis: Optional[str] = None,
        snapshot: Optional[str] = None,
        sig_provider: str = 'EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV=KEY:5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3',
        producer_name: str = 'eosio',
        data_dir: str = '/root/nodeos/data',
        peers: List[str] = [],
        paused: bool = False,
        not_shutdown_thresh_exeded: bool = False,
        extra_params: List[str] = []
    ):
        cmd = [
            'nodeos',
            '-e',
            '-p', producer_name,
            *[f'--plugin=eosio::{p}' for p in plugins],
            f'--http-server-address={http_addr}',
            f'--p2p-listen-endpoint={p2p_addr}',
            '--abi-serializer-max-time-ms=999999',
            '--access-control-allow-origin=\"*\"',
            '--contracts-console',
            '--http-validate-host=false',
            '--verbose-http-errors',
            f'--data-dir={data_dir}',
            f'--signature-provider={sig_provider}',
            *[f'--p2p-peer-address={peer}' for peer in peers]]

        if paused:
            cmd += ['-x']

        if snapshot:
            cmd += [f'--snapshot={snapshot}']

        if genesis:
            cmd += [f'--genesis-json={genesis}']

        if not_shutdown_thresh_exeded:
            cmd += ['--resource-monitor-not-shutdown-on-threshold-exceeded']

        if 'state_history_plugin' in plugins:
            # https://github.com/LEAP/eos/issues/6334
            assert hist_addr
            cmd += [f'--state-history-endpoint={hist_addr}']
            cmd += ['--disable-replay-opts']

        cmd += extra_params

        exec_id, exec_stream = self.open_process(cmd)
        self.__nodeos_exec_id = exec_id
        self.__nodeos_exec_stream = exec_stream

        self._nodeos_exec_stream = exec_stream

    def start_nodeos_from_config(
        self,
        config: str,
        state_plugin: bool = False,
        genesis: Optional[str] = None,
        data_dir: str = '/mnt/dev/data',
        snapshot: Optional[str] = None,
        logging_cfg: Optional[str] = None,
        logfile: Optional[str] = '/root/nodeos.log',
        is_local: bool = True,
        not_shutdown_thresh_exeded: bool = False,
        extra_params: List[str] = []
    ):
        cmd = [
            'nodeos',
            '-e',
            '-p', 'eosio',
            f'--data-dir={data_dir}',
            f'--config={config}',
            *extra_params
        ]
        if state_plugin:
            # https://github.com/EOSIO/eos/issues/6334
            cmd += ['--disable-replay-opts']

        if genesis:
            cmd += [f'--genesis-json={genesis}']

        if snapshot:
            cmd += [f'--snapshot={snapshot}']

        if logging_cfg:
            cmd += [f'--logconf={logging_cfg}']

        if not_shutdown_thresh_exeded:
            cmd += ['--resource-monitor-not-shutdown-on-threshold-exceeded']

        if logfile:
            cmd += [f'> {logfile} 2>&1'] 


        final_cmd = ['/bin/bash', '-c', ' '.join(cmd)]

        self.logger.info(f'starting nodeos with cmd: {final_cmd}')
        exec_id, exec_stream = self.open_process(final_cmd)
        self.__nodeos_exec_id = exec_id
        self.__nodeos_exec_stream = exec_stream

        if is_local:
            return self.wait_produced(from_file=logfile)
        else:
            return self.wait_received(from_file=logfile)

    def stop_nodeos(self, from_file: Optional[str] = None):
        # gracefull nodeos exit
        proc_inf = self.open_process(['pkill', 'nodeos'])

        ec, out = self.wait_process(*proc_inf)
        self.logger.info(f'pkill nodeos: {ec}')
        self.logger.info('await gracefull nodeos exit...')

        self.wait_stopped(from_file=from_file, timeout=120)

        self.logger.info('nodeos exit.')

    def gather_nodeos_output(self) -> str:
        return self.wait_process(
            self.__nodeos_exec_id,
            self.__nodeos_exec_stream
        )

    def gather_keosd_output(self) -> str:
        return self.wait_process(
            self.__keosd_exec_id,
            self.__keosd_exec_stream
        )

    def is_nodeos_running(self) -> bool:
        return self.client.api.exec_inspect(
            self.__nodeos_exec_id)['Running']

    def is_keosd_running(self) -> bool:
        return self.client.api.exec_inspect(
            self.__keosd_exec_id)['Running']

    def wait_for_phrase_in_nodeos_logs(
        self,
        phrase: str,
        lines: int = 100,
        timeout: int = 60,
        from_file: Optional[str] = None
    ):
        if from_file:
            exec_id, exec_stream = self.open_process(
                ['/bin/bash', '-c', f'timeout {timeout}s tail -n {lines} -f {from_file}'])
        else:
            exec_stream = self.__nodeos_exec_stream

        out = ''
        for chunk in exec_stream:
            msg = chunk.decode('utf-8')
            out += msg
            self.logger.info(msg.rstrip())
            if phrase in msg:
                break

        return out

    def wait_stopped(self, **kwargs):
        return self.wait_for_phrase_in_nodeos_logs('nodeos successfully exiting', **kwargs)

    def wait_produced(self, **kwargs):
        return self.wait_for_phrase_in_nodeos_logs('Produced', **kwargs)

    def wait_received(self, **kwargs):
        return self.wait_for_phrase_in_nodeos_logs('Received', **kwargs)

    def deploy_contract(
        self,
        contract_name: str,
        build_dir: str,
        privileged: bool = False,
        account_name: Optional[str] = None,
        create_account: bool = True,
        staked: bool = True,
        verify_hash: bool = True,
        debug: bool = False
    ):
        """Deploy a built contract inside the container.

        :param contract_name: Name of contract wasm file. 
        :param build_dir: Path to the directory the wasm and abi files are
            located. Fuzzy searches all subdirectories for the .wasm and then
            picks the one most similar to ``contract_name``, asumes .abi has
            the same name and is in same directory.
        :param privileged: ``True`` if contract should be privileged (system
            contracts).
        :param account_name: Name of the target account of the deployment.
        :param create_account: ``True`` if target account should be created.
        :param staked: ``True`` if this account should use RAM & NET resources.
        :param verify_hash: Query remote node for ``contract_name`` and compare
            hashes.
        """
        self.logger.info(f'contract {contract_name}:')

        account_name = contract_name if not account_name else account_name

        if create_account:
            self.logger.info('\tcreate account...')
            if staked:
                self.create_account_staked('eosio', account_name)
            else:
                self.create_account('eosio', account_name)
            self.logger.info('\taccount created')

        if privileged:
            self.push_action(
                'eosio', 'setpriv',
                [account_name, 1],
                'eosio@active'
            )

        self.logger.info('\tgive .code permissions...')
        cmd = [
            'cleos', '--url', self.url, 'set', 'account', 'permission', account_name,
            'active', '--add-code'
        ]
        ec, out = self.run(cmd)
        self.logger.info(f'\tcmd: {cmd}')
        self.logger.info(f'\t{out}')
        assert ec == 0
        self.logger.info('\tpermissions granted.')

        ec, out = self.run(
            ['find', build_dir, '-type', 'f', '-name', '*.wasm'],
            retry=0
        )
        wasms = out.rstrip().split('\n')

        wasms.sort()  # alphabetical
        wasms.sort(key=len)  # length

        self.logger.info(f'wasm candidates:\n{json.dumps(wasms, indent=4)}')

        # Fuzzy match all .wasm files, select one most similar to {contract.name} 
        matches = sorted(
            [Path(wasm) for wasm in wasms],
            key=lambda match: SequenceMatcher(
                None, contract_name, match.stem).ratio(),
            reverse=True
        )
        if len(matches) == 0: 
            raise ContractDeployError(
                f'Couldn\'t find {contract_name}.wasm')

        wasm_path = matches[0]
        wasm_file = str(wasm_path).split('/')[-1]
        abi_file = wasm_file.replace('.wasm', '.abi')

        # verify contract hash using remote node
        if verify_hash:
            self.logger.info('verifing wasm hash...')
            ec, out = self.run(
                ['sha256sum', wasm_path],
                retry=0
            )
            assert ec == 0

            local_shasum = out.split(' ')[0]
            self.logger.info(f'local sum: {local_shasum}')

            self.logger.info(f'asking remote {self.remote_endpoint}...')
            resp = self._post(
                f'{self.remote_endpoint}/v1/chain/get_code',
                json={
                    'account_name': account_name,
                    'code_as_wasm': 1
                }).json()

            if 'code_hash' not in resp:
                self.logger.warning('couldn\'t perform hash check remote response:')
                self.logger.warning(json.dumps(resp, indent=4))

            else:

                remote_shasum = resp['code_hash']
                self.logger.info(f'remote sum: {remote_shasum}')

                if local_shasum != remote_shasum:
                    raise ContractDeployError(
                        f'Local contract hash doesn\'t match remote:\n'
                        f'local: {local_shasum}\n'
                        f'remote: {remote_shasum}')

        self.logger.info('deploy...')
        self.logger.info(f'wasm path: {wasm_path}')
        self.logger.info(f'wasm: {wasm_file}')
        self.logger.info(f'abi: {abi_file}')

        cmd = [
            'cleos',
            '--url', self.url,
            'set', 'contract', account_name,
            str(wasm_path.parent),
            wasm_file,
            abi_file,
            '-p', f'{account_name}@active', '-j'
        ]

        if debug:
            cmd = cmd[:1] + ['--print-response'] + cmd[1:]

        self.logger.info('contract deploy: ')
        ec, out = self.run(cmd, retry=6)

        if debug:
            self.logger.info(out)

        if ec == 0:
            self.logger.info('deployed')

            # chop first two lines and return as json dict
            return json.loads(out.split('\n', 2)[2])

        else:
            raise ContractDeployError(f'Couldn\'t deploy {account_name} contract.')

    def deploy_contract_from_host(
        self,
        contract_name: str,
        build_dir: str,
        privileged: bool = False,
        account_name: Optional[str] = None,
        create_account: bool = True,
        staked: bool = True,
        verify_hash: bool = True
    ):
        """Deploy a built contract from outside container.

        :param contract_name: Name of contract wasm file. 
        :param build_dir: Path to the directory the wasm and abi files are
            located. Fuzzy searches all subdirectories for the .wasm and then
            picks the one most similar to ``contract_name``, asumes .abi has
            the same name and is in same directory.
        :param privileged: ``True`` if contract should be privileged (system
            contracts).
        :param account_name: Name of the target account of the deployment.
        :param create_account: ``True`` if target account should be created.
        :param staked: ``True`` if this account should use RAM & NET resources.
        :param verify_hash: Query remote node for ``contract_name`` and compare
            hashes.
        """
        ec, out = self.run(
            ['mkdir', '-p', '/tmp/host_contracts'])
        assert ec == 0

        dir_name = Path(build_dir).name

        docker_move_into(
            self.client,
            self.vtestnet,
            build_dir,
            f'/tmp/host_contracts')

        return self.deploy_contract(
            contract_name,
            f'/tmp/host_contracts/{build_dir}',
            privileged=privileged,
            account_name=account_name,
            create_account=create_account,
            staked=staked,
            verify_hash=verify_hash
        )

    def create_snapshot(self, target_url: str, body: dict):
        resp = self._post(
            f'{target_url}/v1/producer/create_snapshot',
            json=body
        )
        return resp

    def schedule_snapshot(self, target_url: str, **kwargs):
        resp = self._post(
            f'{target_url}/v1/producer/schedule_snapshot',
            json=kwargs
        )
        return resp

    def get_node_activations(self, target_url: str) -> List[Dict]:
        lower_bound = 0
        step = 250
        more = True
        features = []
        while more:
            r = self._post(
                f'{target_url}/v1/chain/get_activated_protocol_features',
                json={
                    'limit': step,
                    'lower_bound': lower_bound,
                    'upper_bound': lower_bound + step
                }
            )
            resp = r.json()

            assert 'activated_protocol_features' in resp
            features += resp['activated_protocol_features']
            lower_bound += step
            more = 'more' in resp

        # sort in order of activation
        features = sorted(features, key=lambda f: f['activation_ordinal'])
        features.pop(0)  # remove PREACTIVATE_FEATURE

        return features

    def clone_node_activations(self, target_url: str):
        features = self.get_node_activations(target_url)

        feature_names = [
            feat['specification'][0]['value']
            for feat in features
        ]

        self.logger.info('activating features:')
        self.logger.info(
            json.dumps(feature_names, indent=4))

        for f in features:
            self.activate_feature_with_digest(f['feature_digest'])

    def diff_protocol_activations(self, target_one: str, target_two: str):
        features_one = self.get_node_activations(target_one)
        features_two = self.get_node_activations(target_two)

        features_one_names = [
            feat['specification'][0]['value']
            for feat in features_one
        ]
        features_two_names = [
            feat['specification'][0]['value']
            for feat in features_two
        ]

        return list(set(features_one_names) - set(features_two_names))

    def boot_sequence(
        self,
        token_sym: Symbol = DEFAULT_SYS_TOKEN_SYM,
        ram_amount: int = 16_000_000_000,
        activations_node: Optional[str] = None,
        sys_contracts_mount='/root/nodeos/contracts',
        verify_hash: bool = False 
    ):
        """Perform enterprise operating system bios sequence acording to:

            https://developers.eos.io/welcome/latest/tutorials/bios-boot-sequence

        This includes:

            1) Creating the following accounts:

                - ``eosio.bpay``
                - ``eosio.names``
                - ``eosio.ram``
                - ``eosio.ramfee``
                - ``eosio.saving``
                - ``eosio.stake``
                - ``eosio.vpay``
                - ``eosio.rex``

            2) Deploy the following contracts that come in vtestnet image:

                ``eosio.token``, ``eosio.msig``, ``eosio.wrap``

            3) Initialize the ``SYS`` token.
            4) Activate v1 feature ``PREACTIVATE_FEATURE``.
            5) Deploy ``eosio.system`` to ``eosio`` account.
            6) Activate v2 features ``ONLY_BILL_FIRST_AUTHORIZER`` and ``RAM_RESTRICTIONS``.
            7) Set ``eosio.msig`` account as privileged in order to delegate permissions.
            8) System init.
            9) Parse contract manifest and deploy user contracts.

        """
        for name in [
            'eosio.bpay',
            'eosio.names',
            'eosio.ram',
            'eosio.ramfee',
            'eosio.saving',
            'eosio.stake',
            'eosio.vpay',
            # 'eosio.null',
            'eosio.rex',

            # custom telos
            'eosio.tedp',
            'works.decide',
            'amend.decide'
        ]:
            ec, _ = self.create_account('eosio', name)
            assert ec == 0 

        self.deploy_contract(
            'eosio.token',
            f'{sys_contracts_mount}/eosio.token',
            staked=False,
            verify_hash=verify_hash
        )

        self.deploy_contract(
            'eosio.msig',
            f'{sys_contracts_mount}/eosio.msig',
            staked=False,
            verify_hash=verify_hash
        )

        self.deploy_contract(
            'eosio.wrap',
            f'{sys_contracts_mount}/eosio.wrap',
            staked=False,
            verify_hash=verify_hash
        )

        self.init_sys_token(token_sym=token_sym)

        self.activate_feature_v1('PREACTIVATE_FEATURE')

        self.sys_deploy_info = self.deploy_contract(
            'eosio.system',
            f'{sys_contracts_mount}/eosio.system',
            account_name='eosio',
            create_account=False,
            verify_hash=verify_hash
        )

        self.wait_blocks(3)

        if not activations_node:
            activations_node = self.remote_endpoint

        self.clone_node_activations(activations_node)

        ec, _ = self.push_action(
            'eosio', 'setpriv',
            ['eosio.msig', 1],
            'eosio@active'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio', 'setpriv',
            ['eosio.wrap', 1],
            'eosio@active'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio', 'init',
            ['0', token_sym],
            'eosio@active'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio', 'setram',
            [ram_amount],
            'eosio@active'
        )
        assert ec == 0

        # Telos specific

        self.create_account_staked(
            'eosio', 'telos.decide', ram=2700000)

        self.deploy_contract(
            'telos.decide',
            f'{sys_contracts_mount}/telos.decide',
            create_account=False,
            verify_hash=verify_hash
        )

        for name in ['exrsrv.tf']:
            self.create_account_staked('eosio', name)

    # Producer API

    def is_block_production_paused(self):
        return self._post(
            f'{self.url}/v1/producer/paused').json()

    def resume_block_production(self):
        return self._post(
            f'{self.url}/v1/producer/resume').json()

    def pause_block_production(self):
        return self._post(
            f'{self.url}/v1/producer/pause').json()

    # Net API

    def connected_nodes(self):
        return self._post(
            f'{self.url}/v1/net/connections').json()

    def connect_node(self, endpoint: str):
        return self._post(
            f'{self.url}/v1/net/connect',
            json=endpoint).json()

    def disconnect_node(self, endpoint: str):
        return self._post(
            f'{self.url}/v1/net/disconnect',
            json=endpoint).json()

    def create_key_pair(self) -> Tuple[str, str]:
        """Generate a new LEAP key pair.

        :return: Public and private key.
        :rtype: Tuple[str, str]
        """
        ec, out = self.run(['cleos', '--url', self.url, 'create', 'key', '--to-console'])
        assert ec == 0
        assert ('Private key' in out) and ('Public key' in out)
        lines = out.split('\n')
        self.logger.info('created key pair')
        return lines[0].split(' ')[2].rstrip(), lines[1].split(' ')[2].rstrip()

    def create_key_pairs(self, n: int) -> List[Tuple[str, str]]:
        """Generate ``n`` LEAP key pairs, faster than calling
        :func:`~pytest_eosio.LEAPTestSession.create_key_pair` on a loop.

        :return: List of key pairs with a length of ``n``.
        :rtype: List[Tuple[str, str]]
        """
        procs = [
            self.open_process(['cleos', '--url', self.url, 'create', 'key', '--to-console'])
            for _ in range(n)
        ]
        results = [
            self.wait_process(proc_id, proc_stream)
            for proc_id, proc_stream in procs
        ]
        keys = []
        for ec, out in results:
            assert ec == 0
            assert ('Private key' in out) and ('Public key' in out)
            lines = out.split('\n')
            keys.append((lines[0].split(' ')[2].rstrip(), lines[1].split(' ')[2].rstrip()))

        self.logger.info(f'created {n} key pairs')
        return keys

    def import_key(self, private_key: str):
        """Import a private key into wallet inside testnet container.
        """
        ec, out = self.run(
            ['cleos', '--url', self.url, 'wallet', 'import', '--private-key', private_key]
        )
        self.logger.info(out)
        assert ec == 0
        self.logger.info('key imported')
        return out.split(':')[1][1:].rstrip()

    def import_keys(self, private_keys: List[str]):
        """Import a list of private keys into wallet inside testnet container.
        Faster than calling :func:`~pytest_eosio.LEAPTestSession.import_key` on a loop.
        """
        procs = [
            self.open_process(
                ['cleos', '--url', self.url, 'wallet', 'import', '--private-key', private_key])
            for private_key in private_keys
        ]
        results = [
            self.wait_process(proc_id, proc_stream)
            for proc_id, proc_stream in procs
        ]
        for ec, _ in results:
            assert ec == 0

        self.logger.info(f'imported {len(private_keys)} keys')

    def setup_wallet(self, dev_key: str):
        """Setup wallet acording to `telos docs <https://docs.telos.net/develope
        rs/platform/development-environment/create-development-wallet>`_.
        """

        # Step 1: Create a Wallet
        self.logger.info('create wallet...')
        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'create', '--to-console'])
        self.wallet_key = out.split('\n')[-2].strip('\"')
        assert ec == 0
        assert len(self.wallet_key) == 53
        self.logger.info('wallet created')

        # Step 2: Open the Wallet
        self.logger.info('open wallet...')
        ec, _ = self.run(['cleos', '--url', self.url, 'wallet', 'open'])
        assert ec == 0
        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'list'])
        assert ec == 0
        assert 'default' in out
        self.logger.info('wallet opened')

        # Step 3: Unlock it
        self.logger.info('unlock wallet...')
        ec, out = self.run(
            ['cleos', '--url', self.url, 'wallet', 'unlock', '--password', self.wallet_key]
        )
        assert ec == 0

        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'list'])
        assert ec == 0
        assert 'default *' in out
        self.logger.info('wallet unlocked')

        # Step 4:  Import keys into your wallet
        self.logger.info('import key...')
        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'create_key'])
        self.logger.info(out)
        assert ec == 0
        public_key = out.split('\"')[1]
        assert len(public_key) == 53
        self.dev_wallet_pkey = public_key
        self.logger.info(f'imported {public_key}')

        # Step 5: Import the Development Key
        self.logger.info('import development key...')
        self.keys['eosio'] = self.import_key(dev_key)
        self.private_keys['eosio'] = dev_key
        self.logger.info('imported dev key')

    def list_keys(self):
        return self.run(
            ['cleos', 'wallet', 'list'])

    def list_all_keys(self):
        return self.run(
            ['cleos', 'wallet', 'private_keys', f'--password={self.wallet_key}'])

    def unlock_wallet(self):
        return self.run(
            ['cleos', '--url', self.url, 'wallet', 'unlock', '--password', self.wallet_key]
        )

    def get_feature_digest(self, feature_name: str) -> str:
        """Given a feature name, query the v1 API endpoint: 

            ``/v1/producer/get_supported_protocol_features``

        to retrieve hash digest.

        :return: Feature hash digest.
        :rtype: str
        """
        r = self._post(
            f'{self.endpoint}/v1/producer/get_supported_protocol_features',
            json={}
        )
        resp = r.json()
        assert isinstance(resp, list) 

        for item in resp:
            if item['specification'][0]['value'] == feature_name:
                digest = item['feature_digest']
                break
        else:
            raise ValueError(f'{feature_name} feature not found.')

        self.logger.info(f'{feature_name} digest: {digest}')
        return digest

    def activate_feature_v1(self, feature_name: str):
        """Given a v1 feature name, activate it.
        """

        digest = self.get_feature_digest(feature_name)
        self.logger.info(f'activating {feature_name}...')
        r = self._post(
            f'{self.endpoint}/v1/producer/schedule_protocol_feature_activations',
            json={
                'protocol_features_to_activate': [digest]
            }
        ).json()

        self.logger.info(json.dumps(r, indent=4))

        assert 'result' in r
        assert r['result'] == 'ok'

        self.logger.info(f'{digest} active.')

    def activate_feature_with_digest(self, digest: str):
        """Given a v2 feature digest, activate it.
        """

        self.logger.info(f'activating {digest}...')
        ec, _ = self.push_action(
            'eosio', 'activate',
            [digest],
            'eosio@active'
        )
        assert ec == 0
        self.logger.info(f'{digest} active.')

    def activate_feature(self, feature_name: str):
        """Given a v2 feature name, activate it.
        """

        self.logger.info(f'activating {feature_name}...')
        digest = self.get_feature_digest(feature_name)
        self.activate_feature_with_digest(digest)

    def get_ram_price(self) -> Asset:
        row = self.get_table(
            'eosio', 'eosio', 'rammarket')[0]

        quote = asset_from_str(row['quote']['balance']).amount
        base = asset_from_str(row['base']['balance']).amount

        return Asset((quote / base) * 1024 / 0.995, self.sys_token_supply.symbol)

    def sign_transaction(
        self,
        key: str,
        tx: Dict,
        push: bool = False 
    ):
        chain_id = self.get_info()['chain_id']
        if push:
            push = ['--push-transaction']
        else:
            push = []
        ec, out = self.run(
            ['cleos', 'sign', '--public-key', key, '-c', chain_id, tx] + push)
        try:
            out = json.loads(out)

        except (json.JSONDecodeError, TypeError):
            ...

        return ec, out


    def push_action(
        self,
        contract: str,
        action: str,
        args: List[str],
        permissions: str,
        retry: int = 3,
        dump_tx: bool = False,
        sign: bool = True
    ) -> ActionResult:
        """Execute an action defined in a given contract, in case of failure retry.

        :param contract: Contract were action is defined.
        :param action: Name of the action to execute.
        :param args: List of action arguments.
        :param permissions: Authority with which to sign this transaction.
        :param retry: Max amount of retries allowed, can be zero for no retries.

        :return: Always returns a tuple with the exit code at the beggining and
            depending if the transaction was exectued, either the resulting json dict,
            or the full output including errors as a string at the end.
        :rtype: :ref:`typing_action_result`
        """

        args = [
            str(arg)
            if (
                isinstance(arg, Symbol) or
                isinstance(arg, Asset)
            )
            else arg
            for arg in args
        ]
        cmd = [
            'cleos', '--url', self.url, 'push', 'action', contract, action,
            json.dumps(args), '-p', permissions, '-j', '-f'
        ]
        if dump_tx:
            cmd += ['-d']

        if not sign:
            cmd += ['-s']

        if 'leap' in DEFAULT_NODEOS_IMAGE:
            cmd += ['--use-old-rpc', '-t', 'false']

        ec, out = self.run(cmd, retry=retry)

        if 'ABI for contract eosio.null not found. Action data will be shown in hex only.' in out:
            out = '\n'.join(out.split('\n')[1:])

        try:
            out = json.loads(out)

        except (json.JSONDecodeError, TypeError):
            ...

        return ec, out

    def push_transaction(
        self,
        trx: Dict, 
        retry: int = 3,
        dump_tx: bool = False
    ) -> ActionResult:
        """Execute a transaction, in case of failure retry.

        :param trx: JSON transaction.
        :param retry: Max amount of retries allowed, can be zero for no retries.

        :return: Always returns a tuple with the exit code at the beggining and
            depending if the transaction was exectued, either the resulting json dict,
            or the full output including errors as a string at the end.
        :rtype: :ref:`typing_action_result`
        """

        self.logger.info(f"push transaction: {json.dumps(trx, indent=4)}")
        cmd = [
            'cleos', '--url', self.url, 'push', 'transaction', 
            json.dumps(trx), '-j', '-f'
        ]
        ec, out = self.run(cmd, retry=retry)
        try:
            out = json.loads(out)

        except (json.JSONDecodeError, TypeError):
            ...

        return ec, out

    def parallel_push_action(
        self,
        actions: Tuple[
            Iterator[str],   # contract name
            Iterator[str],   # action name
            Iterator[List],  # params
            Iterator[str]    # permissions
        ]
    ) -> List[ActionResult]:
        """Push several actions in parallel and collect their results. 

        Example code:

        .. code-block:: python

            results = eosio_testnet.parallel_push_action((
                (contract for contract in contract_names),
                (action for action in action_names),
                (params for params in param_lists),
                (f'{auth}@active' for auth in authorities)
            ))
            for ec, _ in results:
                assert ec == 0

        :param actions: A tuple of four iterators, each iterator when consumed
            produces one of the following attriubutes:

            ``contract name``, ``action name``, ``arguments list`` and ``permissions``

        :return: A list the results for each action execution.
        :rtype: List[:ref:`typing_action_result`]
        """
        procs = [
            self.open_process([
                'cleos', '--url', self.url, 'push', 'action', contract, action,
                json.dumps(args), '-p', permissions, '-j', '-f'
            ]) for contract, action, args, permissions in zip(*actions)
        ]
        return [
            self.wait_process(proc_id, proc_stream)
            for proc_id, proc_stream in procs
        ]

    def create_account(
        self,
        owner: str,
        name: str,
        key: Optional[str] = None,
    ) -> ExecutionResult:
        """Create an unstaked eosio account, usualy used by system contracts.

        :param owner: The system accunt that authorizes the creation of a new account.
        :param name: The name of the new account conforming to account naming conventions.
        :param key: The owner public key or permission level for the new account (optional).

        :return: Exitcode and output.
        :rtype: :ref:`typing_exe_result`
        """

        if not key:
            priv, pub = self.create_key_pair()
            self.import_key(priv)
            key = pub
            self.private_keys[name] = priv

        ec, out = self.run(['cleos', '--url', self.url, 'create', 'account', owner, name, key])
        assert ec == 0
        self.keys[name] = key
        self.logger.info(f'created account: {name}')
        return ec, out

    def create_account_staked(
        self,
        owner: str,
        name: str,
        net: float = 10.0,
        cpu: float = 10.0,
        ram: int = 181920,
        key: Optional[str] = None
    ) -> ExecutionResult:
        """Create a staked eosio account.

        :param owner: The system account that authorizes the creation of a new
            account.
        :param name: The name of the new account conforming to account naming
            conventions.
        :param net: Amount of system tokens to stake to reserve network bandwith.
        :param cpu: Amount of system tokens to stake to reserve cpu time.
        :param ram: Amount of bytes of ram to buy for this account.
        :param key: The owner public key or permission level for the new account
            (optional).

        :return: Exitcode and output.
        :rtype: :ref:`typing_exe_result`
        """
        net = Asset(net, self.sys_token_supply.symbol)
        cpu = Asset(cpu, self.sys_token_supply.symbol)

        if not key:
            priv, pub = self.create_key_pair()
            self.import_key(priv)
            key = pub
            self.private_keys[name] = priv

        ec, out = self.run([
            'cleos', '--url', self.url,
            'system',
            'newaccount',
            owner,
            '--transfer',
            name, key, key,
            '--stake-net', net,
            '--stake-cpu', cpu,
            '--buy-ram-bytes', ram
        ])
        assert ec == 0
        self.keys[name] = key
        self.logger.info(f'created staked account: {name}')
        return ec, out

    def create_accounts_staked(
        self,
        owner: str,
        names: List[str],
        keys: List[str],
        net: float = 10.0,
        cpu: float = 10.0,
        ram: int = 181920
    ) -> List[ExecutionResult]:
        """Same as :func:`~pytest_eosio.LEAPTestSession.create_account_staked`,
        but takes lists of names and keys, to create the accounts in parallel,
        which is faster than calling :func:`~pytest_eosio.LEAPTestSession.creat
        e_account_staked` in a loop.

        :param owner: The system account that authorizes the creation of a new
            account.
        :param names: List of names for the new accounts conforming to account naming
            conventions.
        :param keys: List of public keys or permission levels for the new accounts.
        :param net: Amount of system tokens to stake to reserve network bandwith.
        :param cpu: Amount of system tokens to stake to reserve cpu time.
        :param ram: Amount of bytes of ram to buy for each account.

        :return: A list the results for each command execution.
        :rtype: List[ExecutionResult]
        """
        net = Asset(net, self.sys_token_supply.symbol)
        cpu = Asset(cpu, self.sys_token_supply.symbol)

        assert len(names) == len(keys)
        procs = [
            self.open_process([
                'cleos', '--url', self.url,
                'system',
                'newaccount',
                owner,
                '--transfer',
                name, key,
                '--stake-net', net,
                '--stake-cpu', cpu,
                '--buy-ram-bytes', ram
            ]) for name, key in zip(names, keys)
        ]
        results = [
            self.wait_process(proc_id, proc_stream)
            for proc_id, proc_stream in procs
        ]
        for ec, _ in results:
            assert ec == 0
        for name, key in zip(names, keys):
            self.keys[name] = key
        self.logger.info(f'created {len(names)} staked accounts.')

    def get_table(
        self,
        account: str,
        scope: str,
        table: str,
        **kwargs
    ) -> List[Dict]:
        """Get table rows from the blockchain.

        :param account: Account name of contract were table is located.
        :param scope: Table scope in LEAP name format.
        :param table: Table name.
        :param args: Additional arguments to pass to ``cleos get table`` (`cleos 
            docs <https://developers.eos.io/manuals/eos/latest/cleos/command-ref
            erence/get/table>`_).

        :return: List of rows matching query.
        :rtype: List[Dict]
        """

        done = False
        rows = []
        params = {
            'code': account,
            'scope': scope,
            'table': table,
            'json': True,
            **kwargs
        }
        while not done:
            resp = self._post(f'{self.url}/v1/chain/get_table_rows', json=params).json()
            if 'code' in resp and resp['code'] != 200:
                self.logger.critical(json.dumps(resp, indent=4))
                assert False

            self.logger.info(resp)
            rows.extend(resp['rows'])
            done = not resp['more']
            if not done:
                params['index_position'] = resp['next_key']

        return rows

    async def aget_table(
        self,
        account: str,
        scope: str,
        table: str,
        **kwargs
    ) -> List[Dict]:
        done = False
        rows = []
        _kwargs = dict(kwargs)
        for key, arg in kwargs.items():
            if (isinstance(arg, Name) or
                isinstance(arg, Asset) or
                isinstance(arg, Symbol) or
                isinstance(arg, Checksum256)):
                _kwargs[key] = str(arg)

        params = {
            'code': str(account),
            'scope': str(scope),
            'table': str(table),
            'json': True,
            **_kwargs
        }
        while not done:
            resp = (await self._apost(f'{self.url}/v1/chain/get_table_rows', json=params)).json()
            if ('code' in resp)  or ('statusCode' in resp):
                self.logger.critical(json.dumps(resp, indent=4))
                assert False

            self.logger.info(resp)
            rows.extend(resp['rows'])
            done = not resp['more']
            if not done:
                params['lower_bound'] = resp['next_key']

        return rows

    def get_info(self) -> Dict[str, Union[str, int]]:
        """Get blockchain statistics.

            - ``server_version``
            - ``head_block_num``
            - ``last_irreversible_block_num``
            - ``head_block_id``
            - ``head_block_time``
            - ``head_block_producer``
            - ``recent_slots``
            - ``participation_rate``

        :return: A dictionary with blockchain information.
        :rtype: Dict[str, Union[str, int]]
        """
        resp = self._get(f'{self.url}/v1/chain/get_info')
        assert resp.status_code == 200
        return resp.json()

    async def a_get_info(self) -> Dict[str, Union[str, int]]:
        """Get blockchain statistics.

            - ``server_version``
            - ``head_block_num``
            - ``last_irreversible_block_num``
            - ``head_block_id``
            - ``head_block_time``
            - ``head_block_producer``
            - ``recent_slots``
            - ``participation_rate``

        :return: A dictionary with blockchain information.
        :rtype: Dict[str, Union[str, int]]
        """
        resp = await self._aget(f'{self.url}/v1/chain/get_info')
        assert resp.status_code == 200
        return resp.json()

    def get_resources(self, account: str) -> List[Dict]:
        """Get account resources.

        :param account: Name of account to query resources.

        :return: A list with a single dictionary which contains, resource info.
        :rtype: List[Dict]
        """

        return self.get_table('eosio', account, 'userres')

    def new_account(
        self,
        name: Optional[str] = None,
        owner: str = 'eosio',
        **kwargs
    ) -> str:
        """Create a new account with a random key and name, import the private
        key into the wallet.

        :param name: To set a specific name and not a random one.

        :return: New account name.
        :rtype: str
        """

        if name:
            account_name = name
        else:
            account_name = random_leap_name()

        if 'key' not in kwargs:
            private_key, public_key = self.create_key_pair()
            self.import_key(private_key)
            kwargs['key'] = public_key

        self.create_account_staked(owner, account_name, **kwargs)
        return account_name

    def new_accounts(self, n: int) -> List[str]:
        """Same as :func:`~pytest_eosio.LEAPTestSession.new_account` but to
        create multiple accounts at the same time, faster than calling :func:`~p
        ytest_eosio.LEAPTestSession.new_account` on a loop.

        :param n: Number of accounts to create.

        :return: List of names of the new accounts.
        :rtype: List[str]
        """

        names = [random_leap_name() for _ in range(n)]
        keys = self.create_key_pairs(n)
        self.import_keys([priv_key for priv_key, pub_key in keys])
        self.create_accounts_staked(
            'eosio', names, [pub_key for priv_key, pub_key in keys])
        return names

    def buy_ram_bytes(
        self,
        payer: str,
        amount: int,
        receiver: Optional[str] = None
    ) -> ActionResult:
        """Buy a number of RAM bytes for an account.

        :param payer: Account to bill.
        :param amount: Amount of RAM to buy in bytes.
        :param receiver: In case its present buy RAM bytes for this account
            instead of receiver.

        :return: Exitcode and output of ``buyrambytes`` push action call.
        :rtype: :ref:`typing_action_result`
        """

        if not receiver:
            receiver = payer

        return self.push_action(
            'eosio', 'buyrambytes',
            [payer, receiver, amount],
            f'{payer}@active'
        )

    def wait_blocks(self, n: int, sleep_time: float = 0.25):
        """Busy wait till ``n`` amount of blocks are produced on the chain.

        :param n: Number of blocks to wait.
        :param sleep_time: Time to re-check chain.
        """
        def try_get_info():
            try:
                return self.get_info()
            except (
                AssertionError,
                requests.exceptions.ConnectionError
            ):
                return None

        # when waiting for nodeos to start
        info = try_get_info()
        while info == None:
            if not self.is_nodeos_running():
                ec, out = self.gather_nodeos_output()
                self.logger.error(out)
                raise AssertionError(f'Nodeos crashed with exitcode {ec}')
            time.sleep(sleep_time)
            info = try_get_info()

        start = self.get_info()['head_block_num']
        current = start
        end = start + n
        while current < end:
            self.logger.info(f'block num: {current}, remaining: {end - current}')
            time.sleep(sleep_time)
            current = self.get_info()['head_block_num']

        if hasattr(self, 'wallet_key'):
            # ensure wallet is still unlocked after wait
            ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'list'])
            assert ec == 0

            if '*' not in out:
                ec, out = self.run(
                    ['cleos', '--url', self.url, 'wallet', 'unlock', '--password', self.wallet_key]
                )
                assert ec == 0


    """Multi signature
    """

    def multi_sig_propose(
        self,
        proposer: str,
        req_permissions: List[str],
        tx_petmissions: List[str],
        contract: str,
        action_name: str,
        data: Dict[str, str]
    ) -> str:
        """Create a multi signature proposal with a random name.

        :param proposer: Account to authorize the proposal.
        :param req_permissions: List of permissions required to run proposed
            transaction.
        :param tx_permissions: List of permissions that will be applied to
            transaction when executed.
        :param contract: Contract were action is defined.
        :param action_name: Action name.
        :param data: Dictionary with transaction to execute with multi signature.

        :return: New proposal name.
        :rtype: str
        """

        proposal_name = random_leap_name()
        cmd = [
            'cleos', '--url', self.url,
            'multisig',
            'propose',
            proposal_name,
            json.dumps([
                {'actor': perm[0], 'permission': perm[1]}
                for perm in [p.split('@') for p in req_permissions]
            ]),
            json.dumps([
                {'actor': perm[0], 'permission': perm[1]}
                for perm in [p.split('@') for p in tx_petmissions]
            ]),
            contract,
            action_name,
            json.dumps(data),
            '-p', proposer
        ]
        ec, out = self.run(cmd)
        assert ec == 0

        return proposal_name

    def multi_sig_propose_tx(
        self,
        proposer: str,
        req_permissions: List[str],
        tx: Dict 
    ) -> str:
        """Create a multi signature proposal with a random name.

        :param proposer: Account to authorize the proposal.
        :param req_permissions: List of permissions required to run proposed
            transaction.
        :param tx: Dictionary with transaction to execute with multi signature.

        :return: New proposal name.
        :rtype: str
        """
        proposal_name = random_leap_name()
        cmd = [
            'cleos', '--url', self.url,
            'multisig',
            'propose_trx',
            proposal_name,
            json.dumps([
                {'actor': perm[0], 'permission': perm[1]}
                for perm in [p.split('@') for p in req_permissions]
            ]),
            tx,
            '-p', f'{proposer}@active'
        ]
        ec, out = self.run(cmd)
        assert ec == 0

        return proposal_name

    def multi_sig_approve(
        self,
        proposer: str,
        proposal_name: str,
        permissions: List[str],
        approver: str
    ) -> ActionResult:
        """Approve a multisig proposal.

        :param proposer: Account that created the proposal.
        :param proposal_name: Name of the proposal.
        :param permissions: List of permissions to append to proposal.
        :param approver: Permissions to run this action.

        :return: Exitcode and output of ``multisig.approve`` push action call.
        :rtype: :ref:`typing_action_result`
        """

        cmd = [
            'cleos', '--url', self.url,
            'multisig',
            'approve',
            proposer,
            proposal_name,
            *[
                json.dumps({'actor': perm[0], 'permission': perm[1]})
                for perm in [p.split('@') for p in permissions]
            ],
            '-p', approver
        ]
        ec, out = self.run(cmd)
        return ec, out

    def multi_sig_exec(
        self,
        proposer: str,
        proposal_name: str,
        permission: str,
        wait: int = 3
    ) -> ActionResult:
        """Execute multi signature transaction proposal.

        :param proposer: Account that created the proposal.
        :param proposal_name: Name of the proposal.
        :param permission: Permissions to run this action.
        :param wait: Number of blocks to wait after executing.

        :return: Exitcode and output of ``multisig.exec`` push action call.
        :rtype: :ref:`typing_action_result`
        """

        cmd = [
            'cleos', '--url', self.url,
            'multisig',
            'exec',
            proposer,
            proposal_name,
            '-p', permission
        ]

        if 'leap' in DEFAULT_NODEOS_IMAGE:
            cmd += ['--use-old-rpc', '-t', 'false']

        ec, out = self.run(cmd)

        if ec == 0:
            self.wait_blocks(wait)

        return ec, out

    def multi_sig_review(
        self,
        proposer: str,
        proposal_name: str
    ) -> ActionResult:
        """Review a multisig proposal.

        :param proposer: Account that created the proposal.
        :param proposal_name: Name of the proposal.

        :return: Exitcode and output of ``multisig.review`` push action call.
        :rtype: :ref:`typing_action_result`
        """

        cmd = [
            'cleos', '--url', self.url,
            'multisig',
            'review',
            proposer,
            proposal_name
        ]
        ec, out =  self.run(cmd)
        return ec, out

    """Token managment
    """

    def get_token_stats(
        self,
        sym: str,
        token_contract: str = 'eosio.token'
    ) -> Dict[str, str]:
        """Get token statistics.

        :param sym: Token symbol.
        :param token_contract: Token contract.

        :return: A dictionary with ``\'supply\'``, ``\'max_supply\'`` and
            ``\'issuer\'`` as keys.
        :rtype: Dict[str, str]
        """

        return self.get_table(
            token_contract,
            sym,
            'stat'
        )[0]

    def get_balance(
        self,
        account: str,
        token_contract: str = 'eosio.token'
    ) -> Optional[str]:
        """Get account balance.

        :param account: Account to query.
        :param token_contract: Token contract.

        :return: Account balance in asset form, ``None`` if user has no balance
            entry.
        :rtype: Optional[str]
        """

        balances = self.get_table(
            token_contract,
            account,
            'accounts'
        )
        if len(balances) == 1:
            return balances[0]['balance']

        elif len(balances) > 1:
            return balances

        else:
            return None 

    def create_token(
        self,
        issuer: str,
        max_supply: Union[str, Asset],
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """Create a new token issued by ``issuer``.

        :param issuer: Account that issues new token.
        :param max_supply: Max token supply in asset form.
        :param token_contract: Token contract.

        :return: ``token_contract.create`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'create',
            [issuer, str(max_supply)],
            'eosio.token',
            **kwargs
        )

    def issue_token(
        self,
        issuer: str,
        quantity: Union[str, Asset],
        memo: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """Issue a specific quantity of tokens.

        :param issuer: Account that issues new tokens.
        :param quantity: Quantity of tokens to issue in asset form.
        :param memo: Memo string to attach to transaction.
        :param token_contract: Token contract.

        :return: ``token_contract.issue`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'issue',
            [issuer, str(quantity), memo],
            f'{issuer}@active',
            **kwargs
        )

    def transfer_token(
        self,
        _from: str,
        _to: str,
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """Transfer tokens.

        :param _from: Account that sends the tokens.
        :param _to: Account that recieves the tokens.
        :param quantity: Quantity of tokens to issue in asset form.
        :param memo: Memo string to attach to transaction.
        :param token_contract: Token contract.

        :return: ``token_contract.issue`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'transfer',
            [_from, _to, str(quantity), memo],
            f'{_from}@active',
            **kwargs
        )

    def give_token(
        self,
        _to: str,
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract='eosio.token',
        **kwargs
    ) -> ActionResult:
        """Transfer tokens from token contract to an account.

        :param _to: Account that recieves the tokens.
        :param quantity: Quantity of tokens to issue in asset form.
        :param memo: Memo string to attach to transaction.
        :param token_contract: Token contract.

        :return: ``token_contract.issue`` execution result.
        :rtype: :ref:`typing_action_result`
        """
        return self.transfer_token(
            'eosio',
            _to,
            str(quantity),
            memo,
            token_contract=token_contract,
            **kwargs
        )

    def retire_token(
        self,
        issuer: str,
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """Retire tokens.

        :param issuer: Account that retires the tokens.
        :param quantity: Quantity of tokens to retire in asset form.
        :param memo: Memo string to attach to transaction.
        :param token_contract: Token contract.

        :return: ``token_contract.retire`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'retire',
            [str(quantity), memo],
            f'{issuer}@active',
            **kwargs
        )

    def open_token(
        self,
        owner: str,
        sym: Union[str, Symbol],
        ram_payer: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """Allows `ram_payer` to create an account `owner` with zero balance for
        token `sym` at the expense of `ram_payer`.

        :param owner: the account to be created,
        :param sym: the token to be payed with by `ram_payer`,
        :param ram_payer: the account that supports the cost of this action.
        :param token_contract: Token contract.

        :return: ``token_contract.open`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'open',
            [owner, sym, ram_payer],
            f'{ram_payer}@active',
            **kwargs
        )

    def close_token(
        self,
        owner: str,
        sym: Union[str, Symbol],
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> ActionResult:
        """This action is the opposite for open, it closes the account `owner`
        for token `sym`.

        :param owner: the owner account to execute the close action for,
        :param symbol: the symbol of the token to execute the close action for.

        :return: ``token_contract.close`` execution result.
        :rtype: :ref:`typing_action_result`
        """

        return self.push_action(
            token_contract,
            'close',
            [owner, sym],
            f'{owner}@active',
            **kwargs
        )


    def init_sys_token(
        self,
        token_sym: Symbol = DEFAULT_SYS_TOKEN_SYM,
        token_amount: int = 420_000_000
    ):
        """Initialize ``SYS`` token.

        Issue all of it to ``eosio`` account.
        """
        if not self._sys_token_init:

            self.sys_token_supply = Asset(token_amount, token_sym)

            ec, _ = self.create_token('eosio', self.sys_token_supply)
            assert ec == 0

            ec, _ = self.issue_token('eosio', self.sys_token_supply, __name__)
            assert ec == 0

            self._sys_token_init = True

    def get_global_state(self):
        return self.get_table(
            'eosio', 'eosio', 'global')[0]

    def rex_deposit(self, owner: str, quantity: str):
        return self.push_action(
            'eosio',
            'deposit',
            [owner, quantity],
            f'{owner}@active'
        )

    def rex_buy(self, _from: str, quantity: str):
        return self.push_action(
            'eosio',
            'buyrex',
            [_from, quantity],
            f'{_from}@active'
        )

    def delegate_bandwidth(
        self,
        _from: str,
        _to: str,
        net: str,
        cpu: str
    ):
        return self.push_action(
            'eosio',
            'delegatebw',
            [_from, _to, net, cpu, 0],
            f'{_from}@active'
        )

    def register_producer(
        self,
        producer: str,
        url: str = '',
        location: int = 0
    ):
        return self.push_action(
            'eosio',
            'regproducer',
            [producer, self.keys[producer], url, location],
            f'{producer}@active'
        )

    def vote_producers(
        self, 
        voter: str,
        proxy: str,
        producers: List[str]
    ):
        return self.push_action(
            'eosio',
            'voteproducer',
            [voter, proxy, producers],
            f'{voter}@active'
        )

    def claim_rewards(self, owner: str):
        return self.push_action(
            'eosio',
            'claimrewards',
            [owner],
            f'{owner}@active'
        )

    def get_schedule(self):
        ec, out = self.run([
            'cleos', '--url', self.url, 'get', 'schedule', '-j'])

        if ec == 0:
            return json.loads(out) 
        else:
            return None

    def get_producers(self):
        return self.get_table(
            'eosio',
            'eosio',
            'producers'
        )

    def get_producer(self, producer: str) -> Optional[Dict]:
        rows = self.get_table(
            'eosio', 'eosio', 'producers',
            '--key-type', 'name', '--index', '1',
            '--lower', producer,
            '--upper', producer)

        if len(rows) == 0:
            return None
        else:
            return rows[0]

    def get_payrate(self, producer: str) -> Optional[Dict]:
        rows = self.get_table(
            'eosio', 'eosio', 'payrate')

        if len(rows) == 0:
            return None
        else:
            return rows[0]

    def wrap_exec(
        self,
        executer: str,
        tx: Dict,
        **kwargs
    ):
        return self.push_action(
            'eosio.wrap',
            'exec',
            [executer, tx],
            f'{executer}@active',
            **kwargs
        )
