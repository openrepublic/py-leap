#!/usr/bin/env python3

import time
import json
import select
import logging
import requests

from typing import (
    Dict,
    List,
    Union,
    Tuple,
    Optional,
    Iterator
)
from pathlib import Path
from difflib import SequenceMatcher

from docker.client import DockerClient
from docker.models.containers import Container

from .init import (
    sys_token_supply,
    sys_token_init_issue,
    ram_supply
)
from .sugar import (
    collect_stdout,
    random_eosio_name,
    wait_for_attr,
    asset_from_str,
    Asset,
    Symbol,
    docker_open_process,
    docker_wait_process
)
from .tokens import sys_token, ram_token
from .typing import (
    ExecutionResult, 
    ExecutionStream,
    ActionResult
)


class CLEOS:

    def __init__(
        self,
        docker_client: DockerClient, 
        vtestnet: Container,
        url: str = 'http://127.0.0.1:8888',
        logger = None
    ):

        self.client = docker_client
        self.vtestnet = vtestnet
        self.url = url

        if logger is None:
            self.logger = logging.getLogger('cleos')
        else:
            self.logger = logger

        ports = wait_for_attr(
            self.vtestnet,
            ('NetworkSettings', 'Ports', '8888/tcp'))

        container_port = ports[0]['HostPort']
        self.endpoint = f'http://localhost:{container_port}'

        self._sys_token_init = False
    
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

        self.logger.info(f'exec run: {cmd}')
        for i in range(1, 2 + retry):
            ec, out = self.vtestnet.exec_run(
                [str(chunk) for chunk in cmd], *args, **kwargs)
            if ec == 0:
                break

            self.logger.warning(f'cmd run retry num {i}...')
               
        if ec != 0:
            self.logger.error('command exited with non zero status:')
            self.logger.error(out.decode('utf-8'))

        return ec, out.decode('utf-8')

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

    def start_nodeos(
        self,
        plugins: List[str] = [
            'producer_plugin',
            'producer_api_plugin',
            'chain_api_plugin',
            'http_plugin',
            'history_plugin',
            'history_api_plugin'
        ],
        http_addr: str = '0.0.0.0:8888',
        p2p_addr: str = '0.0.0.0:9876'
    ):
        exec_id, exec_stream = self.open_process(
            ['nodeos',
                '-e',
                '-p', 'eosio',
                *[f'--plugin={p}' for p in plugins],
                f'--http-server-address=\'{http_addr}\'',
                f'--p2p-listen-endpoint=\'{p2p_addr}\'',
                '--abi-serializer-max-time-ms=999999',
                '--filter-on=\'*\'',
                '--access-control-allow-origin=\'*\'',
                '--contracts-console',
                '--http-validate-host=false',
                '--verbose-http-errors'
        ])
        self.__nodeos_exec_id = exec_id
        self.__nodeos_exec_stream = exec_stream

    def start_nodeos_from_config(
        self,
        config: str,
        state_plugin: bool = False,
        genesis: Optional[str] = None,
        data_dir: str = '/mnt/dev/data',
        snapshot: Optional[str] = None,
        logfile: str = '/root/nodeos.log'
    ):
        cmd = [
            'nodeos',
            '-e',
            '-p', 'eosio',
            f'--data-dir={data_dir}',
            f'--config={config}' 
        ]
        if state_plugin:
            # https://github.com/EOSIO/eos/issues/6334
            cmd += ['--disable-replay-opts']

        if genesis:
            cmd += [f'--genesis-json={genesis}']

        if snapshot:
            cmd += [f'--snapshot={snapshot}']

        final_cmd = ' '.join(cmd) + f' > {logfile} 2>&1'
        final_cmd = ['/bin/bash', '-c', final_cmd]

        self.logger.info(f'starting nodeos with cmd: {final_cmd}')
        exec_id, exec_stream = self.open_process(final_cmd)
        self.__nodeos_exec_id = exec_id
        self.__nodeos_exec_stream = exec_stream

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

    def wait_produced(self, from_file: Optional[str] = None):
        if from_file:
            exec_id, exec_stream = docker_open_process(
                    self.client, self.vtestnet,
                    ['/bin/bash', '-c',
                    f'tail -f {from_file}'])
        else:
            exec_id = self.__nodeos_exec_id
            exec_stream = self.__nodeos_exec_stream
        
        for chunk in exec_stream:
            msg = chunk.decode('utf-8')
            self.logger.info(msg.rstrip())
            if 'Produced' in msg:
                break

    def wait_received(self, from_file: Optional[str] = None):
        if from_file:
            exec_id, exec_stream = docker_open_process(
                    self.client, self.vtestnet,
                    ['/bin/bash', '-c',
                    f'tail -f {from_file}'])
        else:
            exec_id = self.__nodeos_exec_id
            exec_stream = self.__nodeos_exec_stream
        
        for chunk in exec_stream:
            msg = chunk.decode('utf-8')
            self.logger.info(msg.rstrip())
            if 'Received' in msg:
                break

    def deploy_contract(
        self,
        contract_name: str,
        build_dir: str,
        privileged: bool = False,
        account_name: Optional[str] = None,
        create_account: bool = True,
        staked: bool = True
    ):
        """Deploy a built contract.

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
            raise FileNotFoundError(
                f'Couldn\'t find {contract_name}.wasm')

        wasm_path = matches[0]
        wasm_file = str(wasm_path).split('/')[-1]
        abi_file = wasm_file.replace('.wasm', '.abi')

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
            '-p', f'{account_name}@active'
        ]
        
        self.logger.info('contract deploy: ')
        ec, out = self.run(cmd, retry=6)
        self.logger.info(out)

        if ec == 0:
            self.logger.info('deployed')

        else:
            raise AssertionError(f'Couldn\'t deploy {account_name} contract.')


    def boot_sequence(
        self,
        sys_contracts_mount='/usr/opt/telos.contracts/contracts'
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

            3) Initialize the ``TLOS`` token.
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
            'eosio.rex'
        ]:
            ec, _ = self.create_account('eosio', name)
            assert ec == 0 

        self.deploy_contract(
            'eosio.token',
            f'{sys_contracts_mount}/eosio.token',
            staked=False
        )

        self.deploy_contract(
            'eosio.msig',
            f'{sys_contracts_mount}/eosio.msig',
            staked=False
        )

        self.deploy_contract(
            'eosio.wrap',
            f'{sys_contracts_mount}/eosio.wrap',
            staked=False
        )

        self.init_sys_token()

        self.activate_feature_v1('PREACTIVATE_FEATURE')

        self.deploy_contract(
            'eosio.system',
            f'{sys_contracts_mount}/eosio.system',
            account_name='eosio',
            create_account=False
        )

        self.activate_feature('ONLY_BILL_FIRST_AUTHORIZER')
        self.activate_feature('RAM_RESTRICTIONS')

        ec, _ = self.push_action(
            'eosio', 'setpriv',
            ['eosio.msig', 1],
            'eosio@active'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio', 'init',
            ['0', sys_token],
            'eosio@active'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio', 'setram',
            [ram_supply.amount],
            'eosio@active'
        )
        assert ec == 0

    def create_key_pair(self) -> Tuple[str, str]:
        """Generate a new EOSIO key pair.

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
        """Generate ``n`` EOSIO key pairs, faster than calling
        :func:`~pytest_eosio.EOSIOTestSession.create_key_pair` on a loop.

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
        assert ec == 0
        self.logger.info('key imported')

    def import_keys(self, private_keys: List[str]):
        """Import a list of private keys into wallet inside testnet container.
        Faster than calling :func:`~pytest_eosio.EOSIOTestSession.import_key` on a loop.
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
        wallet_key = out.split('\n')[-2].strip('\"')
        assert ec == 0
        assert len(wallet_key) == 53
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
            ['cleos', '--url', self.url, 'wallet', 'unlock', '--password', wallet_key]
        )
        assert ec == 0

        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'list'])
        assert ec == 0
        assert 'default *' in out
        self.logger.info('wallet unlocked')

        # Step 4:  Import keys into your wallet
        self.logger.info('import key...')
        ec, out = self.run(['cleos', '--url', self.url, 'wallet', 'create_key'])
        public_key = out.split('\"')[1]
        assert ec == 0
        assert len(public_key) == 53
        self.dev_wallet_pkey = public_key
        self.logger.info(f'imported {public_key}')

        # Step 5: Import the Development Key
        self.logger.info('import development key...')
        self.import_key(dev_key)
        self.logger.info('imported dev key')

    def list_keys(self):
        return self.run(
            ['cleos', 'wallet', 'list'])

    def get_feature_digest(self, feature_name: str) -> str:
        """Given a feature name, query the v1 API endpoint: 
        
            ``/v1/producer/get_supported_protocol_features``

        to retrieve hash digest.

        :return: Feature hash digest.
        :rtype: str
        """
        r = requests.post(
            f'{self.endpoint}/v1/producer/get_supported_protocol_features',
            json={}
        )
        for item in r.json():
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
        r = requests.post(
            f'{self.endpoint}/v1/producer/schedule_protocol_feature_activations',
            json={
                'protocol_features_to_activate': [digest]
            }
        ).json()
        
        self.logger.info(json.dumps(r, indent=4))

        assert 'result' in r
        assert r['result'] == 'ok'

        self.logger.info(f'{digest} active.')

    def activate_feature(self, feature_name: str):
        """Given a v2 feature name, activate it.
        """

        self.logger.info(f'activating {feature_name}...')
        digest = self.get_feature_digest(feature_name)
        ec, _ = self.push_action(
            'eosio', 'activate',
            [digest],
            'eosio@active'
        )
        assert ec == 0
        self.logger.info(f'{digest} active.')

    def get_ram_price(self) -> Asset:
        row = self.get_table(
            'eosio', 'eosio', 'rammarket')[0]

        quote = asset_from_str(row['quote']['balance']).amount
        base = asset_from_str(row['base']['balance']).amount

        return Asset((quote / base) * 1024 / 0.995, Symbol('TLOS', 4)) 

    def push_action(
        self,
        contract: str,
        action: str,
        args: List[str],
        permissions: str,
        retry: int = 3
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
        self.logger.info(f"push action: {action}({args}) as {permissions}")
        cmd = [
            'cleos', '--url', self.url, 'push', 'action', contract, action,
            json.dumps(args), '-p', permissions, '-j', '-f'
        ]
        ec, out = self.run(cmd, retry=retry)
        try:
            out = json.loads(out)
            self.logger.info(collect_stdout(out))
            
        except (json.JSONDecodeError, TypeError):
            self.logger.error(f'\n{out}')
            self.logger.error(f'cmd line: {cmd}')

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
            key = self.dev_wallet_pkey
        ec, out = self.run(['cleos', '--url', self.url, 'create', 'account', owner, name, key])
        assert ec == 0
        self.logger.info(f'created account: {name}')
        return ec, out

    def create_account_staked(
        self,
        owner: str,
        name: str,
        net: str = '10.0000 TLOS',
        cpu: str = '10.0000 TLOS',
        ram: int = 81920,
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
        if key is None:
            key = self.dev_wallet_pkey

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
        self.logger.info(f'created staked account: {name}')
        return ec, out

    def create_accounts_staked(
        self,
        owner: str,
        names: List[str],
        keys: List[str],
        net: str = '10.0000 TLOS',
        cpu: str = '10.0000 TLOS',
        ram: int = 8192
    ) -> List[ExecutionResult]:
        """Same as :func:`~pytest_eosio.EOSIOTestSession.create_account_staked`,
        but takes lists of names and keys, to create the accounts in parallel,
        which is faster than calling :func:`~pytest_eosio.EOSIOTestSession.creat
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
        self.logger.info(f'created {len(names)} staked accounts.')

    def get_table(
        self,
        account: str,
        scope: str,
        table: str,
        *args
    ) -> List[Dict]:
        """Get table rows from the blockchain.

        :param account: Account name of contract were table is located.
        :param scope: Table scope in EOSIO name format.
        :param table: Table name.
        :param args: Additional arguments to pass to ``cleos get table`` (`cleos 
            docs <https://developers.eos.io/manuals/eos/latest/cleos/command-ref
            erence/get/table>`_).

        :return: List of rows matching query.
        :rtype: List[Dict]
        """

        done = False
        rows = []
        while not done:
            ec, out = self.run([
                'cleos', '--url', self.url, 'get', 'table',
                account, scope, table,
                '-l', '1000', *args
            ])
            if ec != 0:
                self.logger.critical(out)

            assert ec == 0
            out = json.loads(out)
            rows.extend(out['rows']) 
            done = not out['more']

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

        ec, out = self.run(['cleos', '--url', self.url, 'get', 'info'])
        assert ec == 0
        return json.loads(out)

    def get_resources(self, account: str) -> List[Dict]:
        """Get account resources.

        :param account: Name of account to query resources.

        :return: A list with a single dictionary which contains, resource info.
        :rtype: List[Dict]
        """

        return self.get_table('eosio', account, 'userres')

    def new_account(self, name: Optional[str] = None, **kwargs) -> str:
        """Create a new account with a random key and name, import the private
        key into the wallet.

        :param name: To set a specific name and not a random one.

        :return: New account name.
        :rtype: str
        """

        if name:
            account_name = name
        else:
            account_name = random_eosio_name()

        if 'key' not in kwargs:
            private_key, public_key = self.create_key_pair()
            self.import_key(private_key)
            kwargs['key'] = public_key

        self.create_account_staked('eosio', account_name, **kwargs)
        return account_name

    def new_accounts(self, n: int) -> List[str]:
        """Same as :func:`~pytest_eosio.EOSIOTestSession.new_account` but to
        create multiple accounts at the same time, faster than calling :func:`~p
        ytest_eosio.EOSIOTestSession.new_account` on a loop.

        :param n: Number of accounts to create.

        :return: List of names of the new accounts.
        :rtype: List[str]
        """

        names = [random_eosio_name() for _ in range(n)]
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
            except AssertionError:
                return None

        # when waiting for nodeos to start
        while (info := try_get_info()) == None:
            time.sleep(sleep_time)
    
        start = self.get_info()['head_block_num']
        while (info := self.get_info())['head_block_num'] - start < n:
            time.sleep(sleep_time)

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

        proposal_name = random_eosio_name()
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
        else:
            return None

    def create_token(
        self,
        issuer: str,
        max_supply: Union[str, Asset],
        token_contract: str = 'eosio.token'
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
            'eosio.token'
        )

    def issue_token(
        self,
        issuer: str,
        quantity: Union[str, Asset],
        memo: str,
        token_contract: str = 'eosio.token'
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
            f'{issuer}@active'
        )

    def transfer_token(
        self,
        _from: str,
        _to: str,
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract: str = 'eosio.token'
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
            f'{_from}@active'
        )

    def give_token(
        self,
        _to: str,
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract='eosio.token'
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
            token_contract,
            _to,
            str(quantity),
            memo,
            token_contract=token_contract
        )

    def init_sys_token(self):
        """Initialize ``SYS`` token.

        Issue all of it to ``eosio`` account.
        """

        if not self._sys_token_init:
            self._sys_token_init = True
            ec, _ = self.create_token('eosio', sys_token_supply)
            assert ec == 0
            ec, _ = self.issue_token('eosio', sys_token_init_issue, __name__)
            assert ec == 0

