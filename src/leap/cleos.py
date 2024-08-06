#!/usr/bin/env python3

import sys
import time
import base64
import logging
import requests
import binascii
import json as json_module

from copy import deepcopy
from typing import Any
from pathlib import Path
from hashlib import sha256
from urllib3.util.retry import Retry

from datetime import datetime, timedelta

import asks

from requests.adapters import HTTPAdapter

from .sugar import random_leap_name
from .errors import ChainHTTPError, ChainAPIError, ContractDeployError, TransactionPushError
from .tokens import DEFAULT_SYS_TOKEN_CODE, DEFAULT_SYS_TOKEN_SYM
from .protocol import *


# disable warnings about connection retries
logging.getLogger("urllib3").setLevel(logging.ERROR)


class CLEOS:
    '''Leap http client

    :param url: node endpoint
    :type url: str
    :param remote: endpoint used to verify contracts against and clone activations
    :type remote: str
    :param logger: optional logger, will create one named cleos if none
    :type logger: logging.Logger
    '''

    def __init__(
        self,
        endpoint: str = 'http://127.0.0.1:8888',
        logger = None
    ):
        if logger is None:
            self.logger = logging.getLogger('cleos')
        else:
            self.logger = logger

        self.endpoint = endpoint

        self.keys: dict[str, str] = {}
        self.private_keys: dict[str, str] = {}
        self._key_to_acc: dict[str, list[str]] = {}

        self._loaded_abis: dict[str, dict] = {}

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

        self._asession = asks.Session(connections=200)

    # local abi store methods

    def load_abi(self, account: str, abi: dict):
        self._loaded_abis[account] = abi

    def load_abi_file(self, account: str, abi_path: str | Path):
        with open(abi_path, 'rb') as abi_file:
            self.load_abi(account, json_module.load(abi_file))

    def get_loaded_abi(self, account: str) -> dict:
        if account not in self._loaded_abis:
            raise ValueError(f'ABI for {account} not loaded!')

        return self._loaded_abis[account]

    # generic http+session handlers

    def _unwrap_response(self, maybe_error: Any) -> dict:
        if not hasattr(maybe_error, 'json'):
            return maybe_error

        maybe_error = maybe_error.json()

        if ChainHTTPError.is_json_error(maybe_error):
            err = ChainAPIError.from_json(maybe_error['error'])
            self.logger.error(repr(err))
            raise err

        return maybe_error

    def _session_method(
        self,
        method: str,
        route: str,
        *args,
        base_route: str | None = None,
        is_async: bool = False,
        params: dict | None = None,
        json: dict | None = None,
        data: str | None = None,
        headers: dict[str, str] = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        },
        **kwargs
    ):
        if not isinstance(base_route, str):
            base_route = self.endpoint

        _data = '{}'
        if isinstance(data, str):
            _data = data

        elif isinstance(json, dict):
            _data = json_module.dumps(json)

        elif isinstance(params, dict):
            _data = json_module.dumps(params)

        kwargs['data'] = _data

        session = self._asession if is_async else self._session
        return getattr(session, method)(base_route + route, *args, headers=headers, **kwargs)

    def _get(self, *args, **kwargs):
        return self._unwrap_response(
            self._session_method('get', *args, is_async=False, **kwargs))

    def _post(self, *args, **kwargs):
        return self._unwrap_response(
            self._session_method('post', *args, is_async=False, **kwargs))

    async def _async_get(self, *args, **kwargs):
        return self._unwrap_response(
            await self._session_method('get', *args, is_async=True, **kwargs))

    async def _async_post(self, *args, **kwargs):
        return self._unwrap_response(
            await self._session_method('post', *args, is_async=True, **kwargs))

    # tx send machinery

    def _get_abis_for_actions(self, actions: list[dict]) -> dict[str, dict]:
        return {
            action['account']: self.get_loaded_abi(action['account'])
            for action in actions
        }

    def _push_tx(
        self,
        tx: dict,
        retries: int = 2
    ) -> dict:
        res = {}
        for i in range(1, retries + 1):
            try:
                return self._post(
                    '/v1/chain/push_transaction', json=tx)

            except ChainAPIError as err:
                if i == retries:  # that was last retry, raise
                    raise TransactionPushError.from_other(err)

                else:
                    continue

    async def _a_push_tx(
        self,
        tx: dict,
        retries: int = 2
    ) -> dict:
        res = {}
        for i in range(1, retries + 1):
            try:
                return await self._apost(
                    '/v1/chain/push_transaction', is_async=True, json=tx)

            except ChainAPIError as err:
                if i == retries:  # that was last retry, raise
                    raise TransactionPushError.from_other(err)

                else:
                    continue

    def _create_signed_tx(
        self,
        actions: list[dict],
        key: str,
        **kwargs
    ):
        chain_info = self.get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id: str = chain_info['chain_id']
        abis: dict[str, dict] = self._get_abis_for_actions(actions)

        return create_and_sign_tx(chain_id, abis, actions, key, **kwargs)

    async def _a_create_signed_tx(
        self,
        actions: list[dict],
        key: str,
        **kwargs
    ):
        chain_info = await self.a_get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id: str = chain_info['chain_id']
        abis: dict[str, dict] = self._get_abis_for_actions(actions)

        return create_and_sign_tx(chain_id, abis, actions, key, **kwargs)

    def push_actions(
        self,
        actions: list[dict],
        key: str,
        retries: int = 2,
        **kwargs
    ):
        '''Async push actions, uses a single tx for all actions.

        :param actions: list of actions
        :type actions: str
        :param key: private key used to sign
        :type key: str
        '''
        tx = self._create_signed_tx(actions, key, **kwargs)
        return self._push_tx(tx, retries=retries)

    def push_action(
        self,
        account: str,
        action: str,
        data: list,
        actor: str,
        key: str | None = None,
        permission: str = 'active',
        **kwargs
    ):
        '''Async push action

        :param account: smart contract account name
        :type account: str
        :param action: smart contract action name
        :type action: str
        :param data: action data
        :type data: list
        :param key: private key used to sign
        :type key: str
        :param permission: permission name
        :type permission: str
        '''
        if not isinstance(key, str):
            key = self.get_private_key(account)

        return self.push_actions([{
            'account': account,
            'name': action,
            'data': data,
            'authorization': [{
                'actor': actor,
                'permission': permission
            }]
        }], key, **kwargs)

    async def a_push_actions(
        self,
        actions: list[dict],
        key: str,
        retries: int = 2,
        **kwargs
    ):
        '''Async push actions, uses a single tx for all actions.

        :param actions: list of actions
        :type actions: str
        :param key: private key used to sign
        :type key: str
        '''
        tx = await self._a_create_signed_tx(actions, key, **kwargs)
        return await self._a_push_tx(tx, retries=retries)

    async def a_push_action(
        self,
        account: str,
        action: str,
        data: list,
        actor: str,
        key: str | None = None,
        permission: str = 'active',
        **kwargs
    ):
        '''Async push action

        :param account: smart contract account name
        :type account: str
        :param action: smart contract action name
        :type action: str
        :param data: action data
        :type data: list
        :param key: private key used to sign
        :type key: str
        :param permission: permission name
        :type permission: str
        '''
        if not isinstance(key, str):
            key = self.get_private_key(account)

        return await self.a_push_actions([{
            'account': account,
            'name': action,
            'data': data,
            'authorization': [{
                'actor': actor,
                'permission': permission
            }]
        }], key, **kwargs)

    # system action helpers

    def add_permission(
        self,
        account: str,
        permission: str,
        parent: str,
        auth: dict
    ):
        '''Add permission to an account

        :param account: account name
        :type account: str
        :param permission: permission name
        :type permission: str
        :param parent: parent account name
        :type parent: str
        :param auth: authority schema
        :type auth: dict
        '''
        return self.push_action(
            'eosio',
            'updateauth',
            [
                account,
                permission,
                parent,
                auth
            ],
            account,
            key=self.get_private_key(account)
        )

    def deploy_contract(
        self,
        account_name: str,
        wasm: bytes,
        abi: dict,
        privileged: bool = False,
        create_account: bool = True,
        staked: bool = True
    ):
        '''Deploy a built contract.

        :param account_name: Name of account to deploy contract at.
        :type account_name: str
        :param wasm: Raw wasm as bytearray
        :type wasm: bytes
        :param abi: Json abi as dict
        :type abi: dict
        :param privileged: ``True`` if contract should be privileged (system
            contracts).
        :type privileged: bool
        :param create_account: ``True`` if target account should be created.
        :type create_account: bool
        :param staked: ``True`` if this account should use RAM & NET resources.
        :type staked: bool
        :type verify_hash: bool
        '''

        if create_account:
            if staked:
                self.create_account_staked('eosio', account_name)
            else:
                self.create_account('eosio', account_name)
            self.logger.info(f'created account {account_name}')

        self.wait_blocks(1)

        if privileged:
            self.push_action(
                'eosio', 'setpriv',
                [account_name, 1],
                'eosio'
            )

        self.wait_blocks(1)

        self.add_permission(
            account_name,
            'active', 'owner',
            {
                'threshold': 1,
                'keys': [{'key': self.keys[account_name], 'weight': 1}],
                'accounts': [{
                    'permission': {'actor': account_name, 'permission': 'eosio.code'},
                    'weight': 1
                }],
                'waits': []
            }
        )
        self.logger.info('gave eosio.code permissions')

        local_shasum = sha256(wasm).hexdigest()
        self.logger.info(f'contract hash: {local_shasum}')

        self.logger.info(f'loading abi...')
        self.load_abi(account_name, abi)

        self.logger.info('deploy...')

        actions = [{
            'account': 'eosio',
            'name': 'setcode',
            'data': [
                account_name,
                0, 0,
                wasm
            ],
            'authorization': [{
                'actor': account_name,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'setabi',
            'data': [
                account_name,
                abi
            ],
            'authorization': [{
                'actor': account_name,
                'permission': 'active'
            }]
        }]

        try:
            res = self.push_actions(
                actions, self.private_keys[account_name])

            self.logger.info('deployed')
            return res

        except TransactionPushError as err:
            raise ContractDeployError.from_other(err)

    def deploy_contract_from_path(
        self,
        account_name: str,
        contract_path: str | Path,
        contract_name: str | None = None,
        **kwargs
    ):
        if not contract_name:
            contract_name = Path(contract_path).parts[-1]

        # will fail if not found
        contract_path = Path(contract_path).resolve(strict=True)

        wasm = b''
        with open(contract_path / f'{contract_name}.wasm', 'rb') as wasm_file:
            wasm = wasm_file.read()

        abi = None
        with open(contract_path / f'{contract_name}.abi', 'rb') as abi_file:
            abi = json_module.load(abi_file)

        return self.deploy_contract(
            account_name, wasm, abi, **kwargs)

    def get_account(
        self,
        account_name: str
    ) -> dict:
        return self._post(
            '/v1/chain/get_account',
            json={
                'account_name': account_name
            }
        )

    def get_code(
        self,
        account_name: str
    ) -> tuple[str, bytes]:
        '''Fetches and decodes the WebAssembly (WASM) code for a given account.

        :param account_name: Account to get the WASM code for
        :type account_name: str
        :return: A tuple containing the hash and the decoded WASM code.
        :rtype: tuple[str, bytes]
        '''
        resp = self._post(
            '/v1/chain/get_raw_code_and_abi',
            json={
                'account_name': account_name
            }
        )

        wasm = base64.b64decode(resp['wasm'])
        wasm_hash = sha256(wasm).hexdigest()

        return wasm_hash, wasm

    def get_abi(self, account_name: str) -> dict:
        '''Fetches the ABI (Application Binary Interface) for a given account.

        :param account_name: Account to get the ABI for
        :type account_name: str
        :return: An dictionary containing the ABI data.
        :rtype: dict
        '''
        resp = self._post(
            '/v1/chain/get_abi',
            json={
                'account_name': account_name
            }
        )

        return resp['abi']

    def create_snapshot(self, body: dict):
        return self._post(
            '/v1/producer/create_snapshot',
            json=body
        )

    def schedule_snapshot(self, body: dict):
        return self._post(
            '/v1/producer/schedule_snapshot',
            json=body
        )

    def get_node_activations(self) -> list[dict]:
        lower_bound = 0
        step = 250
        more = True
        features = []
        while more:
            resp = self._post(
                '/v1/chain/get_activated_protocol_features',
                json={
                    'limit': step,
                    'lower_bound': lower_bound,
                    'upper_bound': lower_bound + step
                }
            )

            features += resp['activated_protocol_features']
            lower_bound += step
            more = 'more' in resp

        # sort in order of activation
        features = sorted(features, key=lambda f: f['activation_ordinal'])
        features.pop(0)  # remove PREACTIVATE_FEATURE

        return features

    def clone_activations(self, other_cleos: 'CLEOS'):
        '''Clones the activated protocol features from the remote to the current node.
        '''

        features = other_cleos.get_node_activations()

        feature_names = [
            feat['specification'][0]['value']
            for feat in features
        ]

        self.logger.info('activating features:')
        self.logger.info(
            json_module.dumps(feature_names, indent=4))

        actions = [{
            'account': 'eosio',
            'name': 'activate',
            'data': [f['feature_digest']],
            'authorization': [{
                'actor': 'eosio',
                'permission': 'active'
            }]
        } for f in features]

        self.push_actions(actions, self.private_keys['eosio'])
        self.logger.info('activated')

    def diff_protocol_activations(self, other_cleos: 'CLEOS'):
        '''Compares the activated protocol features between the remote and local endpoints.
        '''

        features_one = self.get_node_activations()
        features_two = other_cleos.get_node_activations()

        features_one_names = [
            feat['specification'][0]['value']
            for feat in features_one
        ]
        features_two_names = [
            feat['specification'][0]['value']
            for feat in features_two
        ]

        return list(set(features_one_names) - set(features_two_names))

    def download_contract(
        self,
        account_name: str,
        download_location: str | Path,
        local_name: str | None = None,
        abi: dict | None = None
    ):
        '''Downloads the smart contract associated with a given account.

        :param account_name: The name of the account holding the smart contract.
        :type account_name: str
        :param download_location: The directory where the contract will be downloaded.
        :type download_location: str | Path
        :param target_url: Optional URL to a specific node. Defaults to the node set in the client.
        :type local_name: str | None

        :raises: Custom exceptions based on download failure.

        The function downloads both the WebAssembly (`.wasm`) and ABI (`.abi`) files.
        '''

        if isinstance(download_location, str):
            download_location = Path(download_location).resolve()

        if not local_name:
            local_name = account_name

        _, wasm = self.get_code(account_name)

        if not abi:
            abi = self.get_abi(account_name)

        with open(download_location / f'{local_name}.wasm', 'wb+') as wasm_file:
            wasm_file.write(wasm)

        with open(download_location / f'{local_name}.abi', 'w+') as abi_file:
            abi_file.write(json_module.dumps(abi))


    def boot_sequence(
        self,
        contracts: str | Path = 'tests/contracts',
        token_sym: str = DEFAULT_SYS_TOKEN_SYM,
        ram_amount: int = 16_000_000_000,
        remote_node: 'CLEOS' = None,
        extras: list[str] = []
    ):
        '''Boots a blockchain with required system contracts and settings.

        :param contracts: Path to directory containing compiled contract artifacts. Defaults to 'tests/contracts'.
        :type contracts: str | Path
        :param token_sym: System token symbol. Defaults to :const:`DEFAULT_SYS_TOKEN_SYM`.
        :type token_sym: str
        :param ram_amount: Initial RAM allocation for system. Defaults to 16,000,000,000.
        :type ram_amount: int
        :param verify_hash: Whether to verify contract hash after deployment. Defaults to False.
        :type verify_hash: bool

        :raises Exception: Missing ABI or WASM files.

        :return: None
        '''

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
            self.create_account('eosio', name)

        # load contracts wasm and abi from specified dir
        contract_paths: dict[str, Path] = {}
        for contract_dir in Path(contracts).iterdir():
            contract_paths[contract_dir.name] = contract_dir.resolve()


        self.deploy_contract_from_path(
            'eosio.token', contract_paths['eosio.token'],
            staked=False
        )

        self.deploy_contract_from_path(
            'eosio.msig', contract_paths['eosio.msig'],
            staked=False
        )

        self.deploy_contract_from_path(
            'eosio.wrap', contract_paths['eosio.wrap'],
            staked=False
        )

        self.init_sys_token(token_sym=token_sym)

        self.activate_feature_v1('PREACTIVATE_FEATURE')

        self.sys_deploy_info = self.deploy_contract_from_path(
            'eosio', contract_paths['eosio.bios'],
            create_account=False
        )

        if remote_node is not None:
            self.clone_activations(remote_node)

        self.sys_deploy_info = self.deploy_contract_from_path(
            'eosio', contract_paths['eosio.system'],
            create_account=False
        )

        self.push_action(
            'eosio',
            'setpriv',
            ['eosio.msig', 1],
            'eosio'
        )

        self.push_action(
            'eosio',
            'setpriv',
            ['eosio.wrap', 1],
            'eosio'
        )

        self.push_action(
            'eosio',
            'init',
            [0, token_sym],
            'eosio'
        )

        self.push_action(
            'eosio',
            'setram',
            [ram_amount],
            'eosio'
        )

        if 'telos' in extras:
            self.create_account_staked(
                'eosio', 'telos.decide', ram=4475000)

            self.deploy_contract_from_path(
                'telos.decide', contract_paths['telos.decide'],
                create_account=False
            )

            self.create_account_staked('eosio', 'exrsrv.tf')

    # Producer API

    def is_block_production_paused(self, *args, **kwargs):
        '''Checks if block production is currently paused.

        :return: Response from the `/v1/producer/paused` endpoint.
        :rtype: dict
        '''
        return self._post('/v1/producer/paused')

    def resume_block_production(self):
        '''Resumes block production.

        :return: Response from the `/v1/producer/resume` endpoint.
        :rtype: dict
        '''
        return self._post('/v1/producer/resume')

    def pause_block_production(self):
        '''Pauses block production.

        :return: Response from the `/v1/producer/pause` endpoint.
        :rtype: dict
        '''
        return self._post('/v1/producer/pause')

    # Net API

    def connected_nodes(self):
        '''Retrieves the connected nodes.

        :return: Response from the `/v1/net/connections` endpoint.
        :rtype: dict
        '''
        return self._post('/v1/net/connections')

    def connect_node(self, endpoint: str):
        '''Connects to a specified node.

        :param endpoint: Node endpoint to connect to.
        :type endpoint: str
        :return: Response from the `/v1/net/connect` endpoint.
        :rtype: dict
        '''
        return self._post(
            '/v1/net/connect',
            json=endpoint)

    def disconnect_node(self, endpoint: str):
        '''Disconnects from a specified node.

        :param endpoint: Node endpoint to disconnect from.
        :type endpoint: str
        :return: Response from the `/v1/net/disconnect` endpoint.
        :rtype: dict
        '''
        return self._post(
            '/v1/net/disconnect',
            json=endpoint)

    def create_key_pair(self) -> tuple[str, str]:
        '''Generates a key pair.

        :return: Private and public keys.
        :rtype: tuple[str, str]
        '''
        priv, pub = gen_key_pair()
        return priv, pub

    def create_key_pairs(self, n: int) -> list[tuple[str, str]]:
        '''Generates multiple key pairs.

        :param n: Number of key pairs to generate.
        :type n: int
        :return: list of generated private and public keys.
        :rtype: list[tuple[str, str]]
        '''
        keys = []
        for _ in range(n):
            keys.append(self.create_key_pair())

        self.logger.info(f'created {n} key pairs')
        return keys

    def import_key(self, account: str, private_key: str):
        '''Imports a key pair for a given account.

        :param account: Account name.
        :type account: str
        :param private_key: Private key to import.
        :type private_key: str
        '''
        public_key = get_pub_key(private_key)
        self.keys[account] = public_key
        self.private_keys[account] = private_key
        if public_key not in self._key_to_acc:
            self._key_to_acc[public_key] = []

        self._key_to_acc[public_key] += [account]

    def get_private_key(self, account: str) -> str:
        key = self.private_keys.get(account, None)
        if not isinstance(key, str):
            raise ValueError(f'no key imported for account {account}')

        return key

    def assign_key(self, account: str, public_key: str):
        '''Assigns an existing public key to a new account.

        :param account: New account name.
        :type account: str
        :param public_key: Public key to assign.
        :type public_key: str
        '''
        if public_key not in self._key_to_acc:
            raise ValueError(f'{public_key} not found on other accounts')

        owner = self._key_to_acc[public_key][0]
        self.keys[account] = self.keys[owner]
        self.private_keys[account] = self.private_keys[owner]

    def get_feature_digest(self, feature_name: str) -> str:
        '''Retrieves the feature digest for a given feature name.

        :param feature_name: Name of the feature.
        :type feature_name: str
        :return: Feature digest.
        :rtype: str
        '''
        resp = self._post(
            '/v1/producer/get_supported_protocol_features',
            json={}
        )
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
        '''Activates a feature using v1 protocol.

        :param feature_name: Name of the feature to activate.
        :type feature_name: str
        '''
        digest = self.get_feature_digest(feature_name)
        r = self._post(
            '/v1/producer/schedule_protocol_feature_activations',
            json={
                'protocol_features_to_activate': [digest]
            }
        )

        assert 'result' in r
        assert r['result'] == 'ok'

        self.logger.info(f'{feature_name} -> {digest} active.')

    def activate_feature_with_digest(self, digest: str):
        '''Activates a feature using its digest.

        :param digest: Feature digest.
        :type digest: str
        '''
        self.push_action(
            'eosio',
            'activate',
            [digest],
            'eosio'
        )
        self.logger.info(f'{digest} active.')

    def activate_feature(self, feature_name: str):
        '''Wrapper for activating a feature using its name.

        :param feature_name: Name of the feature to activate.
        :type feature_name: str
        '''
        digest = self.get_feature_digest(feature_name)
        self.activate_feature_with_digest(digest)

    def get_ram_price(self) -> Asset:
        '''Fetches the current RAM price in the blockchain.

        This function queries the `eosio.rammarket` table and calculates the RAM price based on the quote and base balances.

        :return: Current RAM price as an :class:`leap.protocol.Asset` object.
        :rtype: :class:`leap.protocol.Asset`
        '''
        row = self.get_table(
            'eosio', 'eosio', 'rammarket')[0]

        quote = Asset.from_str(row['quote']['balance']).amount
        base = Asset.from_str(row['base']['balance']).amount

        return Asset(
            int((quote / base) * 1024 / 0.995) * (
                10 ** self.sys_token_supply.symbol.precision),
            self.sys_token_supply.symbol)

    def create_account(
        self,
        owner: str,
        name: str,
        key: str | None = None,
    ):
        '''Creates a new blockchain account.

        :param owner: The account that will own the new account.
        :type owner: str
        :param name: The new account name.
        :type name: str
        :param key: Public key to be assigned to new account. Defaults to a newly created key.
        :type key: str | None

        :return: Exit code and response dictionary.
        :rtype: tuple[int, dict]
        '''
        if not key:
            priv, pub = self.create_key_pair()
            self.import_key(name, priv)

        else:
            pub = key
            self.assign_key(name, pub)

        return self.push_action(
            'eosio',
            'newaccount',
            [owner, name,
             {'threshold': 1, 'keys': [{'key': pub, 'weight': 1}], 'accounts': [], 'waits': []},
             {'threshold': 1, 'keys': [{'key': pub, 'weight': 1}], 'accounts': [], 'waits': []}],
            owner, self.private_keys[owner]
        )

    def create_account_staked(
        self,
        owner: str,
        name: str,
        net: str = f'10.0000 {DEFAULT_SYS_TOKEN_CODE}',
        cpu: str = f'10.0000 {DEFAULT_SYS_TOKEN_CODE}',
        ram: int = 10_000_000,
        key: str | None = None
    ) -> tuple[int, dict]:
        '''Creates a new staked blockchain account.

        :param owner: The account that will own the new account.
        :type owner: str
        :param name: The new account name.
        :type name: str
        :param net: Amount of NET to stake. Defaults to \"10.0000 TLOS\".
        :type net: str 
        :param cpu: Amount of CPU to stake. Defaults to \"10.0000 TLOS\".
        :type cpu: str 
        :param ram: Amount of RAM to buy in bytes. Defaults to 10,000,000.
        :type ram: int
        :param key: Public key to be assigned to new account. Defaults to a newly created key.
        :type key: str | None

        :return: Exit code and response dictionary.
        :rtype: tuple[int, dict]
        '''
        if not key:
            priv, pub = self.create_key_pair()
            self.import_key(name, priv)
        else:
            pub = key
            self.assign_key(name, pub)

        actions = [{
            'account': 'eosio',
            'name': 'newaccount',
            'data': [
                owner, name,
                {'threshold': 1, 'keys': [{'key': pub, 'weight': 1}], 'accounts': [], 'waits': []},
                {'threshold': 1, 'keys': [{'key': pub, 'weight': 1}], 'accounts': [], 'waits': []}
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'buyrambytes',
            'data': [
                owner, name, ram
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'delegatebw',
            'data': [
                owner, name,
                net, cpu, True
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }]

        return self.push_actions(
            actions, self.private_keys[owner])

    def get_table(
        self,
        account: str,
        scope: str,
        table: str,
        **kwargs
    ) -> list[dict]:
        """Get table rows from the blockchain.

        :param account: Account name of contract were table is located.
        :param scope: Table scope in LEAP name format.
        :param table: Table name.
        :param args: Additional arguments to pass to ``cleos get table`` (`cleos 
            docs <https://developers.eos.io/manuals/eos/latest/cleos/command-ref
            erence/get/table>`_).

        :return: list of rows matching query.
        :rtype: list[dict]
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
            resp = self._post(
                '/v1/chain/get_table_rows', json=params)

            self.logger.debug(f'get_table {account} {scope} {table}: {resp}')
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
    ) -> list[dict]:

        done = False
        rows = []
        params = {
            'code': str(account),
            'scope': str(scope),
            'table': str(table),
            'json': True,
            **kwargs
        }

        while not done:
            resp = await (self._post(
                '/v1/chain/get_table_rows', is_async=True, json=params))

            self.logger.debug(f'get_table {account} {scope} {table}: {resp}')
            rows.extend(resp['rows'])
            done = not resp['more']
            if not done:
                params['index_position'] = resp['next_key']

        return rows

    def get_info(self) -> dict[str, str | int]:
        '''Get blockchain statistics.

            - ``server_version``
            - ``head_block_num``
            - ``last_irreversible_block_num``
            - ``head_block_id``
            - ``head_block_time``
            - ``head_block_producer``
            - ``recent_slots``
            - ``participation_rate``

        :return: A dictionary with blockchain information.
        :rtype: dict[str, str | int]
        '''
        return self._get('/v1/chain/get_info')

    async def a_get_info(self) -> dict[str, str | int]:
        '''Get blockchain statistics.

            - ``server_version``
            - ``head_block_num``
            - ``last_irreversible_block_num``
            - ``head_block_id``
            - ``head_block_time``
            - ``head_block_producer``
            - ``recent_slots``
            - ``participation_rate``

        :return: A dictionary with blockchain information.
        :rtype: dict[str, str | int]
        '''
        return await self._aget('/v1/chain/get_info')

    def get_resources(self, account: str) -> list[dict]:
        '''Get account resources.

        :param account: Name of account to query resources.

        :return: A list with a single dictionary which contains, resource info.
        :rtype: list[dict]
        '''

        return self.get_table('eosio', account, 'userres')

    def new_account(
        self,
        name: str | None = None,
        owner: str = 'eosio',
        **kwargs
    ) -> str:
        '''Create a new account with a random key and name, import the private
        key into the wallet.

        :param name: To set a specific name and not a random one.

        :return: New account name.
        :rtype: str
        '''

        if name:
            account_name = name
        else:
            account_name = random_leap_name()

        self.create_account_staked(owner, account_name, **kwargs)
        return account_name

    def buy_ram_bytes(
        self,
        payer: str,
        amount: int,
        receiver: str | None = None
    ):
        '''Buys a specific amount of RAM in bytes.

        :param payer: Account responsible for the payment.
        :type payer: str
        :param amount: Amount of RAM to purchase in bytes.
        :type amount: int
        :param receiver: Account that receives the RAM. Defaults to `payer`.
        :type receiver: str | None

        :return: Action result as an tuple[int, dict] object.
        :rtype: tuple[int, dict]
        '''

        if not receiver:
            receiver = payer

        return self.push_action(
            'eosio',
            'buyrambytes',
            [payer, receiver, amount],
            payer
        )

    @property
    def head_block_num(self) -> int:
        return self.get_info()['head_block_num']

    def wait_block(self, block_num: int, progress: bool = False, interval: float = .5):
        '''Wait for specific block to be reached on node.
        '''
        current_block = self.head_block_num

        last_report = -1000
        def report():
            self.logger.info(f'waiting for block {block_num}, current: {current_block}, remaining: {block_num - current_block}')
            last_report = current_block

        while current_block < block_num:
            if current_block - last_report > 1000:
                report()

            time.sleep(interval)
            current_block = self.head_block_num

    def wait_blocks(self, n: int, **kwargs):
        '''Waits for a specific number of blocks to be produced.

        :param n: Number of blocks to wait for.
        :type n: int

        :return: None
        '''
        target_block = int(self.head_block_num) + n
        self.wait_block(target_block, **kwargs)


    '''Token managment
    '''

    def get_token_stats(
        self,
        sym: str,
        token_contract: str = 'eosio.token'
    ) -> dict[str, str]:
        '''Get token statistics.

        :param sym: Token symbol.
        :param token_contract: Token contract.

        :return: A dictionary with ``\'supply\'``, ``\'max_supply\'`` and
            ``\'issuer\'`` as keys.
        :rtype: dict[str, str]
        '''

        return self.get_table(
            token_contract,
            sym,
            'stat'
        )[0]

    def get_balance(
        self,
        account: str,
        token_contract: str = 'eosio.token'
    ) -> str | None:
        '''Get account balance.

        :param account: Account to query.
        :param token_contract: Token contract.

        :return: Account balance in asset form, ``None`` if user has no balance
            entry.
        :rtype: str | None
        '''

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
        max_supply: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        '''Creates a new token contract.

        :param issuer: Account authorized to issue and manage the token.
        :type issuer: str
        :param max_supply: Maximum supply for the token.
        :type max_supply: str
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.
        '''

        return self.push_action(
            token_contract,
            'create',
            [issuer, max_supply],
            token_contract,
            self.private_keys[token_contract],
            **kwargs
        )

    def issue_token(
        self,
        issuer: str,
        quantity: str,
        memo: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        '''Issues tokens to the issuer account.

        :param issuer: Account authorized to issue the token.
        :type issuer: str
        :param quantity: Amount of tokens to issue.
        :type quantity: str
        :param memo: Memo for the issued tokens.
        :type memo: str
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.
        '''

        return self.push_action(
            token_contract,
            'issue',
            [issuer, quantity, memo],
            issuer,
            self.private_keys[issuer],
            **kwargs
        )

    def transfer_token(
        self,
        _from: str,
        _to: str,
        quantity: str,
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        '''Transfers tokens from one account to another.

        :param _from: Sender account.
        :type _from: str
        :param _to: Receiver account.
        :type _to: str
        :param quantity: Amount of tokens to transfer.
        :type quantity: str
        :param memo: Optional memo for the transaction.
        :type memo: str
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.
        '''
        return self.push_action(
            token_contract,
            'transfer',
            [_from, _to, quantity, memo],
            _from,
            self.private_keys[_from],
            **kwargs
        )

    def give_token(
        self,
        _to: str,
        quantity: str,
        memo: str = '',
        token_contract='eosio.token',
        **kwargs
    ):
        return self.transfer_token(
            'eosio',
            _to,
            quantity,
            memo,
            token_contract=token_contract,
            **kwargs
        )

    def retire_token(
        self,
        issuer: str,
        quantity: str,
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        return self.push_action(
            token_contract,
            'retire',
            [quantity, memo],
            issuer,
            self.private_keys[issuer],
            **kwargs
        )

    def open_token(
        self,
        owner: str,
        sym: str,
        ram_payer: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        return self.push_action(
            token_contract,
            'open',
            [owner, sym, ram_payer],
            ram_payer,
            self.private_keys[ram_payer],
            **kwargs
        )

    def close_token(
        self,
        owner: str,
        sym: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ):
        return self.push_action(
            token_contract,
            'close',
            [owner, sym],
            owner,
            self.private_keys[owner],
            **kwargs
        )


    def init_sys_token(
        self,
        token_sym: str = DEFAULT_SYS_TOKEN_SYM,
        token_amount: int = 420_000_000 * (10 ** 4)
    ):
        '''Initialize ``SYS`` token.

        Issue all of it to ``eosio`` account.
        '''
        if not self._sys_token_init:

            self.sys_token_supply = Asset(token_amount, token_sym)

            self.create_token('eosio', self.sys_token_supply)
            self.issue_token('eosio', self.sys_token_supply, __name__)

            self._sys_token_init = True

    def get_global_state(self):
        return self.get_table(
            'eosio', 'eosio', 'global')[0]

    def rex_deposit(
        self,
        owner: str,
        quantity: str
    ):
        return self.push_action(
            'eosio',
            'deposit',
            [owner, quantity],
            owner
        )

    def rex_buy(
        self,
        _from: str,
        quantity: str
    ):
        return self.push_action(
            'eosio',
            'buyrex',
            [_from, quantity],
            _from
        )

    def delegate_bandwidth(
        self,
        _from: str,
        _to: str,
        net: str,
        cpu: str,
        transfer: bool = True
    ):
        return self.push_action(
            'eosio',
            'delegatebw',
            [
                _from,
                _to,
                net,
                cpu,
                transfer
            ],
            _from
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
            [
                producer,
                self.keys[producer],
                url,
                location
            ],
            producer
        )

    def vote_producers(
        self,
        voter: str,
        proxy: str,
        producers: list[str]
    ):
        return self.push_action(
            'eosio',
            'voteproducer',
            [voter, proxy, producers],
            voter
        )

    def claim_rewards(
        self,
        owner: str
    ):
        return self.push_action(
            'eosio',
            'claimrewards',
            [owner],
            owner
        )

    def get_schedule(self):
        '''Fetches the current producer schedule.

        :return: JSON object containing the current producer schedule.
        :rtype: dict
        '''
        return self._post(
            f'/v1/chain/get_producer_schedule')

    def get_producers(self):
        '''Fetches information on producers.

        :return: list of dictionaries containing producer information.
        :rtype: list[dict]
        '''
        return self.get_table(
            'eosio',
            'eosio',
            'producers'
        )

    def get_producer(self, producer: str) -> dict | None:
        '''Fetches information on a specific producer.

        :param producer: The name of the producer to query.
        :type producer: str

        :return: dictionary containing producer information, or None if not found.
        :rtype: dict | None
        '''

        rows = self.get_table(
            'eosio', 'eosio', 'producers',
            '--key-type', 'name', '--index', '1',
            '--lower', producer,
            '--upper', producer)

        if len(rows) == 0:
            return None
        else:
            return rows[0]

    def get_payrate(self) -> dict | None:
        '''Fetches the current payrate.

        :return: dictionary containing payrate information, or None if not found.
        :rtype: dict | None
        '''
        rows = self.get_table(
            'eosio', 'eosio', 'payrate')

        if len(rows) == 0:
            return None
        else:
            return rows[0]
