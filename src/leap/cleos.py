#!/usr/bin/env python3

import sys
import time
import json
import base64
import logging
import requests
import binascii

from copy import deepcopy
from typing import (
    Any,
    Dict,
    List,
    Union,
    Tuple,
    Optional
)
from urllib3.util.retry import Retry
from pathlib import Path
from hashlib import sha256

from datetime import datetime, timedelta

import asks

from requests.adapters import HTTPAdapter

from .sugar import random_leap_name
from .errors import ContractDeployError
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
        url: str = 'http://127.0.0.1:8888',
        remote: str = 'https://mainnet.telos.net',
        logger = None
    ):
        self.url = url

        if logger is None:
            self.logger = logging.getLogger('cleos')
        else:
            self.logger = logger

        self.endpoint = url
        self.remote_endpoint = remote

        self.keys: Dict[str, str] = {}
        self.private_keys: Dict[str, str] = {}
        self._key_to_acc: Dict[str, List[str]] = {}

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

    def _pack_action_data(self, action: dict) -> str:
        ds = DataStream()
        data = action['data']

        if isinstance(data, dict):
            value_iter = data.values()

        elif isinstance(data, list):
            value_iter = data

        else:
            raise ValueError(f'type {type(data)} not supported for data argument')

        for val in value_iter:
            if isinstance(val, str):
                ds.pack_string(val)

            elif isinstance(val, int):
                ds.pack_uint64(val)

            elif isinstance(val, Abi):
                abi_raw = DataStream()
                abi_raw.pack_abi(val.get_dict())
                ds.pack_bytes(abi_raw.getvalue())

            elif isinstance(val, bool):
                ds.pack_bool(val)

            elif isinstance(val, Name):
                ds.pack_name(str(val))

            elif isinstance(val, bytes):
                ds.pack_bytes(val)

            elif isinstance(val, Int8):
                ds.pack_int8(val.num)

            elif isinstance(val, Int16):
                ds.pack_int16(val.num)

            elif isinstance(val, Int32):
                ds.pack_int32(val.num)

            elif isinstance(val, Int64):
                ds.pack_int64(val.num)

            elif isinstance(val, UInt8):
                ds.pack_uint8(val.num)

            elif isinstance(val, UInt16):
                ds.pack_uint16(val.num)

            elif isinstance(val, UInt32):
                ds.pack_uint32(val.num)

            elif isinstance(val, UInt64):
                ds.pack_uint64(val.num)

            elif isinstance(val, Asset):
                ds.pack_asset(str(val))

            elif isinstance(val, Symbol):
                ds.pack_symbol(str(val))

            elif isinstance(val, LeapOptional):
                opt_val = val
                ds.pack_optional(opt_val.type, opt_val.value)

            elif isinstance(val, PublicKey):
                ds.pack_public_key(val.get())

            elif isinstance(val, VarInt32):
                ds.pack_varuint32(val.num)

            elif isinstance(val, VarUInt32):
                ds.pack_varuint32(val.num)

            elif isinstance(val, Authority):
                ds.pack_authority(val.get_dict())

            elif isinstance(val, Checksum160):
                ds.pack_rd160(val.hash)

            elif isinstance(val, Checksum256):
                ds.pack_checksum256(str(val))

            elif isinstance(val, ListArgument):
                ds.pack_array(val.type, val.list)

            else:
                raise ValueError(
                    f'datastream packing not implemented for {type(val)}')

        return binascii.hexlify(
            ds.getvalue()).decode('utf-8')

    async def _a_create_and_push_tx(
        self,
        actions: list[dict],
        key: str,
        max_cpu_usage_ms=100,
        max_net_usage_words=0
    ) -> dict:
        chain_info = await self.a_get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id = chain_info['chain_id']

        res = None
        retries = 3
        while retries > 0:
            tx = {
                'delay_sec': 0,
                'max_cpu_usage_ms': max_cpu_usage_ms,
                'actions': deepcopy(actions)
            }

            # package transation
            for i, action in enumerate(tx['actions']):
                tx['actions'][i]['data'] = self._pack_action_data(action)

            tx.update({
                'expiration': get_expiration(
                    datetime.utcnow(), timedelta(minutes=15).total_seconds()),
                'ref_block_num': ref_block_num,
                'ref_block_prefix': ref_block_prefix,
                'max_net_usage_words': max_net_usage_words,
                'max_cpu_usage_ms': max_cpu_usage_ms,
                'delay_sec': 0,
                'context_free_actions': [],
                'transaction_extensions': [],
                'context_free_data': []
            })

            # Sign transaction
            try:
                _, signed_tx = sign_tx(chain_id, tx, key)

            except CannonicalSignatureError:
                continue

            # Pack
            ds = DataStream()
            ds.pack_transaction(signed_tx)
            packed_trx = binascii.hexlify(ds.getvalue()).decode('utf-8')
            final_tx = build_push_transaction_body(signed_tx['signatures'][0], packed_trx)

            # Push transaction
            self.logger.debug(f'pushing tx to: {self.endpoint}')
            res = (await self._apost(f'{self.endpoint}/v1/chain/push_transaction', json=final_tx)).json()
            res_json = json.dumps(res, indent=4)

            self.logger.debug(res_json)

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
        permission: str = 'active',
        **kwargs
    ):
        '''Async push action

        :param account: smart contract account name
        :type account: str
        :param action: smart contract action name
        :type action: str
        :param data: action data
        :type data: dict
        :param key: private key used to sign
        :type key: str
        :param permission: permission name
        :type permission: str
        '''
        return await self._a_create_and_push_tx([{
            'account': str(account),
            'name': str(action),
            'data': data,
            'authorization': [{
                'actor': str(actor),
                'permission': str(permission)
            }]
        }], key, **kwargs)

    async def a_push_actions(
        self,
        actions: list[dict],
        key: str,
        **kwargs
    ):
        '''Async push actions, uses a single tx for all actions.

        :param actions: list of actions
        :type actions: str
        :param key: private key used to sign
        :type key: str
        '''
        return await self._a_create_and_push_tx(actions, key, **kwargs)

    def add_permission(
        self,
        account: Union[str, Name],
        permission: Union[str, Name],
        parent: Union[str, Name],
        auth: Authority
    ):
        '''Add permission to an account

        :param account: account name
        :type account: str
        :param permission: permission name
        :type permission: str
        :param parent: parent account name
        :type parent: str
        :param auth: authority schema
        :type auth: :class:`leap.sugar.Authority`
        '''
        return self.push_action(
            'eosio',
            'updateauth',
            [
                Name.from_str(account),
                Name.from_str(permission),
                Name.from_str(parent),
                auth
            ],
            account,
        )

    def deploy_contract(
        self,
        account_name: str,
        wasm: bytes,
        abi: Abi,
        privileged: bool = False,
        create_account: bool = True,
        staked: bool = True,
        verify_hash: bool = True
    ):
        '''Deploy a built contract.

        :param account_name: Name of account to deploy contract at.
        :type account_name: str
        :param wasm: Raw wasm as bytearray
        :type wasm: bytes
        :param abi: Abi already wrapped on our custom Abi type
        :type abi: :class:`leap.sugar.Abi`
        :param privileged: ``True`` if contract should be privileged (system
            contracts).
        :type privileged: bool
        :param create_account: ``True`` if target account should be created.
        :type create_account: bool
        :param staked: ``True`` if this account should use RAM & NET resources.
        :type staked: bool
        :param verify_hash: Query remote node for ``contract_name`` and compare
            hashes.
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
                [Name.from_str(account_name), UInt8(1)],
                'eosio'
            )

        self.wait_blocks(1)

        ec, _ = self.add_permission(
            account_name,
            'active', 'owner',
            Authority(
                1,
                [KeyWeight(PublicKey(self.keys[account_name]), 1)],
                [PermissionLevelWeight(
                    PermissionLevel(account_name, 'eosio.code'), 1)],
                []
            )
        )
        assert ec == 0
        self.logger.info('gave eosio.code permissions')

        local_shasum = sha256(wasm).hexdigest()
        self.logger.info(f'contract hash: {local_shasum}')

        # verify contract hash using remote node
        if verify_hash:
            remote_shasum, _  = self.get_code(account_name, target_url=self.remote_endpoint)

            if local_shasum != remote_shasum:
                raise ContractDeployError(
                    f'Local contract hash doesn\'t match remote:\n'
                    f'local: {local_shasum}\n'
                    f'remote: {remote_shasum}')

        self.logger.info('deploy...')

        actions = [{
            'account': 'eosio',
            'name': 'setcode',
            'data': [
                Name.from_str(account_name),
                UInt8(0), UInt8(0),
                ListArgument(wasm, 'uint8')
            ],
            'authorization': [{
                'actor': account_name,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'setabi',
            'data': [
                Name.from_str(account_name),
                abi
            ],
            'authorization': [{
                'actor': account_name,
                'permission': 'active'
            }]
        }]

        ec, res = self.push_actions(
            actions, self.private_keys[account_name])

        if 'error' not in res:
            self.logger.info('deployed')
            return res

        else:
            self.logger.error(json.dumps(res, indent=4))
            raise ContractDeployError(f'Couldn\'t deploy {account_name} contract.')

    def deploy_contract_from_path(
        self,
        account_name: str,
        contract_path: Union[str, Path],
        contract_name: Optional[str] = None,
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
            abi = Abi(json.load(abi_file))

        return self.deploy_contract(
            account_name, wasm, abi, **kwargs)

    def get_code(
        self,
        account_name: Union[str, Name],
        target_url: Optional[str] = None
    ) -> Tuple[str, bytes]:
        '''Fetches and decodes the WebAssembly (WASM) code for a given account.

        :param account_name: Account to get the WASM code for, can be a string
            or a :class:`leap.sugar.Abi` object.
        :type account_name: Union[str, :class:`leap.sugar.Abi`]
        :param target_url: The URL to fetch the WASM code from. Defaults to `self.url`.
        :type target_url: Optional[str]
        :return: A tuple containing the hash and the decoded WASM code.
        :rtype: Tuple[str, bytes]
        :raises Exception: If the response contains an 'error' field.
        '''
        if not target_url:
            target_url = self.url

        resp_obj = self._post(
            f'{target_url}/v1/chain/get_raw_code_and_abi',
            json={
                'account_name': str(account_name)
            }
        )

        resp = resp_obj.json()

        if 'error' in resp:
            raise Exception(resp)

        wasm = base64.b64decode(resp['wasm'])
        wasm_hash = sha256(wasm).hexdigest()

        return wasm_hash, wasm

    def get_abi(self, account_name: Union[str, Name], target_url: Optional[str] = None) -> Abi:
        '''Fetches the ABI (Application Binary Interface) for a given account.

        :param account_name: Account to get the ABI for, can be a string or a :class:`leap.sugar.Name` object.
        :type account_name: Union[str, :class:`leap.sugar.Name`]
        :param target_url: The URL to fetch the ABI from. Defaults to `self.url`.
        :type target_url: Optional[str]
        :return: An instance of :class:`leap.sugar.Abi` containing the ABI data.
        :rtype: :class:`leap.sugar.Abi`
        :raises Exception: If the response contains an 'error' field.
        '''
        if not target_url:
            target_url = self.url

        resp = self._post(
            f'{target_url}/v1/chain/get_abi',
            json={
                'account_name': str(account_name)
            }
        ).json()

        if 'error' in resp:
            raise Exception(resp)

        return Abi(resp['abi'])

    def create_snapshot(self, target_url: str, body: dict):
        '''Initiates a snapshot of the AntelopeIO blockchain at the given URL.

        :param target_url: The URL where the snapshot will be created.
        :type target_url: str
        :param body: Parameters for snapshot creation in dictionary format.
        :type body: dict
        :return: The HTTP response object.
        :rtype: Response
        :note: This function only works if `producer_api_plugin` is enabled on the target node.
        '''

        resp = self._post(
            f'{target_url}/v1/producer/create_snapshot',
            json=body
        )
        return resp

    def schedule_snapshot(self, target_url: str, **kwargs):
        '''Schedules a snapshot of the AntelopeIO blockchain at the given URL.

        :param target_url: The URL where the snapshot will be scheduled.
        :type target_url: str
        :param kwargs: Additional keyword arguments for snapshot scheduling.
        :return: The HTTP response object.
        :rtype: Response
        :note: This function only works if `producer_api_plugin` is enabled on the target node.
        '''

        resp = self._post(
            f'{target_url}/v1/producer/schedule_snapshot',
            json=kwargs
        )
        return resp

    def get_node_activations(self, target_url: str) -> List[Dict]:
        '''Fetches a list of activated protocol features from the AntelopeIO blockchain at the given URL.

        :param target_url: The URL to fetch the activated protocol features from.
        :type target_url: str
        :return: A list of dictionaries, each representing an activated protocol feature.
        :rtype: List[Dict]
        '''

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
        '''Clones the activated protocol features from a target AntelopeIO node to the current node.

        :param target_url: The URL to fetch the activated protocol features from.
        :type target_url: str
        :raises Exception: If the activation fails.
        '''

        features = self.get_node_activations(target_url)

        feature_names = [
            feat['specification'][0]['value']
            for feat in features
        ]

        self.logger.info('activating features:')
        self.logger.info(
            json.dumps(feature_names, indent=4))

        actions = [{
            'account': 'eosio',
            'name': 'activate',
            'data': [Checksum256(f['feature_digest'])],
            'authorization': [{
                'actor': 'eosio',
                'permission': 'active'
            }]
        } for f in features]

        ec, res = self.push_actions(actions, self.private_keys['eosio'])
        if ec != 0:
            raise Exception(json.dumps(res, indent=4))

        self.logger.info('activated')

    def diff_protocol_activations(self, target_one: str, target_two: str):
        '''Compares the activated protocol features between two AntelopeIO nodes.

        :param target_one: The URL of the first node to compare.
        :type target_one: str
        :param target_two: The URL of the second node to compare.
        :type target_two: str
        :return: A list of feature names activated in `target_one` but not in `target_two`.
        :rtype: List[str]
        '''

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

    def download_contract(
        self,
        account_name: Union[str, Name],
        download_location: Union[str, Path],
        target_url: Optional[str] = None,
        local_name: Optional[str] = None
    ):
        '''Downloads the smart contract associated with a given account.

        :param account_name: The name of the account holding the smart contract.
        :type account_name: Union[str, :class:`leap.sugar.Name`]
        :param download_location: The directory where the contract will be downloaded.
        :type download_location: Union[str, Path]
        :param target_url: Optional URL to a specific node. Defaults to the node set in the client.
        :type target_url: Optional[str]
        :param local_name: Optional name for the downloaded contract files. Defaults to `account_name`.
        :type local_name: Optional[str]

        :raises: Custom exceptions based on download failure.

        The function downloads both the WebAssembly (`.wasm`) and ABI (`.abi`) files.
        '''

        if isinstance(download_location, str):
            download_location = Path(download_location).resolve()

        if not target_url:
            target_url = self.url

        if not local_name:
            local_name = str(account_name)

        _, wasm = self.get_code(account_name, target_url=target_url)
        abi = self.get_abi(account_name, target_url=target_url)

        with open(download_location / f'{local_name}.wasm', 'wb+') as wasm_file:
            wasm_file.write(wasm)

        with open(download_location / f'{local_name}.abi', 'w+') as abi_file:
            abi_file.write(json.dumps(abi.get_dict()))


    def boot_sequence(
        self,
        contracts: Union[str, Path] = 'tests/contracts',
        token_sym: Symbol = DEFAULT_SYS_TOKEN_SYM,
        ram_amount: int = 16_000_000_000,
        activations_node: Optional[str] = None,
        verify_hash: bool = False,
        extras: List[str] = []
    ):
        '''Boots a blockchain with required system contracts and settings.

        :param contracts: Path to directory containing compiled contract artifacts. Defaults to 'tests/contracts'.
        :type contracts: Union[str, Path]
        :param token_sym: System token symbol. Defaults to :const:`DEFAULT_SYS_TOKEN_SYM`.
        :type token_sym: :class:`leap.sugar.Symbol`
        :param ram_amount: Initial RAM allocation for system. Defaults to 16,000,000,000.
        :type ram_amount: int
        :param activations_node: Endpoint to clone protocol features from. Defaults to None, using `self.remote_endpoint`.
        :type activations_node: Optional[str]
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
            ec, _ = self.create_account('eosio', name)
            assert ec == 0

        # load contracts wasm and abi from specified dir
        contract_paths: Dict[str, Path] = {}
        for contract_dir in Path(contracts).iterdir():
            contract_paths[contract_dir.name] = contract_dir.resolve()


        self.deploy_contract_from_path(
            'eosio.token', contract_paths['eosio.token'],
            staked=False,
            verify_hash=verify_hash
        )

        self.deploy_contract_from_path(
            'eosio.msig', contract_paths['eosio.msig'],
            staked=False,
            verify_hash=verify_hash
        )

        self.deploy_contract_from_path(
            'eosio.wrap', contract_paths['eosio.wrap'],
            staked=False,
            verify_hash=verify_hash
        )

        self.init_sys_token(token_sym=token_sym)

        self.activate_feature_v1('PREACTIVATE_FEATURE')

        self.sys_deploy_info = self.deploy_contract_from_path(
            'eosio', contract_paths['eosio.bios'],
            create_account=False,
            verify_hash=verify_hash
        )

        if not activations_node:
            activations_node = self.remote_endpoint

        self.clone_node_activations(activations_node)

        self.sys_deploy_info = self.deploy_contract_from_path(
            'eosio', contract_paths['eosio.system'],
            create_account=False,
            verify_hash=verify_hash
        )

        ec, _ = self.push_action(
            'eosio',
            'setpriv',
            [Name.from_str('eosio.msig'), UInt8(1)],
            'eosio'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio',
            'setpriv',
            [Name.from_str('eosio.wrap'), UInt8(1)],
            'eosio'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio',
            'init',
            [VarUInt32(0), token_sym],
            'eosio'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio',
            'setram',
            [UInt64(ram_amount)],
            'eosio'
        )
        assert ec == 0

        if 'telos' in extras:
            self.create_account_staked(
                'eosio', 'telos.decide', ram=4475000)

            self.deploy_contract_from_path(
                'telos.decide', contract_paths['telos.decide'],
                create_account=False,
                verify_hash=verify_hash
            )

            self.create_account_staked('eosio', 'exrsrv.tf')

    # Producer API

    def is_block_production_paused(self):
        '''Checks if block production is currently paused.

        :return: Response from the `/v1/producer/paused` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/producer/paused').json()

    def resume_block_production(self):
        '''Resumes block production.

        :return: Response from the `/v1/producer/resume` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/producer/resume').json()

    def pause_block_production(self):
        '''Pauses block production.

        :return: Response from the `/v1/producer/pause` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/producer/pause').json()

    # Net API

    def connected_nodes(self):
        '''Retrieves the connected nodes.

        :return: Response from the `/v1/net/connections` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/net/connections').json()

    def connect_node(self, endpoint: str):
        '''Connects to a specified node.

        :param endpoint: Node endpoint to connect to.
        :type endpoint: str
        :return: Response from the `/v1/net/connect` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/net/connect',
            json=endpoint).json()

    def disconnect_node(self, endpoint: str):
        '''Disconnects from a specified node.

        :param endpoint: Node endpoint to disconnect from.
        :type endpoint: str
        :return: Response from the `/v1/net/disconnect` endpoint.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/net/disconnect',
            json=endpoint).json()

    def create_key_pair(self) -> Tuple[str, str]:
        '''Generates a key pair.

        :return: Private and public keys.
        :rtype: Tuple[str, str]
        '''
        priv, pub = gen_key_pair()
        return priv, pub

    def create_key_pairs(self, n: int) -> List[Tuple[str, str]]:
        '''Generates multiple key pairs.

        :param n: Number of key pairs to generate.
        :type n: int
        :return: List of generated private and public keys.
        :rtype: List[Tuple[str, str]]
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
        '''Activates a feature using v1 protocol.

        :param feature_name: Name of the feature to activate.
        :type feature_name: str
        '''
        digest = self.get_feature_digest(feature_name)
        r = self._post(
            f'{self.endpoint}/v1/producer/schedule_protocol_feature_activations',
            json={
                'protocol_features_to_activate': [digest]
            }
        ).json()

        assert 'result' in r
        assert r['result'] == 'ok'

        self.logger.info(f'{feature_name} -> {digest} active.')

    def activate_feature_with_digest(self, digest: Union[str, Checksum256]):
        '''Activates a feature using its digest.

        :param digest: Feature digest.
        :type digest: Union[str, :class:`leap.sugar.Checksum256`]
        '''
        if isinstance(digest, str):
            digest = Checksum256(digest)

        ec, _ = self.push_action(
            'eosio',
            'activate',
            [digest],
            'eosio'
        )
        assert ec == 0
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

        :return: Current RAM price as an :class:`leap.sugar.Asset` object.
        :rtype: :class:`leap.sugar.Asset`
        '''
        row = self.get_table(
            'eosio', 'eosio', 'rammarket')[0]

        quote = Asset.from_str(row['quote']['balance']).amount
        base = Asset.from_str(row['base']['balance']).amount

        return Asset(
            int((quote / base) * 1024 / 0.995) * (
                10 ** self.sys_token_supply.symbol.precision),
            self.sys_token_supply.symbol)

    def _create_and_push_tx(
        self,
        actions: list[dict],
        key: str,
        max_cpu_usage_ms=255,
        max_net_usage_words=0
    ) -> dict:
        chain_info = self.get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id = chain_info['chain_id']

        res = None
        retries = 2
        while retries > 0:
            tx = {
                'delay_sec': 0,
                'max_cpu_usage_ms': max_cpu_usage_ms,
                'actions': deepcopy(actions)
            }

            # package transation
            for i, action in enumerate(tx['actions']):
                tx['actions'][i]['data'] = self._pack_action_data(action)

            tx.update({
                'expiration': get_expiration(
                    datetime.utcnow(), timedelta(minutes=15).total_seconds()),
                'ref_block_num': ref_block_num,
                'ref_block_prefix': ref_block_prefix,
                'max_net_usage_words': max_net_usage_words,
                'max_cpu_usage_ms': max_cpu_usage_ms,
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
            logging.debug(f'pushing tx to: {self.endpoint}')
            res = self._post(f'{self.endpoint}/v1/chain/push_transaction', json=final_tx).json()
            res_json = json.dumps(res, indent=4)

            logging.debug(res_json)

            retries -= 1

            if 'error' in res:
                continue

            else:
                break

        if not res:
            ValueError('res is None')

        return res


    def push_action(
        self,
        account: Union[str, Name],
        action: Union[str, Name],
        data: Union[Dict, List[Any]],
        actor: Union[str, Name],
        key: Optional[str] = None,
        permission: str = 'active',
        **kwargs
    ) -> Tuple[int, dict]:
        '''Pushes a single action to the blockchain.

        :param account: The account to which the action belongs.
        :type account: str
        :param action: The action name.
        :type action: str
        :param data: The action data.
        :type data: Union[Dict, List[Any]]
        :param actor: The authorizing account.
        :type actor: str
        :param key: The private key for signing. Defaults to actor's private key.
        :type key: Optional[str]
        :param permission: Permission level for the action. Defaults to 'active'.
        :type permission: str

        :return: Exit code and response dictionary.
        :rtype: Tuple[int, dict]
        '''

        if not key:
            key = self.private_keys[actor]

        res = self._create_and_push_tx([{
            'account': str(account),
            'name': str(action),
            'data': data,
            'authorization': [{
                'actor': str(actor),
                'permission': str(permission)
            }]
        }], key, **kwargs)

        if 'error' in res:
            self.logger.error(json.dumps(res, indent=4))
            return 1, res
        else:
            return 0, res

    def push_actions(
        self,
        actions: list[dict],
        key: str,
        **kwargs
    ):
        '''Pushes multiple actions to the blockchain in a single transaction.

        :param actions: List of action dictionaries.
        :type actions: list[dict]
        :param key: The private key for signing.
        :type key: str

        :return: Exit code and response dictionary.
        :rtype: Tuple[int, dict]
        '''

        res = self._create_and_push_tx(actions, key, **kwargs)

        if 'error' in res:
            self.logger.error(json.dumps(res, indent=4))
            return 1, res
        else:
            return 0, res

    def create_account(
        self,
        owner: str,
        name: str,
        key: Optional[str] = None,
    ):
        '''Creates a new blockchain account.

        :param owner: The account that will own the new account.
        :type owner: str
        :param name: The new account name.
        :type name: str
        :param key: Public key to be assigned to new account. Defaults to a newly created key.
        :type key: Optional[str]

        :return: Exit code and response dictionary.
        :rtype: Tuple[int, dict]
        '''
        if not key:
            priv, pub = self.create_key_pair()
            self.import_key(name, priv)

        else:
            pub = key
            self.assign_key(name, pub)

        ec, out = self.push_action(
            'eosio',
            'newaccount',
            [Name.from_str(owner), Name.from_str(name),
             Authority(1, [KeyWeight(PublicKey(pub), 1)], [], []),
             Authority(1, [KeyWeight(PublicKey(pub), 1)], [], [])],
            owner, self.private_keys[owner]
        )
        assert ec == 0
        return ec, out

    def create_account_staked(
        self,
        owner: str,
        name: str,
        net: Union[Asset, str] = f'10.0000 {DEFAULT_SYS_TOKEN_CODE}',
        cpu: Union[Asset, str] = f'10.0000 {DEFAULT_SYS_TOKEN_CODE}',
        ram: int = 10_000_000,
        key: Optional[str] = None
    ) -> Tuple[int, dict]:
        '''Creates a new staked blockchain account.

        :param owner: The account that will own the new account.
        :type owner: str
        :param name: The new account name.
        :type name: str
        :param net: Amount of NET to stake. Defaults to 10 * 10^4.
        :type net: Union[:class:`leap.sugar.Asset`, int]
        :param cpu: Amount of CPU to stake. Defaults to 10 * 10^4.
        :type cpu: Union[:class:`leap.sugar.Asset`, int]
        :param ram: Amount of RAM to buy in bytes. Defaults to 10,000,000.
        :type ram: int
        :param key: Public key to be assigned to new account. Defaults to a newly created key.
        :type key: Optional[str]

        :return: Exit code and response dictionary.
        :rtype: Tuple[int, dict]
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
                Name.from_str(owner), Name.from_str(name),
                Authority(1, [KeyWeight(PublicKey(pub), 1)], [], []),
                Authority(1, [KeyWeight(PublicKey(pub), 1)], [], [])
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'buyrambytes',
            'data': [
                Name.from_str(owner), Name.from_str(name),
                UInt32(ram)
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }, {
            'account': 'eosio',
            'name': 'delegatebw',
            'data': [
                Name.from_str(owner), Name.from_str(name),
                Asset.from_str(net), Asset.from_str(cpu), True
            ],
            'authorization': [{
                'actor': owner,
                'permission': 'active'
            }]
        }]

        ec, res = self.push_actions(
            actions, self.private_keys[owner])

        return ec, res

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

            self.logger.debug(f'aget_table {account} {scope} {table}: {resp}')
            rows.extend(resp['rows'])
            done = not resp['more']
            if not done:
                params['lower_bound'] = resp['next_key']

        return rows

    def get_info(self) -> Dict[str, Union[str, int]]:
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
        :rtype: Dict[str, Union[str, int]]
        '''
        resp = self._get(f'{self.url}/v1/chain/get_info')
        assert resp.status_code == 200
        return resp.json()

    async def a_get_info(self) -> Dict[str, Union[str, int]]:
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
        :rtype: Dict[str, Union[str, int]]
        '''
        resp = await self._aget(f'{self.url}/v1/chain/get_info')
        assert resp.status_code == 200
        return resp.json()

    def get_resources(self, account: str) -> List[Dict]:
        '''Get account resources.

        :param account: Name of account to query resources.

        :return: A list with a single dictionary which contains, resource info.
        :rtype: List[Dict]
        '''

        return self.get_table('eosio', account, 'userres')

    def new_account(
        self,
        name: Optional[str] = None,
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
        receiver: Optional[str] = None
    ) -> Tuple[int, dict]:
        '''Buys a specific amount of RAM in bytes.

        :param payer: Account responsible for the payment.
        :type payer: str
        :param amount: Amount of RAM to purchase in bytes.
        :type amount: int
        :param receiver: Account that receives the RAM. Defaults to `payer`.
        :type receiver: Optional[str]

        :return: Action result as an Tuple[int, dict] object.
        :rtype: Tuple[int, dict]
        '''

        if not receiver:
            receiver = payer

        return self.push_action(
            'eosio',
            'buyrambytes',
            [Name.from_str(payer), Name.from_str(receiver), UInt32(amount)],
            payer
        )

    def wait_blocks(self, n: int):
        '''Waits for a specific number of blocks to be produced.

        :param n: Number of blocks to wait for.
        :type n: int

        :return: None
        '''
        target_block = int(self.get_info()['head_block_num']) + n

        while True:
            current_block = int(self.get_info()['head_block_num'])
            if current_block >= target_block:
                break

            remaining_blocks = target_block - current_block
            wait_time = remaining_blocks * 0.5
            time.sleep(wait_time)


    '''Token managment
    '''

    def get_token_stats(
        self,
        sym: str,
        token_contract: str = 'eosio.token'
    ) -> Dict[str, str]:
        '''Get token statistics.

        :param sym: Token symbol.
        :param token_contract: Token contract.

        :return: A dictionary with ``\'supply\'``, ``\'max_supply\'`` and
            ``\'issuer\'`` as keys.
        :rtype: Dict[str, str]
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
    ) -> Optional[str]:
        '''Get account balance.

        :param account: Account to query.
        :param token_contract: Token contract.

        :return: Account balance in asset form, ``None`` if user has no balance
            entry.
        :rtype: Optional[str]
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
        issuer: Union[str, Name],
        max_supply: Union[str, Asset],
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        '''Creates a new token contract.

        :param issuer: Account authorized to issue and manage the token.
        :type issuer: Union[str, :class:`leap.sugar.Name`]
        :param max_supply: Maximum supply for the token.
        :type max_supply: Union[str, :class:`leap.sugar.Asset`]
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.

        :return: Tuple containing error code and response.
        :rtype: Tuple[int, dict]
        '''

        return self.push_action(
            token_contract,
            'create',
            [Name.from_str(issuer), Asset.from_str(max_supply)],
            token_contract,
            self.private_keys[token_contract],
            **kwargs
        )

    def issue_token(
        self,
        issuer: Union[str, Name],
        quantity: Union[str, Asset],
        memo: str,
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        '''Issues tokens to the issuer account.

        :param issuer: Account authorized to issue the token.
        :type issuer: Union[str, :class:`leap.sugar.Name`]
        :param quantity: Amount of tokens to issue.
        :type quantity: Union[str, :class:`leap.sugar.Asset`]
        :param memo: Memo for the issued tokens.
        :type memo: str
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.

        :return: Tuple containing error code and response.
        :rtype: Tuple[int, dict]
        '''

        return self.push_action(
            token_contract,
            'issue',
            [Name.from_str(issuer), Asset.from_str(quantity), memo],
            str(issuer),
            self.private_keys[str(issuer)],
            **kwargs
        )

    def transfer_token(
        self,
        _from: Union[str, Name],
        _to: Union[str, Name],
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        '''Transfers tokens from one account to another.

        :param _from: Sender account.
        :type _from: Union[str, :class:`leap.sugar.Name`]
        :param _to: Receiver account.
        :type _to: Union[str, :class:`leap.sugar.Name`]
        :param quantity: Amount of tokens to transfer.
        :type quantity: Union[str, :class:`leap.sugar.Asset`]
        :param memo: Optional memo for the transaction.
        :type memo: str
        :param token_contract: Name of the token contract, defaults to 'eosio.token'.
        :type token_contract: str
        :param kwargs: Additional keyword arguments.

        :return: Tuple containing error code and response.
        :rtype: Tuple[int, dict]
        '''

        return self.push_action(
            token_contract,
            'transfer',
            [Name.from_str(_from), Name.from_str(_to), Asset.from_str(quantity), memo],
            str(_from),
            self.private_keys[str(_from)],
            **kwargs
        )

    def give_token(
        self,
        _to: Union[str, Name],
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract='eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
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
        issuer: Union[str, Name],
        quantity: Union[str, Asset],
        memo: str = '',
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        return self.push_action(
            token_contract,
            'retire',
            [Asset.from_str(quantity), memo],
            str(issuer),
            self.private_keys[str(issuer)],
            **kwargs
        )

    def open_token(
        self,
        owner: Union[str, Name],
        sym: Union[str, Symbol],
        ram_payer: Union[str, Name],
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        return self.push_action(
            token_contract,
            'open',
            [Name.from_str(owner), Symbol.from_str(sym), Name.from_str(ram_payer)],
            str(ram_payer),
            self.private_keys[str(ram_payer)],
            **kwargs
        )

    def close_token(
        self,
        owner: Union[str, Name],
        sym: Union[str, Symbol],
        token_contract: str = 'eosio.token',
        **kwargs
    ) -> Tuple[int, dict]:
        return self.push_action(
            token_contract,
            'close',
            [Name.from_str(owner), Symbol.from_str(sym)],
            str(owner),
            self.private_keys[str(owner)],
            **kwargs
        )


    def init_sys_token(
        self,
        token_sym: Symbol = DEFAULT_SYS_TOKEN_SYM,
        token_amount: int = 420_000_000 * (10 ** 4)
    ):
        '''Initialize ``SYS`` token.

        Issue all of it to ``eosio`` account.
        '''
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

    def rex_deposit(
        self,
        owner: Union[str, Name],
        quantity: Union[str, Asset]
    ):
        return self.push_action(
            'eosio',
            'deposit',
            [Name.from_str(owner), Asset.from_str(quantity)],
            str(owner)
        )

    def rex_buy(
        self,
        _from: Union[str, Name],
        quantity: Union[str, Asset]
    ):
        return self.push_action(
            'eosio',
            'buyrex',
            [Name.from_str(_from), Asset.from_str(quantity)],
            str(_from)
        )

    def delegate_bandwidth(
        self,
        _from: Union[str, Name],
        _to: Union[str, Name],
        net: Union[str, Asset],
        cpu: Union[str, Asset],
        transfer: bool = True
    ):
        return self.push_action(
            'eosio',
            'delegatebw',
            [
                Name.from_str(_from),
                Name.from_str(_to),
                Asset.from_str(net),
                Asset.from_str(cpu),
                transfer
            ],
            str(_from)
        )

    def register_producer(
        self,
        producer: Union[str, Name],
        url: str = '',
        location: int = 0
    ):
        producer_str = str(producer)
        return self.push_action(
            'eosio',
            'regproducer',
            [
                Name.from_str(producer),
                PublicKey(self.keys[producer_str]),
                url,
                UInt16(location)
            ],
            producer_str
        )

    def vote_producers(
        self,
        voter: Union[str, Name],
        proxy: Union[str, Name],
        producers: List[Union[str, Name]]
    ):
        prods = [
            str(p)
            if isinstance(p, Name) else p
            for p in producers
        ]

        return self.push_action(
            'eosio',
            'voteproducer',
            [Name.from_str(voter), Name.from_str(proxy), ListArgument(prods, 'name')],
            str(voter)
        )

    def claim_rewards(
        self,
        owner: Union[str, Name]
    ):
        return self.push_action(
            'eosio',
            'claimrewards',
            [Name.from_str(owner)],
            str(owner)
        )

    def get_schedule(self):
        '''Fetches the current producer schedule.

        :return: JSON object containing the current producer schedule.
        :rtype: dict
        '''
        return self._post(
            f'{self.url}/v1/chain/get_producer_schedule').json()

    def get_producers(self):
        '''Fetches information on producers.

        :return: List of dictionaries containing producer information.
        :rtype: List[Dict]
        '''
        return self.get_table(
            'eosio',
            'eosio',
            'producers'
        )

    def get_producer(self, producer: str) -> Optional[Dict]:
        '''Fetches information on a specific producer.

        :param producer: The name of the producer to query.
        :type producer: str

        :return: Dictionary containing producer information, or None if not found.
        :rtype: Optional[Dict]
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

    def get_payrate(self) -> Optional[Dict]:
        '''Fetches the current payrate.

        :return: Dictionary containing payrate information, or None if not found.
        :rtype: Optional[Dict]
        '''
        rows = self.get_table(
            'eosio', 'eosio', 'payrate')

        if len(rows) == 0:
            return None
        else:
            return rows[0]
