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

from .sugar import *
from .errors import ContractDeployError
from .tokens import DEFAULT_SYS_TOKEN_SYM
from .typing import (
    ExecutionResult,
    ActionResult
)
from .protocol import (
    gen_key_pair,
    get_pub_key,
    sign_tx,
    DataStream,
    CannonicalSignatureError,
    get_expiration, get_tapos_info, build_push_transaction_body
)

logging.getLogger("urllib3").setLevel(logging.ERROR)


class CLEOS:

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

    async def _a_create_and_push_tx(self, actions: list[dict], key: str) -> dict:
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
                tx['actions'][i]['data'] = self._pack_action_data(action)

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
        permission: str = 'active'
    ):
        return await self._a_create_and_push_tx([{
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
        return await self._a_create_and_push_tx(actions, key)

    def add_permission(
        self,
        account: str,
        permission: str,
        parent: str,
        auth: Authority
    ):
        return self.push_action(
            'eosio',
            'updateauth',
            [Name(account), Name(permission), Name(parent), auth],
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
        """Deploy a built contract inside the container.

        :param account_name: Name of account to deploy contract at.
        :param wasm: Raw wasm as bytearray
        :param abi: Abi already wrapped on our custom Abi type
        :param privileged: ``True`` if contract should be privileged (system
            contracts).
        :param account_name: Name of the target account of the deployment.
        :param create_account: ``True`` if target account should be created.
        :param staked: ``True`` if this account should use RAM & NET resources.
        :param verify_hash: Query remote node for ``contract_name`` and compare
            hashes.
        """
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
                [Name(account_name), UInt8(1)],
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
                Name(account_name),
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
                Name(account_name),
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

    def get_code(
        self,
        account_name: Union[str, Name],
        target_url: Optional[str] = None
    ) -> Tuple[str, bytes]:
        if isinstance(account_name, Name):
            account_name = str(account_name)

        if not target_url:
            target_url = self.url

        resp_obj = self._post(
            f'{target_url}/v1/chain/get_raw_code_and_abi',
            json={
                'account_name': account_name
            }
        )

        resp = resp_obj.json()

        if 'error' in resp:
            raise Exception(resp)

        wasm = base64.b64decode(resp['wasm'])
        wasm_hash = sha256(wasm).hexdigest()

        return wasm_hash, wasm

    def get_abi(self, account_name: Union[str, Name], target_url: Optional[str] = None) -> Abi:
        if isinstance(account_name, Name):
            account_name = str(account_name)

        if not target_url:
            target_url = self.url

        resp = self._post(
            f'{target_url}/v1/chain/get_abi',
            json={
                'account_name': account_name
            }
        ).json()

        if 'error' in resp:
            raise Exception(resp)

        return Abi(resp['abi'])

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

        # load contracts wasm and abi from specified dir
        contract_artifacts: Dict[str, Tuple[bytes, Abi]] = {}
        for contract_dir in Path(contracts).iterdir():
            abi_path = contract_dir / f'{contract_dir.name}.abi'
            wasm_path = contract_dir / f'{contract_dir.name}.wasm'

            if not abi_path.exists():
                raise Exception(
                    f'Couldn\'t find abi at {abi_path}')

            if not wasm_path.exists():
                raise Exception(
                    f'Couldn\'t find abi at {wasm_path}')

            with abi_path.open('r') as abi_file:
                abi = Abi(json.loads(abi_file.read()))

            with wasm_path.open('rb') as wasm_file:
                wasm = wasm_file.read()

            contract_artifacts[contract_dir.name] = (wasm, abi)


        wasm, abi = contract_artifacts['eosio.token']
        self.deploy_contract(
            'eosio.token', wasm, abi,
            staked=False,
            verify_hash=verify_hash
        )

        wasm, abi = contract_artifacts['eosio.msig']
        self.deploy_contract(
            'eosio.msig', wasm, abi,
            staked=False,
            verify_hash=verify_hash
        )

        wasm, abi = contract_artifacts['eosio.wrap']
        self.deploy_contract(
            'eosio.wrap', wasm, abi,
            staked=False,
            verify_hash=verify_hash
        )

        self.init_sys_token(token_sym=token_sym)

        self.activate_feature_v1('PREACTIVATE_FEATURE')

        wasm, abi = contract_artifacts['eosio.bios']
        self.sys_deploy_info = self.deploy_contract(
            'eosio', wasm, abi,
            create_account=False,
            verify_hash=verify_hash
        )

        if not activations_node:
            activations_node = self.remote_endpoint

        self.clone_node_activations(activations_node)

        wasm, abi = contract_artifacts['eosio.system']
        self.sys_deploy_info = self.deploy_contract(
            'eosio', wasm, abi,
            create_account=False,
            verify_hash=verify_hash
        )

        ec, _ = self.push_action(
            'eosio',
            'setpriv',
            [Name('eosio.msig'), UInt8(1)],
            'eosio'
        )
        assert ec == 0

        ec, _ = self.push_action(
            'eosio',
            'setpriv',
            [Name('eosio.wrap'), UInt8(1)],
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

        # Telos specific
        self.create_account_staked(
            'eosio', 'telos.decide', ram=4475000)

        self.deploy_contract(
            'telos.decide', wasm, abi,
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
        priv, pub = gen_key_pair()
        return priv, pub

    def create_key_pairs(self, n: int) -> List[Tuple[str, str]]:
        """Generate ``n`` LEAP key pairs, faster than calling
        :func:`~pytest_eosio.LEAPTestSession.create_key_pair` on a loop.

        :return: List of key pairs with a length of ``n``.
        :rtype: List[Tuple[str, str]]
        """
        keys = []
        for _ in range(n):
            keys.append(self.create_key_pair())

        self.logger.info(f'created {n} key pairs')
        return keys

    def import_key(self, account: str, private_key: str):
        """Import a private key.
        """
        public_key = get_pub_key(private_key)
        self.keys[account] = public_key
        self.private_keys[account] = private_key
        if public_key not in self._key_to_acc:
            self._key_to_acc[public_key] = []

        self._key_to_acc[public_key] += [account]

    def assign_key(self, account: str, public_key: str):
        if public_key not in self._key_to_acc:
            raise ValueError(f'{public_key} not found on other accounts')

        owner = self._key_to_acc[public_key][0]
        self.keys[account] = self.keys[owner]
        self.private_keys[account] = self.private_keys[owner]

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
        """Given a v2 feature digest, activate it.
        """
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
        """Given a v2 feature name, activate it.
        """

        digest = self.get_feature_digest(feature_name)
        self.activate_feature_with_digest(digest)

    def get_ram_price(self) -> Asset:
        row = self.get_table(
            'eosio', 'eosio', 'rammarket')[0]

        quote = asset_from_str(row['quote']['balance']).amount
        base = asset_from_str(row['base']['balance']).amount

        return Asset(
            int((quote / base) * 1024 / 0.995) * (
                10 ** self.sys_token_supply.symbol.precision),
            self.sys_token_supply.symbol)

    def _create_and_push_tx(self, actions: list[dict], key: str) -> dict:
        chain_info = self.get_info()
        ref_block_num, ref_block_prefix = get_tapos_info(
            chain_info['last_irreversible_block_id'])

        chain_id = chain_info['chain_id']

        res = None
        retries = 2
        while retries > 0:
            tx = {
                'delay_sec': 0,
                'max_cpu_usage_ms': 0,
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
        account: str,
        action: str,
        data: Union[Dict, List[Any]],
        actor: str,
        key: Optional[str] = None,
        permission: str = 'active'
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
        }], key)

        if 'error' in res:
            self.logger.error(json.dumps(res, indent=4))
            return 1, res
        else:
            return 0, res

    def push_actions(
        self,
        actions: list[dict],
        key: str,
    ):
        res = self._create_and_push_tx(actions, key)

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
            self.import_key(name, priv)

        else:
            pub = key
            self.assign_key(name, pub)

        ec, out = self.push_action(
            'eosio',
            'newaccount',
            [Name(owner), Name(name),
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
        net: Union[Asset, int] = 10 * (10 ** 4),
        cpu: Union[Asset, int] = 10 * (10 ** 4),
        ram: int = 10_000_000,
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
        if isinstance(net, int):
            net = Asset(net, self.sys_token_supply.symbol)
        if isinstance(cpu, int):
            cpu = Asset(cpu, self.sys_token_supply.symbol)

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
                Name(owner), Name(name),
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
                Name(owner), Name(name),
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
                Name(owner), Name(name),
                net, cpu, True
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

        self.create_account_staked(owner, account_name, **kwargs)
        return account_name

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
            'eosio',
            'buyrambytes',
            [Name(payer), Name(receiver), UInt32(amount)],
            payer
        )

    def wait_blocks(self, n: int):
        """Busy wait till ``n`` amount of blocks are produced on the chain.

        :param n: Number of blocks to wait.
        """
        target_block = int(self.get_info()['head_block_num']) + n

        while True:
            current_block = int(self.get_info()['head_block_num'])
            if current_block >= target_block:
                break

            remaining_blocks = target_block - current_block
            wait_time = remaining_blocks * 0.5
            time.sleep(wait_time)


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
        issuer: Union[str, Name],
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
        if isinstance(max_supply, str):
            max_supply = asset_from_str(max_supply)
        if isinstance(issuer, str):
            issuer = Name(issuer)

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
        issuer: Union[str, Name],
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
        if isinstance(quantity, str):
            quantity = asset_from_str(quantity)
        if isinstance(issuer, str):
            issuer = Name(issuer)

        return self.push_action(
            token_contract,
            'issue',
            [issuer, quantity, memo],
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
        if isinstance(quantity, str):
            quantity = asset_from_str(quantity)
        if isinstance(_from, str):
            _from = Name(_from)
        if isinstance(_to, str):
            _to = Name(_to)

        return self.push_action(
            token_contract,
            'transfer',
            [_from, _to, quantity, memo],
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
        issuer: Union[str, Name],
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
        if isinstance(quantity, str):
            quantity = asset_from_str(quantity)
        if isinstance(issuer, str):
            issuer = Name(issuer)

        return self.push_action(
            token_contract,
            'retire',
            [quantity, memo],
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
        if isinstance(sym, str):
            sym = symbol_from_str(sym)
        if isinstance(owner, str):
            owner = Name(owner)
        if isinstance(ram_payer, str):
            ram_payer = Name(ram_payer)

        return self.push_action(
            token_contract,
            'open',
            [owner, sym, ram_payer],
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
    ) -> ActionResult:
        """This action is the opposite for open, it closes the account `owner`
        for token `sym`.

        :param owner: the owner account to execute the close action for,
        :param symbol: the symbol of the token to execute the close action for.

        :return: ``token_contract.close`` execution result.
        :rtype: :ref:`typing_action_result`
        """
        if isinstance(sym, str):
            sym = symbol_from_str(sym)
        if isinstance(owner, str):
            owner = Name(owner)

        return self.push_action(
            token_contract,
            'close',
            [owner, sym],
            str(owner),
            self.private_keys[str(owner)],
            **kwargs
        )


    def init_sys_token(
        self,
        token_sym: Symbol = DEFAULT_SYS_TOKEN_SYM,
        token_amount: int = 420_000_000 * (10 ** 4)
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

    def rex_deposit(
        self,
        owner: Union[str, Name],
        quantity: Union[str, Asset]
    ):
        if isinstance(quantity, str):
            quantity = asset_from_str(quantity)
        if isinstance(owner, str):
            owner = Name(owner)

        return self.push_action(
            'eosio',
            'deposit',
            [owner, quantity],
            str(owner)
        )

    def rex_buy(
        self,
        _from: Union[str, Name],
        quantity: Union[str, Asset]
    ):
        if isinstance(quantity, str):
            quantity = asset_from_str(quantity)
        if isinstance(_from, str):
            _from = Name(_from)

        return self.push_action(
            'eosio',
            'buyrex',
            [_from, quantity],
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
        if isinstance(_from, str):
            _from = Name(_from)
        if isinstance(_to, str):
            _to = Name(_to)
        if isinstance(net, str):
            net = asset_from_str(net)
        if isinstance(cpu, str):
            cpu = asset_from_str(cpu)

        return self.push_action(
            'eosio',
            'delegatebw',
            [_from, _to, net, cpu, transfer],
            str(_from)
        )

    def register_producer(
        self,
        producer: Union[str, Name],
        url: str = '',
        location: int = 0
    ):
        if isinstance(producer, str):
            producer = Name(producer)

        producer_str = str(producer)
        return self.push_action(
            'eosio',
            'regproducer',
            [producer, PublicKey(self.keys[producer_str]), url, UInt16(location)],
            producer_str
        )

    def vote_producers(
        self,
        voter: Union[str, Name],
        proxy: Union[str, Name],
        producers: List[Union[str, Name]]
    ):
        if isinstance(voter, str):
            voter = Name(voter)
        if isinstance(proxy, str):
            proxy = Name(proxy)

        prods = [
            str(p)
            if isinstance(p, Name) else p
            for p in producers
        ]

        return self.push_action(
            'eosio',
            'voteproducer',
            [voter, proxy, ListArgument(prods, 'name')],
            str(voter)
        )

    def claim_rewards(
        self,
        owner: Union[str, Name]
    ):
        if isinstance(owner, str):
            owner = Name(owner)
        return self.push_action(
            'eosio',
            'claimrewards',
            [owner],
            str(owner)
        )

    def get_schedule(self):
        return self._post(
            f'{self.url}/v1/chain/get_producer_schedule').json()

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
