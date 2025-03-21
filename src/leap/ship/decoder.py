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
import json
from base64 import b64decode

import msgspec
import antelope_rs

from leap.sugar import LeapJSONEncoder
from leap.ship.structs import (
    OutputFormats,
    StateHistoryArgs,
    Action,
    AccountRow,
    ContractRow
)
from leap.ship._utils import Whitelist


class BlockDecoder:

    def __init__(
        self,
        sh_args: StateHistoryArgs
    ):
        self.sh_args = StateHistoryArgs.from_msg(sh_args)
        self._contracts = self.sh_args.start_contracts
        self.action_whitelist = Whitelist.from_msg(self.sh_args.action_whitelist)
        self.delta_whitelist = Whitelist.from_msg(self.sh_args.delta_whitelist)

        for account, abi in self._contracts.items():
            antelope_rs.load_abi(account, abi)

    def decode_traces(self, raw: bytes) -> bytes:
        '''
        Get an antelope formated `transaction_trace[]` payload
        and decode relevant data.

        Return msgpack encoded result.

        '''
        msgpack_traces = antelope_rs.abi_unpack_msgspec(
            'std',
            'transaction_trace[]',
            raw
        )
        traces: list[tuple[str, dict]] = msgspec.msgpack.decode(msgpack_traces)
        ret = []

        for _type, trace in traces:
            for act_type, act_trace in trace['action_traces']:
                action = msgspec.convert(act_trace['act'], type=Action)

                if not self.action_whitelist.is_relevant(action):
                    continue

                if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                    ret.append(act_trace)

                try:
                    act_trace['act']['data'] = action.decode()

                except* Exception as e:
                    e.add_note(f'while decoding action trace {action}')
                    raise e

        return msgspec.msgpack.encode(
            ret
            if self.sh_args.output_format == OutputFormats.OPTIMIZED
            else
            traces
        )

    def decode_deltas(self, raw: bytes) -> bytes:
        '''
        Get an antelope formated `table_delta[]` payload
        and decode relevant data.

        Return msgpack encoded result.

        '''
        msgpack_deltas = antelope_rs.abi_unpack_msgspec(
            'std',
            'table_delta[]',
            raw
        )
        deltas: list[tuple[str, dict]] = msgspec.msgpack.decode(msgpack_deltas)
        ret = {}

        def ret_add_delta(name: str, row: dict):
            if name not in ret:
                ret[name] = []

            ret[name].append(row)

        for delta_type, delta in deltas:
            rows = delta['rows']
            name = delta['name']

            for row in rows:
                match name:
                    case 'account':
                        if not row['present'] or not self.sh_args.decode_abis:
                            continue

                        account_row = AccountRow.from_b64(row['data'])

                        if account_row.name not in self._contracts:
                            continue

                        row_data = row['data']
                        if len(account_row.abi) > 0:
                            abi = antelope_rs.abi_unpack('std', 'abi', (account_row.abi))
                            antelope_rs.load_abi(
                                account_row.name,
                                json.dumps(abi, cls=LeapJSONEncoder).encode('utf-8')
                            )
                            row_data = abi

                        row['data'] = row_data


                    case 'contract_row':
                        contract_row = ContractRow.from_b64(row['data'])

                        if not self.delta_whitelist.is_relevant(contract_row):
                            if self.sh_args.output_format == OutputFormats.STANDARD:
                                row['data'] = contract_row.as_dict()

                            continue

                        if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                            ret_add_delta(name, row)

                        try:
                            row['data'] = contract_row.decode()

                        except* Exception as e:
                            e.add_note(f'while decoding table delta {row}')
                            raise e

                    case (
                        'permission' |
                        'account_metadata' |
                        'contract_table' |
                        'resource_usage' |
                        'resource_limits' |
                        'resource_limits_state'
                    ):
                        if not self.sh_args.decode_meta:
                            continue

                        try:
                            _dtype, table_meta = antelope_rs.abi_unpack(
                                'std',
                                name,
                                b64decode(row['data'])
                            )
                            row['data'] = table_meta

                        except* Exception as e:
                            e.add_note(f'while decoding table metadata {name} {table_meta}')
                            raise e



        return msgspec.msgpack.encode(
            ret
            if self.sh_args.output_format == OutputFormats.OPTIMIZED
            else
            deltas
        )
