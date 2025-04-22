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
import msgspec

from leap.abis import standard
from leap.protocol import ABI
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

    def decode_traces(self, raw: bytes) -> list[dict]:
        '''
        Get an antelope formated `transaction_trace[]` payload
        and decode relevant data.

        Return msgpack encoded result.

        '''
        traces: list[dict] = standard.unpack(
            'transaction_trace[]',
            raw
        )
        ret = []

        for trace in traces:
            for act_trace in trace['action_traces']:
                action = msgspec.convert(act_trace['act'], type=Action)

                if not self.action_whitelist.is_relevant(action):
                    continue

                if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                    ret.append(act_trace)

                try:
                    act_trace['act']['data'] = action.decode(self._contracts[action.account])

                except* Exception as e:
                    e.add_note(f'while decoding action trace {action}')
                    raise e

        return (
            ret
            if self.sh_args.output_format == OutputFormats.OPTIMIZED
            else
            traces
        )

    def decode_deltas(self, raw: bytes) -> list[dict]:
        '''
        Get an antelope formated `table_delta[]` payload
        and decode relevant data.

        Return msgpack encoded result.

        '''
        deltas: list[dict] = standard.unpack(
            'table_delta[]',
            raw
        )
        ret = {}

        def ret_add_delta(name: str, row: dict):
            if name not in ret:
                ret[name] = []

            ret[name].append(row)

        for delta in deltas:
            rows = delta['rows']
            name = delta['name']

            for row in rows:
                match name:
                    case 'account':
                        if not row['present'] or not self.sh_args.decode_abis:
                            continue

                        account_row = AccountRow.from_bytes(row['data'])

                        if account_row.name not in self._contracts:
                            continue

                        if len(account_row.abi) > 0:
                            abi = standard.unpack(
                                'abi',
                                account_row.abi
                            )
                            self._contracts[account_row.name] = ABI.from_str(json.dumps(abi))

                        row['data'] = str(abi)

                    case 'contract_row':
                        contract_row = ContractRow.from_bytes(row['data'])

                        if not self.delta_whitelist.is_relevant(contract_row):
                            if self.sh_args.output_format == OutputFormats.STANDARD:
                                row['data'] = contract_row.as_dict()

                            continue

                        if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                            ret_add_delta(name, row)

                        try:
                            row['data'] = contract_row.decode(self._contracts[contract_row.code])

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
                            table_meta = standard.unpack(
                                name,
                                row['data']
                            )
                            row['data'] = table_meta

                        except* Exception as e:
                            e.add_note(f'while decoding table metadata {name} {table_meta}')
                            raise e



        return (
            ret
            if self.sh_args.output_format == OutputFormats.OPTIMIZED
            else
            deltas
        )
