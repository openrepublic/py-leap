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
from antelope_rs import PanicException

from leap.abis import (
    ABI,
    standard
)
from leap.sugar import LeapJSONEncoder
from leap.ship.structs import (
    OutputFormats,
    StateHistoryArgs,
    GetBlocksResultV0,
    Action,
    AccountV0,
    RawContractRowV0,
)
from leap.ship._utils import Whitelist


class BlockDecoder:

    def __init__(
        self,
        sh_args: StateHistoryArgs
    ):
        self.sh_args = StateHistoryArgs.from_dict(sh_args)
        self._contracts = self.sh_args.start_contracts
        self.action_whitelist = Whitelist.from_dict(self.sh_args.action_whitelist)
        self.delta_whitelist = Whitelist.from_dict(self.sh_args.delta_whitelist)

    def get_abi(self, account: str) -> ABI:
        return self._contracts[account]

    def decode_traces(self, raw: bytes) -> tuple[int, dict | list]:
        '''
        Get an antelope formated `transaction_trace[]` payload
        and decode relevant data.

        '''
        try:
            traces: list[dict] = standard.unpack(
                'transaction_trace[]',
                raw
            )

        except* PanicException as e:
            e.add_note('while decoding transaction_trace[]')
            raise RuntimeError from e

        except* Exception as e:
            e.add_note('while decoding transaction_trace[]')
            raise e

        og_size = len(traces)
        ret = []

        for trace in traces:
            for act_trace in trace['action_traces']:
                action = msgspec.convert(act_trace['act'], type=Action)

                if not self.action_whitelist.is_relevant(action):
                    continue

                if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                    ret.append(act_trace)

                try:
                    act_trace['act']['data'] = action.decode(
                        self.get_abi(action.account)
                    )

                except* Exception as e:
                    e.add_note(f'while decoding action trace {action}')
                    raise e

        return (
            og_size,
            (
                ret
                if self.sh_args.output_format == OutputFormats.OPTIMIZED
                else
                traces
            )
        )

    def decode_deltas(self, raw: bytes) -> tuple[int, dict | list]:
        '''
        Get an antelope formated `table_delta[]` payload
        and decode relevant data.

        '''
        try:
            deltas: list[dict] = standard.unpack(
                'table_delta[]',
                raw
            )

        except* PanicException as e:
            e.add_note('while decoding transaction_trace[]')
            raise RuntimeError from e

        except* Exception as e:
            e.add_note('while decoding table_delta[]')
            raise e

        og_size = len(deltas)
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

                        try:
                            account_row = AccountV0.from_antelope_raw(
                                row['data'],
                                'account'
                            )
                            row['data'] = account_row

                        except* Exception as e:
                            e.add_note(f'while decoding account row {row}')
                            raise e

                        if account_row.name not in self._contracts:
                            continue

                        if len(account_row.abi) > 0:
                            abi = ABI.from_bytes(account_row.abi)
                            self._contracts[account_row.name] = abi

                    case 'contract_row':
                        try:
                            contract_row = RawContractRowV0.from_antelope_raw(
                                row['data'],
                                'contract_row'
                            )

                            if not self.delta_whitelist.is_relevant(contract_row):
                                if self.sh_args.output_format == OutputFormats.STANDARD:
                                    row['data'] = contract_row.to_dict()

                                continue

                            if self.sh_args.output_format == OutputFormats.OPTIMIZED:
                                ret_add_delta(name, row)

                            row['data'] = contract_row.decode(self.get_abi(contract_row.code))

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
            og_size,
            (
                ret
                if self.sh_args.output_format == OutputFormats.OPTIMIZED
                else
                deltas
            )
        )

    def decode_result_dict(self, result: dict) -> tuple[int, int]:
        try:
            if self.sh_args.fetch_block:
                result['block'] = standard.unpack(
                    'signed_block',
                    result['block']
                )

            og_traces = 0
            og_deltas = 0

            if self.sh_args.fetch_traces:
                og_traces, result['traces'] = self.decode_traces(result['traces'])

            if self.sh_args.fetch_deltas:
                og_deltas, result['deltas'] = self.decode_deltas(result['deltas'])

            return og_traces, og_deltas

        except* Exception as e:
            e.add_note(
                'while decoding block:\n' +
                json.dumps(
                    result,
                    indent=4,
                    cls=LeapJSONEncoder
                )
            )
            raise
