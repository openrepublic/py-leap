import json
import logging

from typing import OrderedDict
from dataclasses import dataclass

import antelope_rs
from trio_websocket import open_websocket_url

from leap.abis import STD_ABI
from leap.sugar import LeapJSONEncoder



@dataclass
class DecodedGetBlockResult:
    block_num: int
    block: OrderedDict


def decode_block_result(
    result: dict,
    delta_whitelist: dict[str, list[str]] = {},
    action_whitelist: dict[str, list[str]] = {}
) -> DecodedGetBlockResult:
    block_num = result['this_block']['block_num']

    def maybe_decode_account_row(row):
        if not row['present']:
            return

        _atype, account_delta = antelope_rs.abi_unpack('std', 'account', row['data'])
        account = str(account_delta['name'])

        if account == 'eosio':
            return

        abi_raw = account_delta['abi']
        if len(abi_raw) > 0:
            account = str(account_delta['name'])
            abi = antelope_rs.abi_unpack('std', 'abi', abi_raw)
            account_delta['abi'] = abi
            antelope_rs.load_abi(
                account,
                json.dumps(abi, cls=LeapJSONEncoder).encode('utf-8')
            )
            logging.info(f'updated abi for {account}, {len(abi_raw)} bytes')

        row['data'] = account_delta

    def maybe_decode_contract_row(row):
        _dtype, contract_delta = antelope_rs.abi_unpack('std', 'contract_row', row['data'])
        table = str(contract_delta['table'])
        code = str(contract_delta['code'])

        if len(delta_whitelist) > 0:
            relevant_tables = delta_whitelist.get(code, [])
            if (
                '*' not in relevant_tables
                and
                table not in relevant_tables
            ):
                return

        try:
            row['type'] = table
            row['data'] = antelope_rs.abi_unpack(
                code,
                table,
                contract_delta['value'],
            )
            return row

        except* Exception as e:
            e.add_note(f'while decoding table delta {code}::{contract_delta["table"]}')
            raise e

    def maybe_decode_tx_trace(tx_trace):
        for _trace_type, act_trace in tx_trace['action_traces']:
            action = act_trace['act']
            account = str(action['account'])
            name = str(action['name'])

            if account == 'eosio' and name == 'onblock':
                continue

            if len(action_whitelist) > 0:
                relevant_actions = action_whitelist.get(account, [])
                if name not in relevant_actions:
                    continue

            try:
                action['data'] = antelope_rs.abi_unpack(account, name, action['data'])

            except* Exception as e:
                e.add_note(f'while decoding action trace {account}::{name}')
                raise e


    try:
        if result['block']:
            block = antelope_rs.abi_unpack('std', 'signed_block', result['block'])
            result['block'] = block

        if result['deltas']:
            deltas = antelope_rs.abi_unpack('std', 'table_delta[]', result['deltas'])
            result['deltas'] = deltas

            for _dtype, delta in deltas:
                rows = delta['rows']
                match delta['name']:
                    case 'account':
                        [maybe_decode_account_row(row) for row in rows]

                    case 'contract_row':
                        [maybe_decode_contract_row(row) for row in rows]

                    case (
                        'permission' |
                        'account_metadata' |
                        'contract_table' |
                        'resource_usage' |
                        'resource_limits' |
                        'resource_limits_state'
                    ):
                        for i in range(len(rows)):
                            _dtype, table_info = antelope_rs.abi_unpack(
                                'std', delta['name'], rows[i]['data'])
                            rows[i]['data'] = table_info

                    case _:
                        logging.info(f'unknown delta type: {delta["name"]}')

        if result['traces']:
            tx_traces = antelope_rs.abi_unpack(
                'std', 'transaction_trace[]', result['traces'])

            result['traces'] = tx_traces

            [maybe_decode_tx_trace(tx_trace) for _type, tx_trace in tx_traces]

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        raise e

    return DecodedGetBlockResult(block_num, result)


# generator, yields blocks
async def open_state_history(
    endpoint: str,
    start_block_num: int,
    end_block_num: int = (2 ** 32) - 2,
    max_messages_in_flight: int = 10,
    irreversible_only: bool = False,
    fetch_block: bool = True,
    fetch_traces: bool = True,
    fetch_deltas: bool = True,
    max_message_size: int = 512 * 1024 * 1024,
    contracts: dict[str, dict | str | bytes] = {},
    action_whitelist: dict[str, list[str]] = {},
    delta_whitelist: dict[str, list[str]] = {}
):
    antelope_rs.load_abi('std', STD_ABI)

    for account, abi in contracts.items():
        if isinstance(abi, dict):
            abi = json.dumps(abi)

        if isinstance(abi, str):
            abi = abi.encode('utf-8')

        antelope_rs.load_abi(account, abi)

    async with open_websocket_url(
        endpoint,
        max_message_size=max_message_size,
        message_queue_size=max_messages_in_flight
    ) as ws:
        # first message is ABI
        _ = await ws.get_message()

        # send get_status_request
        await ws.send_message(
            antelope_rs.abi_pack('std', 'request', ['get_status_request_v0', {}]))

        # receive get_status_result
        status_result_bytes = await ws.get_message()
        status = antelope_rs.abi_unpack('std', 'result', status_result_bytes)
        logging.info(status)

        # send get_blocks_request
        get_blocks_msg = antelope_rs.abi_pack(
            'std',
            'request',
            [
                'get_blocks_request_v0',
                {
                    'start_block_num': start_block_num,
                    'end_block_num': end_block_num + 1,
                    'max_messages_in_flight': max_messages_in_flight,
                    'have_positions': [],
                    'irreversible_only': irreversible_only,
                    'fetch_block': fetch_block,
                    'fetch_traces': fetch_traces,
                    'fetch_deltas': fetch_deltas
                }
            ]
        )
        await ws.send_message(get_blocks_msg)

        # receive blocks & manage acks
        acked_block = start_block_num
        block_num = start_block_num - 1
        while block_num != end_block_num:
            # receive get_blocks_result
            result_bytes = await ws.get_message()
            _result_type, result = antelope_rs.abi_unpack('std', 'result', result_bytes)
            block = decode_block_result(
                result,
                delta_whitelist=delta_whitelist,
                action_whitelist=action_whitelist
            )
            block_num = block.block_num

            yield block

            if acked_block == block_num:
                # ack next batch of messages
                await ws.send_message(
                    antelope_rs.abi_pack(
                        'std',
                        'request', [
                            'get_blocks_ack_request_v0', {'num_messages': max_messages_in_flight}]))

                acked_block += max_messages_in_flight
