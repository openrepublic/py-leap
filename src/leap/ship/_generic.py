import json
import logging
from contextlib import asynccontextmanager as acm

import trio
import antelope_rs
from trio_websocket import open_websocket_url

from leap.abis import STD_ABI, STD_EOSIO_ABI
from leap.sugar import LeapJSONEncoder
from leap.ship.structs import StateHistoryArgs


def decode_block_result(
    result: dict,
    delta_whitelist: dict[str, list[str]] | None = None,
    action_whitelist: dict[str, list[str]] | None = None
) -> dict:
    block_num = result['this_block']['block_num']

    def maybe_decode_account_row(row):
        if not row['present']:
            return

        _atype, account_delta = antelope_rs.abi_unpack('std', 'account', bytes.fromhex(row['data']))
        account = str(account_delta['name'])

        if account == 'eosio':
            return

        abi_raw = account_delta['abi']
        if len(abi_raw) > 0:
            account = str(account_delta['name'])
            abi = antelope_rs.abi_unpack('std', 'abi', bytes.fromhex(abi_raw))
            account_delta['abi'] = abi
            antelope_rs.load_abi(
                account,
                json.dumps(abi, cls=LeapJSONEncoder).encode('utf-8')
            )
            logging.info(f'updated abi for {account}, {len(abi_raw)} bytes')

        row['data'] = account_delta

    def maybe_decode_contract_row(row):
        _dtype, contract_delta = antelope_rs.abi_unpack('std', 'contract_row', bytes.fromhex(row['data']))
        table = str(contract_delta['table'])
        code = str(contract_delta['code'])

        if delta_whitelist is not None:
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
                bytes.fromhex(contract_delta['value']),
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

            if action_whitelist is not None:
                relevant_actions = action_whitelist.get(account, [])
                if name not in relevant_actions:
                    continue

            try:
                action['data'] = antelope_rs.abi_unpack(account, name, bytes.fromhex(action['data']))

            except* Exception as e:
                e.add_note(f'while decoding action trace {account}::{name}')
                raise e


    try:
        if result['block']:
            block = antelope_rs.abi_unpack(
                'std',
                'signed_block',
                bytes.fromhex(result['block'])
            )
            result['block'] = block

        if result['deltas']:
            deltas = antelope_rs.abi_unpack(
                'std',
                'table_delta[]',
                bytes.fromhex(result['deltas'])
            )
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
                                'std', delta['name'], bytes.fromhex(rows[i]['data']))
                            rows[i]['data'] = table_info

                    case _:
                        logging.info(f'unknown delta type: {delta["name"]}')

        if result['traces']:
            tx_traces = antelope_rs.abi_unpack(
                'std',
                'transaction_trace[]',
                bytes.fromhex(result['traces'])
            )

            result['traces'] = tx_traces

            [maybe_decode_tx_trace(tx_trace) for _type, tx_trace in tx_traces]

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        raise e

    return result


@acm
async def open_state_history(sh_args: StateHistoryArgs):
    sh_args = StateHistoryArgs.from_msg(sh_args)
    antelope_rs.load_abi('std', STD_ABI)
    antelope_rs.load_abi('eosio', STD_EOSIO_ABI)

    for account, abi in sh_args.contracts.items():
        if isinstance(abi, dict):
            abi = json.dumps(abi)

        if isinstance(abi, str):
            abi = abi.encode('utf-8')

        antelope_rs.load_abi(account, abi)

    send_chan, recv_chan = trio.open_memory_channel(0)

    async with (
        trio.open_nursery() as n,
        open_websocket_url(
            sh_args.endpoint,
            max_message_size=sh_args.max_message_size,
            message_queue_size=sh_args.max_messages_in_flight
        ) as ws
    ):
        async def _receiver():
            # receive blocks & manage acks
            acked_block = sh_args.start_block_num
            block_num = sh_args.start_block_num - 1
            with send_chan:
                while block_num != sh_args.end_block_num - 1:
                    # receive get_blocks_result
                    result_bytes = await ws.get_message()
                    _result_type, result = antelope_rs.abi_unpack('std', 'result', result_bytes)
                    block = decode_block_result(
                        result,
                        delta_whitelist=sh_args.delta_whitelist,
                        action_whitelist=sh_args.action_whitelist
                    )
                    block_num = block['this_block']['block_num']

                    await send_chan.send(block)

                    if acked_block == block_num:
                        # ack next batch of messages
                        await ws.send_message(
                            antelope_rs.abi_pack(
                                'std',
                                'request', [
                                    'get_blocks_ack_request_v0', {'num_messages': sh_args.max_messages_in_flight}]))

                        acked_block += sh_args.max_messages_in_flight


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
                    'start_block_num': sh_args.start_block_num,
                    'end_block_num': sh_args.end_block_num,
                    'max_messages_in_flight': sh_args.max_messages_in_flight,
                    'have_positions': [],
                    'irreversible_only': sh_args.irreversible_only,
                    'fetch_block': sh_args.fetch_block,
                    'fetch_traces': sh_args.fetch_traces,
                    'fetch_deltas': sh_args.fetch_deltas
                }
            ]
        )
        await ws.send_message(get_blocks_msg)

        n.start_soon(_receiver)

        yield recv_chan
