#!/usr/bin/env python3

from typing import OrderedDict
from dataclasses import dataclass

from trio_websocket import open_websocket_url

from leap.errors import SerializationException

from .protocol.abi import ABI, STD_CONTRACT_ABIS, abi_pack, abi_unpack


@dataclass
class DecodedGetBlockResult:
    block_num: int
    header: OrderedDict
    deltas: list[OrderedDict]
    actions: list[OrderedDict]


def decode_block_result(
    blocks_bytes: bytes,
    contracts: dict[str, ABI],
    delta_whitelist: dict[str, list[str]] = {},
    action_whitelist: dict[str, list[str]] = {}
) -> DecodedGetBlockResult:
    result = abi_unpack('result', blocks_bytes)

    block_num = result['this_block']['block_num']

    decoded_deltas = []
    decoded_actions = []

    try:
        if result['block']:
            block = abi_unpack('signed_block', result['block'])
            result['block'] = block

        if result['deltas']:
            deltas = abi_unpack('table_delta[]', result['deltas'])

            for delta in deltas:
                rows = delta['rows']
                match delta['name']:
                    case 'account':
                        for row in rows:
                            if row['present']:
                                account_delta = abi_unpack('account', row['data'])
                                abi_raw = account_delta['abi']
                                if len(abi_raw) > 0:
                                    contracts[account_delta['name']] = abi_unpack(
                                        'abi', abi_raw, spec=ABI)

                        break

                    case 'contract_row':
                        if len(delta_whitelist) == 0:
                            continue

                        for row in rows:
                            contract_delta = abi_unpack('contract_row', row['data'])
                            table = contract_delta['table']
                            code = contract_delta['code']

                            relevant_tables = delta_whitelist.get(code, [])
                            if table not in relevant_tables:
                                continue

                            contract_abi = contracts.get(code, None)
                            if not contract_abi:
                                raise SerializationException(f'abi for contract {code} not found')

                            try:
                                decoded_deltas.append(
                                    abi_unpack(
                                        table,
                                        contract_delta['value'],
                                        abi=contract_abi
                                    )
                                )

                            except Exception as e:
                                e.add_note(f'while decoding table delta {code}::{contract_delta["table"]}')
                                raise e

                        break

        if result['traces'] and not len(action_whitelist) == 0:
            tx_traces = abi_unpack(
                'transaction_trace[]', result['traces'])

            for tx_trace in tx_traces:
                for act_trace in tx_trace['action_traces']:
                    action = act_trace['act']
                    account = action['account']
                    name = action['name']

                    if account == 'eosio' and name == 'onblock':
                        continue

                    try:
                        relevant_actions = action_whitelist.get(account, [])
                        if name not in relevant_actions:
                            continue

                        contract_abi = contracts.get(account, None)
                        if not contract_abi:
                            raise SerializationException(f'abi for contract {account} not found')

                        act_data = abi_unpack(name, action['data'], abi=contract_abi)
                        action['data'] = act_data
                        decoded_actions.append(action)

                    except Exception as e:
                        e.add_note(f'while decoding action trace {account}::{name}')
                        raise e

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        raise e

    return DecodedGetBlockResult(block_num, result, decoded_deltas, decoded_actions)


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
    contracts: dict[str, ABI] = STD_CONTRACT_ABIS,
    action_whitelist: dict[str, list[str]] = {},
    delta_whitelist: dict[str, list[str]] = {}
):
    async with open_websocket_url(
        endpoint,
        max_message_size=max_message_size,
        message_queue_size=max_messages_in_flight
    ) as ws:
        # first message is ABI
        _  = await ws.get_message()

        # send get_status_request
        await ws.send_message(abi_pack('request', ('get_status_request_v0', {})))

        # receive get_status_result
        _ = abi_unpack('result', (await ws.get_message()))

        # send get_blocks_request
        get_blocks_msg = abi_pack(
            'request',
            (
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
            )
        )
        await ws.send_message(get_blocks_msg)

        # receive blocks & manage acks
        acked_block = max_messages_in_flight + 1
        block_num = start_block_num - 1
        while block_num != end_block_num:
            # receive get_blocks_result
            blocks_bytes = await ws.get_message()
            block = decode_block_result(
                blocks_bytes, contracts,
                delta_whitelist=delta_whitelist,
                action_whitelist=action_whitelist
            )
            block_num = block.block_num

            deltas_num = len(block.deltas)
            actions_num = len(block.actions)
            # print(f'[{block_num}]: deltas: {deltas_num}, actions: {actions_num}')

            yield block

            if acked_block == block_num:
                # ack next batch of messages
                await ws.send_message(
                    abi_pack(
                        'request', (
                            'get_blocks_ack_request_v0', {'num_messages': max_messages_in_flight})))

                acked_block += max_messages_in_flight
