#!/usr/bin/env python3

from typing import OrderedDict

import msgspec

from trio_websocket import open_websocket_url

from .protocol.ds import DataStream
from .protocol.abi import ABI, ABIDataStream


def encode_get_status_request() -> bytes:
    ds = DataStream()
    ds.pack_uint8(0)
    return bytes(ds.getvalue())

def encode_ack(num_msgs: int) -> bytes:
    ds = DataStream()
    ds.pack_uint8(2)
    ds.pack_uint32(num_msgs)
    return bytes(ds.getvalue())


def decode_block_result(blocks_bytes: bytes) -> tuple[int, OrderedDict]:
    ds = ABIDataStream(blocks_bytes)
    result: OrderedDict = ds.unpack_type('result')

    block_num = result['this_block']['block_num']

    try:
        if result['block']:
            ds = ABIDataStream(result['block'])
            block: OrderedDict = ds.unpack_type('signed_block')
            result['block'] = block

        if result['deltas']:
            ds = ABIDataStream(result['deltas'])
            deltas: list[OrderedDict] = ds.unpack_type('table_delta[]')

            for delta in deltas:
                rows = delta['rows']
                match delta['name']:
                    case 'account':
                        for row in rows:
                            if row['present']:
                                ds = ABIDataStream(row['data'])
                                account_delta = ds.unpack_type('account')
                                abi_raw = account_delta['abi']
                                if len(abi_raw) > 0:
                                    ds = ABIDataStream(abi_raw)
                                    new_abi = msgspec.convert(
                                        ds.unpack_type('abi'), type=ABI)

                        break

                    case 'contract_row':
                        for row in rows:
                            ds = ABIDataStream(row['data'])
                            contract_delta = ds.unpack_type('contract_row')
                            print(contract_delta)
                        break

        if result['traces']:
            ds = ABIDataStream(result['traces'])
            traces: list[OrderedDict] = ds.unpack_type('transaction_trace[]')

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        raise e

    return block_num, result


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
    max_message_size: int = 512 * 1024 * 1024
):
    async with open_websocket_url(
        endpoint,
        max_message_size=max_message_size,
        message_queue_size=max_messages_in_flight
    ) as ws:
        # first message is ABI
        _  = await ws.get_message()

        # send get_status_request
        await ws.send_message(encode_get_status_request())

        # receive get_status_result
        ds = ABIDataStream((await ws.get_message()))
        _ = ds.unpack_type('result')

        # send get_blocks_request
        ds = ABIDataStream()
        ds.pack_type(
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
        await ws.send_message(ds.getvalue())

        # receive blocks & manage acks
        acked_block = max_messages_in_flight + 1
        block_num = start_block_num - 1
        while block_num != end_block_num:
            # receive get_blocks_result
            blocks_bytes = await ws.get_message()
            block_num, block = decode_block_result(blocks_bytes)

            yield block

            if acked_block == block_num:
                # ack next batch of messages
                await ws.send_message(
                    encode_ack(max_messages_in_flight))
                acked_block += max_messages_in_flight
