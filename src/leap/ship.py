from typing import OrderedDict
from .protocol.ds import DataStream

from trio_websocket import open_websocket_url


def encode_get_status_request() -> bytes:
    ds = DataStream()
    ds.pack_uint8(0)
    return bytes(ds.getvalue())

def decode_get_status_result(status: bytes) -> OrderedDict:
    ds = DataStream(status)
    assert ds.unpack_uint8() == 0
    return ds.unpack_get_status_result_v0()

def encode_get_blocks_request(params) -> bytes:
    ds = DataStream()
    ds.pack_uint8(1)
    ds.pack_get_blocks_request_v0(params)
    return bytes(ds.getvalue())

def decode_get_blocks_result(block: bytes) -> OrderedDict:
    ds = DataStream(block)
    assert ds.unpack_uint8() == 1
    return ds.unpack_get_blocks_result_v0()

def encode_ack(num_msgs: int) -> bytes:
    ds = DataStream()
    ds.pack_uint8(2)
    ds.pack_uint32(num_msgs)
    return bytes(ds.getvalue())


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
        _ = decode_get_status_result(await ws.get_message())

        # send get_blocks_request
        blocks_request = encode_get_blocks_request({
            'start_block_num': start_block_num,
            'end_block_num': end_block_num + 1,
            'max_messages_in_flight': max_messages_in_flight,
            'have_positions': [],
            'irreversible_only': irreversible_only,
            'fetch_block': fetch_block,
            'fetch_traces': fetch_traces,
            'fetch_deltas': fetch_deltas
        })
        await ws.send_message(blocks_request)

        # receive blocks & manage acks
        acked_block = max_messages_in_flight + 1
        block_num = start_block_num - 1

        while block_num != end_block_num:
            # receive get_blocks_result
            block_bytes = await ws.get_message()
            block_result = decode_get_blocks_result(block_bytes)

            block_num = block_result['this_block']['block_num']

            yield block_result

            if acked_block == block_num:
                # ack next batch of messages
                await ws.send_message(
                    encode_ack(max_messages_in_flight))
                acked_block += max_messages_in_flight
