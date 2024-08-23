import msgspec

from leap.protocol.abi import ABI
from .protocol.ds import ABIDataStream, DataStream

from trio_websocket import open_websocket_url


def encode_get_status_request() -> bytes:
    ds = DataStream()
    ds.pack_uint8(0)
    return bytes(ds.getvalue())

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
        abi_str  = await ws.get_message()
        abi = msgspec.json.decode(abi_str, type=ABI)

        # send get_status_request
        await ws.send_message(encode_get_status_request())

        # receive get_status_result
        ds = ABIDataStream(abi, (await ws.get_message()))
        _ = ds.unpack_variant('result')

        # send get_blocks_request
        ds = ABIDataStream(abi)
        ds.pack_variant(
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
            ds = ABIDataStream(abi, blocks_bytes)
            block_result = ds.unpack_variant('result')

            block_num = block_result[1]['this_block']['block_num']

            yield block_result

            if acked_block == block_num:
                # ack next batch of messages
                await ws.send_message(
                    encode_ack(max_messages_in_flight))
                acked_block += max_messages_in_flight
