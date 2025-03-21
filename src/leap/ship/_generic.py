import logging
from contextlib import asynccontextmanager as acm

import trio
import msgspec
import antelope_rs
from trio_websocket import open_websocket_url

from leap.ship.decoder import BlockDecoder
from leap.ship.structs import (
    StateHistoryArgs,
    GetBlocksResultV0,
    Block
)


def decode_block_result(
    result: GetBlocksResultV0,
    decoder: BlockDecoder
) -> Block:
    block_num = result.this_block.block_num

    signed_block: dict | None = None
    traces: list | None = None
    deltas: list | None = None

    try:
        if result.block:
            sblock = antelope_rs.abi_unpack(
                'std',
                'signed_block',
                result.block
            )
            signed_block = sblock

        if result.traces:
            mp_traces = decoder.decode_traces(result.traces)
            traces = msgspec.msgpack.decode(mp_traces)

        if result.deltas:
            mp_deltas = decoder.decode_deltas(result.deltas)
            deltas = msgspec.msgpack.decode(mp_deltas)

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        raise e

    return Block.from_result(result, signed_block, traces, deltas)


@acm
async def open_state_history(sh_args: StateHistoryArgs):
    sh_args = StateHistoryArgs.from_msg(sh_args)

    decoder = BlockDecoder(sh_args)

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
                        msgspec.convert(result, type=GetBlocksResultV0),
                        decoder,
                    )
                    block_num = block.this_block.block_num

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
