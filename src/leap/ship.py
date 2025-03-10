import os
from functools import partial
from contextlib import asynccontextmanager as acm

import trio
import tractor
import antelope_rs
import pyarrow as pa
import pyarrow.ipc
from trio_websocket import open_websocket_url
from pyo3_iceoryx2 import Pair0, Pair1

from leap.abis import STD_ABI


schema_bytes = {
    "data_type": "LargeUtf8",
}

schema_block_position = {
    "data_type": "Struct",
    "children": [
        {"name": "block_num", "data_type": "U32"},
        {"name": "block_id", "data_type": "Utf8"}
    ]
}

schema_get_blocks_result_v0 = {
    "fields": [
        {
            "name": "head",
            **schema_block_position
        },
        {
            "name": "last_irreversible",
            **schema_block_position
        },
        {
            "name": "this_block",
            "nullable": True,
            **schema_block_position
        },
        {
            "name": "prev_block",
            "nullable": True,
            **schema_block_position
        },
        {
            "name": "block",
            "nullable": True,
            **schema_bytes
        },
        {
            "name": "traces",
            "nullable": True,
            **schema_bytes
        },
        {
            "name": "deltas",
            "nullable": True,
            **schema_bytes
        }
    ]
}



async def stage_0_block_decoder(
    stream_key: str,
    output_key: str
):
    log = tractor.log.get_console_log(level='info')

    input_pair = Pair1(stream_key)
    out_pair = Pair0(output_key)
    input_pair.connect()
    out_pair.connect()
    log.info('connected')
    while True:
        msg = input_pair.recv()
        out_pair.send(msg)
        await trio.sleep(0)


async def result_decoder(
    stream_key: str,
    output_key: str
):
    antelope_rs.load_abi('std', STD_ABI)
    log = tractor.log.get_console_log(level='info')
    input_pair = Pair1(stream_key)
    out = Pair0(output_key)
    input_pair.connect()
    out.connect()

    log.info('connected')
    while True:
        msg = input_pair.recv()

        block = antelope_rs.abi_unpack_arrow(
            'std',
            'get_blocks_result_v0',
            msg[1:],
            schema_get_blocks_result_v0
        )
        log.info(f'unpacked into arrow ipc {len(block)}')
        out.send(block)


async def ship_reader(
    output_key: str,
    endpoint: str,
    start_block_num: int,
    end_block_num: int = (2 ** 32) - 2,
    max_messages_in_flight: int = 10,
    max_message_size: int = 512 * 1024 * 1024,
    irreversible_only: bool = False,
    fetch_block: bool = True,
    fetch_traces: bool = True,
    fetch_deltas: bool = True,
):
    log = tractor.log.get_console_log(level='info')
    log.info(f'connecting to ws {endpoint}...')
    antelope_rs.load_abi('std', STD_ABI)
    out = Pair0(output_key)
    out.connect()
    async with open_websocket_url(
        endpoint,
        max_message_size=max_message_size,
        message_queue_size=max_messages_in_flight
    ) as ws:
        # First message is ABI
        _ = await ws.get_message()
        log.info('got abi')

        # Send get_status_request
        await ws.send_message(antelope_rs.abi_pack('std', 'request', ['get_status_request_v0', {}]))

        # Receive get_status_result
        status_result_bytes = await ws.get_message()
        status = antelope_rs.abi_unpack('std', 'result', status_result_bytes)
        log.info(status)
        # Send get_blocks_request
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

        # Receive blocks & manage acks
        unacked_msgs = max_messages_in_flight
        block_num = start_block_num - 1
        while block_num != end_block_num:
            # Receive get_blocks_result
            result_bytes = await ws.get_message()
            unacked_msgs += 1
            out.send(result_bytes)

            if unacked_msgs > max_messages_in_flight:
                # Ack next batch of messages
                await ws.send_message(
                    antelope_rs.abi_pack(
                        'std',
                        'request',
                        ['get_blocks_ack_request_v0', {'num_messages': max_messages_in_flight}]
                    )
                )
                log.info('ack next')
                unacked_msgs = 0


@tractor.context
async def sync_bridge(
    ctx: tractor.Context,
    stream_key: str
) -> None:
    log = tractor.log.get_console_log(level='info')
    await ctx.started()
    try:
        async with ctx.open_stream() as stream:
            try:
                _input = Pair1(stream_key)
                _input.connect()
                log.info('connected!')
                while True:
                    msg = _input.recv()
                    await stream.send(msg)
            except BaseException as berr:
                log.exception('i am stupid UY guy..')
                raise berr
    except BaseException as berr:
        log.exception("we errored with")
        raise berr



async def _final_streamer(send_chan, *args, task_status=trio.TASK_STATUS_IGNORED, **kwargs):
    log = tractor.log.get_console_log(level='info')

    root_key = f'{os.getpid()}-open_state_history'
    result_key = root_key + '.result'

    ship_reader_key = root_key + '.ship_reader'
    result_decoder_key = root_key + '.result_decoder'

    async with tractor.open_nursery(
        loglevel='info',
        debug_mode=True,
    ) as an:
        await an.run_in_actor(
            ship_reader,
            output_key=ship_reader_key,
            **kwargs
        )
        await an.run_in_actor(
            result_decoder,
            stream_key=ship_reader_key,
            output_key=result_decoder_key,
        )
        await an.run_in_actor(
            stage_0_block_decoder,
            stream_key=result_decoder_key,
            output_key=result_key,
        )

        portal = await an.start_actor(
            'sync_bridge', enable_modules=[__name__])

        async with (
            portal.open_context(
                sync_bridge,
                stream_key=result_key
            ) as (ctx, _sent),
            ctx.open_stream() as stream,
            # send_chan,  # TRIO BUG MAYBE!?
        ):
            task_status.started(stream)
            # add to be able to keep streaming
            # await trio.sleep_forever()

        log.error("BYE MISS LAURA")


@acm
async def open_state_history(
    *args, **kwargs
):
    log = tractor.log.get_console_log()
    send_c, recv_c = trio.open_memory_channel(0)
    async def __real_final_streamer(stream):
        try:
            async with send_c:
                async for msg in stream:
                    reader = pa.ipc.RecordBatchStreamReader(pa.BufferReader(msg))
                    result = reader.read_all()
                    await send_c.send(result)
        except BaseException as berr:
            log.exception('real boi got cucked')
            raise berr

    async with (
        trio.open_nursery() as n
    ):
        stream = await n.start(partial(_final_streamer, send_c, *args, **kwargs))
        n.start_soon(__real_final_streamer, stream)
        yield recv_c

