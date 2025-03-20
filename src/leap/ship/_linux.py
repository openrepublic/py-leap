from __future__ import annotations
import os
from typing import Any
from itertools import cycle
from contextlib import (
    ExitStack,
    asynccontextmanager as acm
)

import trio
import tractor
import msgspec
import antelope_rs
from msgspec import (
    Struct,
    to_builtins
)
from tractor.ipc import (
    RBToken,
    open_ringbuf,
    attach_to_ringbuf_schannel,
    attach_to_ringbuf_rchannel,
    attach_to_ringbuf_channel,
)
from tractor.trionics import gather_contexts
from trio_websocket import open_websocket_url

from leap.abis import STD_ABI
from leap.ship.structs import StateHistoryArgs

log = tractor.log.get_logger(__name__)


SIGNED_BLOCK_TYPE = 'signed_block'
DELTAS_ARRAY_TYPE = 'table_delta[]'
TRACES_ARRAY_TYPE = 'transaction_trace[]'


class PerformanceOptions(Struct, frozen=True):
    decoders: int = 4
    debug_mode: bool = False

    def as_msg(self):
        return to_builtins(self)

    @classmethod
    def from_msg(cls, msg: dict) -> PerformanceOptions:
        if isinstance(msg, PerformanceOptions):
            return msg

        return PerformanceOptions(**msg)


class TaggedPayload(Struct, frozen=True):
    index: int
    abi_type: str
    data: bytes

    def decode_antelope(self) -> bytes:
        return antelope_rs.abi_unpack_msgspec(
            'std',
            self.abi_type,
            self.data
        )

    def decode(self, *args, **kwargs) -> Any:
        return msgspec.msgpack.decode(
            self.decode_antelope(), *args, **kwargs
        )

    @property
    def block_attr(self) -> str:
        if self.abi_type == SIGNED_BLOCK_TYPE:
            return 'block'

        if self.abi_type == DELTAS_ARRAY_TYPE:
            return 'deltas'

        if self.abi_type == TRACES_ARRAY_TYPE:
            return 'traces'

        raise ValueError(f'Unknown block attr for abi type {self.abi_type}')


@tractor.context
async def block_joiner(
    ctx: tractor.Context,
    inputs: list[RBToken],
    out_token: RBToken,
    ws_token: RBToken,
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_msg(sh_args)
    unacked_msgs = 0
    next_index = 0

    wip_block_map: dict[int, dict] = {}
    ready_block_map: dict[int, dict] = {}

    end_reached = trio.Event()

    async with (
        trio.open_nursery() as n,
        attach_to_ringbuf_schannel(out_token) as schan,
        attach_to_ringbuf_schannel(ws_token) as ws_chan,
    ):
        async def _process_ready_block(index: int):
            nonlocal next_index, unacked_msgs

            # remove from wip map and put in ready map
            block = wip_block_map.pop(index)
            ready_block_map[index] = block

            # remove all in-order blocks from ready map
            block_batch = []
            while next_index in ready_block_map:
                next_block = ready_block_map.pop(next_index)
                block_batch.append(next_block)
                next_index += 1

            # no ready blocks
            if len(block_batch) == 0:
                return

            # send all ready blocks as a single batch
            payload = msgspec.msgpack.encode(block_batch)
            await schan.send(payload)

            # maybe signal eof
            last_block_num = block_batch[-1]['this_block']['block_num']
            if last_block_num == sh_args.end_block_num - 1:
                await ws_chan.send(b'eof')
                await schan.send(b'')
                end_reached.set()
                return

            # maybe signal ws ack
            unacked_msgs += len(block_batch)
            if unacked_msgs == sh_args.max_messages_in_flight:
                await ws_chan.send(b'ack')
                unacked_msgs = 0


        async def _input_reader(in_token: RBToken):
            async with attach_to_ringbuf_rchannel(in_token) as rchan:
                async for msg in rchan:
                    payload = msgspec.msgpack.decode(msg, type=TaggedPayload)

                    block = wip_block_map.get(payload.index, None)
                    if not block:
                        block = {}
                        wip_block_map[payload.index] = block

                    decoded_msg = msgspec.msgpack.decode(payload.data)

                    if payload.abi_type == 'block_header':
                        block.update(decoded_msg)

                    else:
                        block[payload.block_attr] = decoded_msg

                    if (
                        'head' in block
                        and
                        'block' in block
                        and
                        'deltas' in block
                        and
                        'traces' in block
                    ):
                        await _process_ready_block(payload.index)

        for in_token in inputs:
            n.start_soon(_input_reader, in_token)

        await ctx.started()
        await end_reached.wait()
        n.cancel_scope.cancel()

    log.info('block_joiner exit')


@tractor.context
async def generic_decoder(
    ctx: tractor.Context,
    in_token: RBToken,
    out_token: RBToken,
):
    antelope_rs.load_abi('std', STD_ABI)

    async with (
        attach_to_ringbuf_channel(in_token, out_token) as chan
    ):
        await ctx.started()
        async for msg in chan:
            payload = msgspec.msgpack.decode(msg, type=TaggedPayload)

            if payload.abi_type == 'block_header':
                await chan.send(msg)
                continue

            result = payload.decode_antelope()

            await chan.send(
                msgspec.msgpack.encode(
                    TaggedPayload(
                        index=payload.index,
                        abi_type=payload.abi_type,
                        data=result
                    )
                )
            )

    log.info('generic_decoder exit')


@tractor.context
async def result_decoder(
    ctx: tractor.Context,
    in_token: RBToken,
    outputs: list[RBToken]
):
    antelope_rs.load_abi('std', STD_ABI)

    async with (
        gather_contexts([
            attach_to_ringbuf_schannel(token)
            for token in outputs
        ]) as out_channels,
        attach_to_ringbuf_rchannel(in_token) as rchan
    ):
        turn = cycle(range(len(outputs)))

        async def send(
            index: int,
            abi_type: str,
            data: bytes
        ):
            '''
            Round robin send

            '''
            next_index = next(turn)
            await out_channels[next_index].send(
                msgspec.msgpack.encode(TaggedPayload(
                    index=index,
                    abi_type=abi_type,
                    data=data
                ))
            )

        await ctx.started()
        async for msg in rchan:
            payload = msgspec.msgpack.decode(msg, type=TaggedPayload)
            result = payload.decode()

            for res_attr, abi_type in [
                ('block', SIGNED_BLOCK_TYPE),
                ('deltas', DELTAS_ARRAY_TYPE),
                ('traces', TRACES_ARRAY_TYPE)
            ]:
                if result[res_attr]:
                    await send(
                        payload.index,
                        abi_type,
                        bytes.fromhex(result[res_attr])
                    )
                    result.pop(res_attr)

            await send(
                payload.index,
                'block_header',
                msgspec.msgpack.encode({
                    'head': result['head'],
                    'last_irreversible': result['last_irreversible'],
                    'prev_block': result['prev_block'],
                    'this_block': result['this_block']
                })
            )

    log.info('result_decoder exit')


@tractor.context
async def ship_reader(
    ctx: tractor.Context,
    out_token: RBToken,
    ws_token: RBToken,
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_msg(sh_args)
    log.info(f'connecting to ws {sh_args.endpoint}...')
    antelope_rs.load_abi('std', STD_ABI)

    end_reached = trio.Event()

    async with (
        trio.open_nursery() as n,
        attach_to_ringbuf_schannel(out_token) as schan,
        open_websocket_url(
            sh_args.endpoint,
            max_message_size=sh_args.max_message_size,
            message_queue_size=sh_args.max_messages_in_flight
        ) as ws
    ):
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

        async def session_handler(task_status: trio.TASK_STATUS_IGNORED):
            async with attach_to_ringbuf_rchannel(ws_token) as rchan:
                task_status.started()
                async for msg in rchan:
                    if msg == b'ack':
                        await ws.send_message(
                            antelope_rs.abi_pack(
                                'std',
                                'request',
                                [
                                    'get_blocks_ack_request_v0',
                                    {'num_messages': sh_args.max_messages_in_flight}
                                ]
                            )
                        )

                    elif msg == b'eof':
                        end_reached.set()

        async def msg_proxy():
            msg_index = 0
            while True:
                msg = await ws.get_message()

                payload = TaggedPayload(
                    index=msg_index, abi_type='get_blocks_result_v0', data=msg[1:]
                )

                await schan.send(msgspec.msgpack.encode(payload))
                msg_index += 1

        await n.start(session_handler)
        n.start_soon(msg_proxy)
        await ctx.started()
        await end_reached.wait()
        await schan.send(b'')
        n.cancel_scope.cancel()

    log.info('ship_reader exit')


class BlockReceiver:

    def __init__(self, rchan):
        self._rchan = rchan

    async def __aiter__(self):
        async for batch in self._rchan:
            blocks = msgspec.msgpack.decode(batch)
            for block in blocks:
                yield block


@acm
async def open_state_history(sh_args: StateHistoryArgs):
    sh_args = StateHistoryArgs.from_msg(sh_args)
    pref_args = PerformanceOptions.from_msg(sh_args.backend_kwargs)

    root_key = f'{os.getpid()}-open_state_history'

    ship_reader_key = root_key + '.ship_reader'
    ws_session_key = root_key + '.ack'
    result_key = root_key + '.result'
    final_key = root_key + '.final'

    common = {
        'buf_size': sh_args.max_message_size
    }

    with (
        ExitStack() as stack,
        open_ringbuf(ship_reader_key, **common) as ship_token,
        open_ringbuf(ws_session_key) as ws_token,
        open_ringbuf(result_key, **common) as result_token,
        open_ringbuf(final_key, **common) as final_token
    ):
        decoder_tokens = [
            stack.enter_context(
                tractor.ipc.open_ringbuf_pair(
                    root_key + f'.decoder-{i}', buf_size=128 * 1024 * 1024
                )
            )
            for i in range(pref_args.decoders)
        ]

        decoder_in_fds, decoder_out_fds = (
            tuple(fd for i, _ in decoder_tokens for fd in i.fds),
            tuple(fd for _, o in decoder_tokens for fd in o.fds)
        )

        ship_proc_kwargs = {
            'pass_fds': ship_token.fds + ws_token.fds
        }

        result_proc_kwargs = {
            'pass_fds': ship_token.fds + result_token.fds + decoder_in_fds
        }

        decoders_proc_kwargs = {
            'pass_fds': decoder_in_fds + decoder_out_fds
        }

        joiner_proc_kwargs = {
            'pass_fds': final_token.fds + decoder_out_fds + ws_token.fds
        }

        async with (
            tractor.open_nursery(debug_mode=pref_args.debug_mode) as an,
        ):
            ship_portal = await an.start_actor(
                'ship_reader',
                enable_modules=[__name__],
                proc_kwargs=ship_proc_kwargs
            )

            result_decoder_portal = await an.start_actor(
                'result_decoder',
                enable_modules=[__name__],
                proc_kwargs=result_proc_kwargs
            )

            dec_portals = []
            for i in range(pref_args.decoders):
                dec_portals.append(await an.start_actor(
                    f'decoder-{i}',
                    enable_modules=[__name__],
                    proc_kwargs=decoders_proc_kwargs
                ))

            joiner_portal = await an.start_actor(
                'joiner',
                enable_modules=[__name__],
                proc_kwargs=joiner_proc_kwargs
            )

            async with (
                ship_portal.open_context(
                    ship_reader,
                    out_token=ship_token,
                    ws_token=ws_token,
                    sh_args=sh_args
                ) as (ship_ctx, _sent),

                result_decoder_portal.open_context(
                    result_decoder,
                    in_token=ship_token,
                    outputs=tuple((i for i, _ in decoder_tokens)),
                ) as (result_ctx, _sent),

                gather_contexts([
                    dec_portals[i].open_context(
                        generic_decoder,
                        in_token=tokens[0],
                        out_token=tokens[1],
                    ) for i, tokens in enumerate(decoder_tokens)
                ]) as _decoder_ctxs,

                joiner_portal.open_context(
                    block_joiner,
                    inputs=tuple((o for _, o in decoder_tokens)),
                    out_token=final_token,
                    ws_token=ws_token,
                    sh_args=sh_args
                ) as (joiner_ctx, _sent),

                attach_to_ringbuf_rchannel(final_token) as rchan
            ):
                yield BlockReceiver(rchan)
                await an.cancel()

            log.info('root exit')

