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
from __future__ import annotations
import os
import time
import json
from typing import (
    AsyncContextManager
)
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

from leap.sugar import LeapJSONEncoder
from leap.ship.structs import (
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksAckRequestV0,
    GetBlocksResultV0,
    Block,
    BlockHeader,
)
from leap.ship.decoder import BlockDecoder
from leap.ship._benchmark import BenchmarkedBlockReceiver

log = tractor.log.get_logger(__name__)


SIGNED_BLOCK_TYPE = 'signed_block'
DELTAS_ARRAY_TYPE = 'table_delta[]'
TRACES_ARRAY_TYPE = 'transaction_trace[]'


class PerformanceOptions(Struct, frozen=True):
    decoders: int = 2
    decoder_buf_size: int = 128 * 1024 * 1024
    ack_buf_size: int = 512
    max_msgs_per_buf: float = 20.0
    ws_batch_size: int = 1000

    debug_mode: bool = False

    def as_msg(self):
        return to_builtins(self)

    @classmethod
    def from_dict(cls, msg: dict) -> PerformanceOptions:
        if isinstance(msg, PerformanceOptions):
            return msg

        return PerformanceOptions(**msg)


class TaggedPayload(Struct, frozen=True):
    index: int
    abi_type: str
    data_format: str
    data: msgspec.Raw

    def encode(self) -> bytes:
        return msgspec.msgpack.encode(self)

    def decode_data(self) -> bytes:
        return msgspec.msgpack.decode(self.data, type=bytes)

    def decode_antelope(self) -> bytes:
        return antelope_rs.abi_unpack_msgspec(
            'std',
            self.abi_type,
            self.decode_data()
        )

    def decode(self, type=None) -> any:
        res = msgspec.msgpack.decode(
            self.decode_antelope()
            if self.data_format == 'antelope'
            else
            self.decode_data()
        )
        if type:
            res = msgspec.convert(res, type=type)

        return res

    @property
    def block_attr(self) -> str:
        if self.abi_type == SIGNED_BLOCK_TYPE:
            return 'block'

        if self.abi_type == DELTAS_ARRAY_TYPE:
            return 'deltas'

        if self.abi_type == TRACES_ARRAY_TYPE:
            return 'traces'

        raise ValueError(f'Unknown block attr for abi type {self.abi_type}')


class AckMessage(Struct, frozen=True):
    last_block_num: int


@tractor.context
async def block_joiner(
    ctx: tractor.Context,
    inputs: list[RBToken],
    out_token: RBToken,
    ack_token: RBToken,
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    unacked_msgs = 0
    next_index = 0

    wip_block_map: dict[int, dict] = {}
    ready_block_map: dict[int, dict] = {}

    end_reached = trio.Event()

    def is_block_ready(block: dict) -> bool:
        return (
            'head' in block
            and
            (not sh_args.fetch_block or 'block' in block)
            and
            (not sh_args.fetch_deltas or 'deltas' in block)
            and
            (not sh_args.fetch_traces or 'traces' in block)
        )

    async with (
        trio.open_nursery() as n,
        attach_to_ringbuf_schannel(out_token) as schan,
        attach_to_ringbuf_schannel(ack_token) as ack_chan,
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
                await ack_chan.send(b'')
                await schan.send(b'')
                end_reached.set()
                return

            # maybe signal ws ack
            unacked_msgs += len(block_batch)
            if (
                unacked_msgs >= sh_args.max_messages_in_flight
                or
                sh_args.end_block_num - last_block_num <= sh_args.max_messages_in_flight
            ):
                await ack_chan.send(msgspec.msgpack.encode(AckMessage(
                    last_block_num=last_block_num
                )))
                unacked_msgs = 0


        async def _input_reader(in_token: RBToken):
            async with attach_to_ringbuf_rchannel(in_token) as rchan:
                async for msg in rchan:
                    payload = msgspec.msgpack.decode(msg, type=TaggedPayload)

                    block = wip_block_map.get(payload.index, None)
                    if not block:
                        block = {}
                        wip_block_map[payload.index] = block

                    decoded_msg = payload.decode()

                    if payload.abi_type == 'block_header':
                        block.update(decoded_msg)

                    else:
                        block[payload.block_attr] = decoded_msg

                    if is_block_ready(block):
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
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    decoder = BlockDecoder(sh_args)

    async with attach_to_ringbuf_channel(in_token, out_token) as chan:
        await ctx.started()
        async for msg in chan:
            payload = msgspec.msgpack.decode(msg, type=TaggedPayload)

            result: bytes
            match payload.abi_type:
                case 'block_header':
                    await chan.send(msg)
                    continue

                case 'transaction_trace[]' if sh_args.fetch_traces:
                    result = decoder.decode_traces(payload.decode_data())

                case 'table_delta[]' if sh_args.fetch_deltas:
                    result = decoder.decode_deltas(payload.decode_data())

                case _:
                    result = payload.decode_antelope()

            await chan.send(
                TaggedPayload(
                    index=payload.index,
                    abi_type=payload.abi_type,
                    data_format='msgpack',
                    data=result
                ).encode()
            )

    log.info('generic_decoder exit')


@tractor.context
async def result_decoder(
    ctx: tractor.Context,
    in_token: RBToken,
    outputs: list[RBToken],
):
    '''
    First step of the decoding process, read antelope format
    `get_blocks_result_v0`, and decode into `GetBlocksResultV0` struct
    using `msgpack` behind the scenes.

    For each of the `block`, `deltas` & `traces` result fields, craft
    a `TaggedPayload` and round robin send to generic decoder channels.

    Also send `block_header` so that it gets proxied to `block_joiner`.

    '''
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
            data_format: str,
            data: bytes
        ):
            '''
            Round robin send

            '''
            next_index = next(turn)
            await out_channels[next_index].send(
                TaggedPayload(
                    index=index,
                    abi_type=abi_type,
                    data_format=data_format,
                    data=data
                ).encode()
            )

        await ctx.started()
        async for msg in rchan:
            payload = msgspec.msgpack.decode(msg, type=TaggedPayload)
            result = payload.decode(type=GetBlocksResultV0)

            for res_attr, abi_type in [
                ('block', SIGNED_BLOCK_TYPE),
                ('deltas', DELTAS_ARRAY_TYPE),
                ('traces', TRACES_ARRAY_TYPE)
            ]:
                if getattr(result, res_attr):
                    await send(
                        payload.index,
                        abi_type,
                        'antelope',
                        getattr(result, res_attr)
                    )

            await send(
                payload.index,
                'block_header',
                'msgpack',
                msgspec.msgpack.encode(BlockHeader.from_block(result))
            )

    log.info('result_decoder exit')


@tractor.context
async def ship_reader(
    ctx: tractor.Context,
    out_token: RBToken,
    ack_token: RBToken,
    sh_args: StateHistoryArgs
):
    '''
    Handle ws connection and state history read session setup, once node begins
    sending blocks, proxy them to `out_token` ringbuf.

    Also start a background task to listen to `ack_token` ringbuf messages,
    expect only `ACK_MSG`, in case of EOF this means `block_joiner` as reached
    the configured `sh_args.end_block_num` and we must stop.
    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    pref_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    read_range = sh_args.end_block_num - sh_args.start_block_num

    batch_size = min(read_range, pref_args.ws_batch_size)
    log.info(f'connecting to ws {sh_args.endpoint}...')

    end_reached = trio.Event()

    async with (
        trio.open_nursery() as n,
        attach_to_ringbuf_schannel(
            out_token, batch_size=batch_size
        ) as schan,
        open_websocket_url(
            sh_args.endpoint,
            max_message_size=sh_args.max_message_size,
            message_queue_size=sh_args.max_messages_in_flight
        ) as ws
    ):
        # first message is ABI
        _ = await ws.get_message()
        log.info('got abi')

        # send get_status_request
        await ws.send_message(
            antelope_rs.abi_pack(
                'std',
                'request',
                GetStatusRequestV0().to_dict()
            )
        )

        # receive get_status_result
        status_result_bytes = await ws.get_message()
        status = antelope_rs.abi_unpack('std', 'result', status_result_bytes)
        log.info(status)
        # send get_blocks_request
        get_blocks_msg = antelope_rs.abi_pack(
            'std',
            'request',
            GetBlocksRequestV0(
                start_block_num=sh_args.start_block_num,
                end_block_num=sh_args.end_block_num,
                max_messages_in_flight=sh_args.max_messages_in_flight,
                have_positions=[],
                irreversible_only=sh_args.irreversible_only,
                fetch_block=sh_args.fetch_block,
                fetch_traces=sh_args.fetch_traces,
                fetch_deltas=sh_args.fetch_deltas
            ).to_dict()
        )
        await ws.send_message(get_blocks_msg)

        async def session_handler(task_status: trio.TASK_STATUS_IGNORED):
            '''
            Listen ack signals from `block_joiner` through the ack ring,
            on EOF set `end_reached` event.

            '''
            async with attach_to_ringbuf_rchannel(ack_token) as ack_chan:
                task_status.started()
                async for msg in ack_chan:
                    msg = msgspec.msgpack.decode(msg, type=AckMessage)

                    if sh_args.end_block_num - msg.last_block_num <= batch_size:
                        await schan.flush()
                        schan.batch_size = 1

                    await ws.send_message(
                        antelope_rs.abi_pack(
                            'std',
                            'request',
                            GetBlocksAckRequestV0(
                                num_messages=sh_args.max_messages_in_flight
                            ).to_dict()
                        )
                    )

                end_reached.set()

        async def msg_proxy():
            '''
            Send messages from websocket to the ship ring.

            '''
            msg_index = 0
            while True:
                msg = await ws.get_message()

                payload = TaggedPayload(
                    index=msg_index,
                    abi_type='get_blocks_result_v0',
                    data_format='antelope',
                    data=msg[1:]
                )

                await schan.send(payload.encode())
                msg_index += 1

        await n.start(session_handler)
        n.start_soon(msg_proxy)
        await ctx.started()
        await end_reached.wait()
        await schan.send(b'')
        n.cancel_scope.cancel()

    log.info('ship_reader exit')


class BlockReceiver(BenchmarkedBlockReceiver):
    '''
    Decode a stream of msgspack encoded `Block` structs

    '''

    async def _iterator(self):
        async for batch in self._rchan:
            blocks = msgspec.msgpack.decode(batch)
            self._maybe_benchmark(len(blocks))

            if self._args.output_convert:
                try:
                    blocks = msgspec.convert(blocks, type=list[Block])

                except msgspec.ValidationError as e:
                    try:
                        e.add_note(
                            'Msgspec error while decoding batch:\n' +
                            json.dumps(blocks, indent=4, cls=LeapJSONEncoder)
                        )

                    except Exception as inner_e:
                        inner_e.add_note('could not decode without type either!')
                        raise inner_e from e

                    raise e

            if self._args.output_batched:
                yield blocks

            else:
                for block in blocks:
                    yield block


@acm
async def open_state_history(
    sh_args: StateHistoryArgs
) -> AsyncContextManager[BlockReceiver]:
    '''
    Multi-process state history websocket reader.

    Using `tractor` structured concurrency actor framework for spawning,
    supervision and IPC.

    Actor overview:

        `root`: open ring buffers for IPC
            |
            +---`ship_reader`: handle ws session and proxy new blocks to the
            |       ship ring.
            |
            +---`result_decoder`: decode `get_blocks_result_v0` into py obj and
            |       break up remaining ds work and round robin send to the
            |       generic decoders through their rings.
            |
            +---`decoder-{i}`: deserialize block pieces from antelope
            |       format to msgpack and send through output ring.
            |
            +---`block_joiner`: receive out of order block pieces, assemble
                    them into blocks, when a sequence of in order blocks is
                    ready, output batch to final ring, also keep track of
                    unacked blocks and signal ack necesesity to `ship_reader`
                    through ack ring.

    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    pref_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    root_key = f'{os.getpid()}-open_state_history'

    large_buf_size = int(sh_args.max_message_size * pref_args.max_msgs_per_buf)
    common = {
        'buf_size': large_buf_size
    }

    max_mem_usage = int(
        large_buf_size * 2 +
        pref_args.decoder_buf_size * pref_args.decoders +
        pref_args.ack_buf_size
    )
    log.info(f'max_mem_usage: {max_mem_usage:,} bytes')

    # create ring buffers
    with (
        ExitStack() as stack,
        open_ringbuf(root_key + '.ship_reader', **common) as ship_token,
        open_ringbuf(root_key + '.final', **common) as final_token,
        open_ringbuf(root_key + '.ack') as ack_token
    ):
        # amount of decoders is dynamic, use an ExitStack
        decoder_tokens = [
            stack.enter_context(
                tractor.ipc.open_ringbuf_pair(
                    root_key + f'.decoder-{i}', buf_size=pref_args.decoder_buf_size
                )
            )
            for i in range(pref_args.decoders)
        ]

        decoder_in_fds, decoder_out_fds = (
            tuple(fd for i, _ in decoder_tokens for fd in i.fds),
            tuple(fd for _, o in decoder_tokens for fd in o.fds)
        )

        # resources are ready, spawn sub-actors
        # TODO: auto-pass fds automatically in tractor
        async with tractor.open_nursery(debug_mode=pref_args.debug_mode) as an:

            # writes to ship_token, reads from ack_token
            ship_portal = await an.start_actor(
                'ship_reader',
                enable_modules=[__name__],
                proc_kwargs={
                    'pass_fds': ship_token.fds + ack_token.fds
                }
            )

            # reads from ship_token, writes to all decoder_in_tokens
            result_decoder_portal = await an.start_actor(
                'result_decoder',
                enable_modules=[__name__],
                proc_kwargs={
                    'pass_fds': ship_token.fds + decoder_in_fds
                }
            )

            # each decoder reads from its decoder_in_token and writes to
            # decoder_out_token
            dec_portals = tuple([
                await an.start_actor(
                    f'decoder-{i}',
                    enable_modules=[__name__],
                    proc_kwargs={
                        'pass_fds': tokens[0].fds + tokens[1].fds
                    }
                )
                for i, tokens in enumerate(decoder_tokens)
            ])

            # reads from all decoder_out_tokens, writes to ack_token and final_token
            joiner_portal = await an.start_actor(
                'block_joiner',
                enable_modules=[__name__],
                proc_kwargs={
                    'pass_fds': final_token.fds + decoder_out_fds + ack_token.fds
                }
            )

            # all sub-actors spawned, open contexts on each of them
            async with (
                ship_portal.open_context(
                    ship_reader,
                    out_token=ship_token,
                    ack_token=ack_token,
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
                        sh_args=sh_args
                    ) for i, tokens in enumerate(decoder_tokens)
                ]) as decoder_ctxs,

                joiner_portal.open_context(
                    block_joiner,
                    inputs=tuple((o for _, o in decoder_tokens)),
                    out_token=final_token,
                    ack_token=ack_token,
                    sh_args=sh_args
                ) as (joiner_ctx, _sent),

                # finally attach root to final block channel
                attach_to_ringbuf_rchannel(final_token) as rchan
            ):
                yield BlockReceiver(rchan, sh_args)
                for dctx, _sent in decoder_ctxs:
                    await dctx.cancel()
                await joiner_ctx.cancel()
                await an.cancel()

            log.info('root exit')

