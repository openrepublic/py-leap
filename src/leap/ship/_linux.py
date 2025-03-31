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
import json
from typing import (
    AsyncContextManager
)
from contextlib import (
    ExitStack,
    asynccontextmanager as acm
)

import trio
import psutil
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
    open_ringbuf_fan_out,
    open_ringbuf_fan_in
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
)
from leap.ship.decoder import BlockDecoder
from leap.ship._benchmark import BenchmarkedBlockReceiver


log = tractor.log.get_logger(__name__)
# log = tractor.log.get_console_log(level='info')


class PerformanceOptions(Struct, frozen=True):
    # ringbuf size in bytes
    buf_size: int = 128 * 1024 * 1024
    # ringbuf batch sizes for each step of pipeline
    ws_batch_size: int = 100
    stage_0_batch_size: int = 200
    stage_1_batch_size: int = 100
    final_batch_size: int = 400
    # number of stage 0 decoder procs
    decoders: int = 1
    # ratio of stage 0 -> stage 1 decoder procs
    stage_ratio: float = 2.0
    # root tractor nursery debug_mode
    debug_mode: bool = False
    # run resource monitor
    res_monitor: bool = True
    # cpu measuring amount of samples to be averaged
    res_monitor_samples: int = 3
    # cpu measuring sample interval
    res_monitor_interval: float = 1.0
    # try to keep procs cpu usage above this threshold
    res_monitor_cpu_threshold: float = .85
    # only suggest changes to monitored procs if above
    # threshold + delta or below threshold - delta
    res_monitor_cpu_min_delta: float = .05
    # batch size delta % when recomending batch_size changes
    res_monitor_batch_delta_pct: float = .1
    # min batch size monitor can recomend
    res_monitor_min_batch_size: int = 20
    # max batch size monitor can recomend
    res_monitor_max_batch_size: int = 1000


    def as_msg(self):
        return to_builtins(self)

    @classmethod
    def from_dict(cls, msg: dict) -> PerformanceOptions:
        if isinstance(msg, PerformanceOptions):
            return msg

        return PerformanceOptions(**msg)

    @property
    def stage_1_decoders(self) -> int:
        '''
        Amount of stage 1 decoder processes
        '''
        return int(self.decoders * self.stage_ratio)


class ControlMsg(
    Struct,
    frozen=True,
    tag=True
):
    cmd: str

    def encode(self) -> bytes:
        return msgspec.msgpack.encode(self)


class IndexedPayload(
    Struct,
    frozen=True,
    tag=True
):
    index: int
    data: msgspec.Raw

    def encode(self) -> bytes:
        return msgspec.msgpack.encode(self)

    def decode_data(self, type=bytes) -> bytes:
        return msgspec.msgpack.decode(self.data, type=type)

    def decode(self, type=None) -> any:
        res = msgspec.msgpack.decode(
            self.decode_data()
        )
        if type:
            res = msgspec.convert(res, type=type)

        return res


PipelineMessages = ControlMsg | IndexedPayload


_runtime_vars = {}
_update_event = trio.Event()

async def wait_runtime_vars_update():
    global _update_event
    await _update_event.wait()
    _update_event = trio.Event()


@tractor.context
async def get_runtime_vars(
    ctx: tractor.Context
) -> dict:
    global _runtime_vars
    return _runtime_vars


@tractor.context
async def update_runtime_vars(
    ctx: tractor.Context,
    new_vars: dict
):
    global _runtime_vars, _update_event
    _runtime_vars.update(new_vars)
    _update_event.set()


async def monitor_pid_cpu_usage(
    pid: int,
    samples: int,
    interval: float
) -> float:
    proc = psutil.Process(pid)
    proc.cpu_percent(interval=0)
    _samples: list[float] = []
    for _ in range(samples):
        await trio.sleep(interval)
        _samples.append(
            proc.cpu_percent(interval=0) / 100.0  # normalize
        )

    return sum(_samples) / len(_samples)


@tractor.context
async def resource_monitor(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    actor_names: list[str],
    actor_pids: list[int]
):
    global _runtime_vars
    _runtime_vars['near_end'] = False
    perf_args = PerformanceOptions.from_dict(
        StateHistoryArgs.from_dict(sh_args).backend_kwargs
    )

    usage_upper_bound_pct = (
        perf_args.res_monitor_cpu_threshold + perf_args.res_monitor_cpu_min_delta
    )
    usage_lower_bound_pct = (
        perf_args.res_monitor_cpu_threshold - perf_args.res_monitor_cpu_min_delta
    )

    current_sizes: dict[str, int] = {}

    async with (
        gather_contexts([
            tractor.find_actor(name)
            for name in actor_names
        ]) as portals,
        trio.open_nursery() as n,
    ):
        async def _runtime_vars_listener_task():
            await wait_runtime_vars_update()
            assert _runtime_vars['near_end']
            n.cancel_scope.cancel()

        async def _monitor_actor(
            name: str,
            pid: int,
            portal: tractor.Portal,
        ):
            inital_actor_vars = await portal.run(get_runtime_vars)
            current_sizes[name] = inital_actor_vars['batch_size']

            while not _runtime_vars['near_end']:
                log.info(f'measuring cpu % usage of {name}')

                try:
                    cpu_usage_normalized = await monitor_pid_cpu_usage(
                        pid=pid,
                        samples=perf_args.res_monitor_samples,
                        interval=perf_args.res_monitor_interval
                    )

                except psutil.NoSuchProcess:
                    break

                log.info(f'measured {cpu_usage_normalized:.1f} for {name}')

                if (
                    cpu_usage_normalized >= usage_lower_bound_pct
                    and
                    cpu_usage_normalized <= usage_upper_bound_pct
                ):
                    log.info(f'cpu usage of {name} within bounds, no update')
                    continue

                delta_multiplier = 1
                if cpu_usage_normalized >= usage_upper_bound_pct:
                    delta_multiplier = -1

                batch_size_delta = int(
                    current_sizes[name] * perf_args.res_monitor_batch_delta_pct
                )
                new_batch_size = int(
                    current_sizes[name] + (batch_size_delta * delta_multiplier)
                )
                # make sure batch_size recommendation <= res_monitor_max_batch_size
                new_batch_size = min(
                    perf_args.res_monitor_max_batch_size,
                    new_batch_size
                )

                # make sure batch_size recommendation >= res_monitor_min_batch_size
                new_batch_size = max(
                    perf_args.res_monitor_min_batch_size,
                    new_batch_size
                )

                if current_sizes[name] == new_batch_size:
                    log.info(
                        f'no batch_size recommendation posible for {name}'
                    )
                    continue

                delta = new_batch_size - current_sizes[name]
                await portal.run(
                    update_runtime_vars,
                    new_vars={'batch_size': new_batch_size}
                )

                current_sizes[name] = new_batch_size
                log.info(
                    f'batch size change for {name}: {new_batch_size}, delta: {delta}'
                )

            log.info(f'_monitor_actor {name} exited')

        n.start_soon(_runtime_vars_listener_task)
        for name, pid, portal in zip(
            actor_names, actor_pids, portals
        ):
            n.start_soon(_monitor_actor, name, pid, portal)

        await ctx.started()

    log.info('resource_monitor exit')


@tractor.context
async def block_joiner(
    ctx: tractor.Context,
    inputs: list[RBToken],
    out_token: RBToken,
    sh_args: StateHistoryArgs
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    ringbuf_depth = int(
        (perf_args.res_monitor_max_batch_size * ((perf_args.decoders * perf_args.stage_ratio)) + 2)
        +
        (sh_args.max_messages_in_flight * 2)
    )
    log.info(f'depth: {ringbuf_depth}')

    next_index = 0
    near_end = False

    block_map: dict[int, dict] = {}

    acms = [
        attach_to_ringbuf_schannel(
            out_token,
            batch_size=min(sh_args.block_range, perf_args.final_batch_size)
        ),
        open_ringbuf_fan_in(inputs),
        tractor.wait_for_actor('ship_reader'),
    ]

    if perf_args.res_monitor:
        acms.append(tractor.wait_for_actor('resource_monitor'))

    async with gather_contexts(acms) as results:
        if perf_args.res_monitor:
            schan, rchan, ship_portal, res_portal = results

        else:
            schan, rchan, ship_portal = results

        await ctx.started()

        async for msg in rchan:
            payload = msgspec.msgpack.decode(msg, type=IndexedPayload)
            block_map[payload.index] = payload.decode_data(type=dict)

            # remove all in-order blocks from ready map
            block_batch = []
            while next_index in block_map:
                next_block = block_map.pop(next_index)
                block_batch.append(next_block)
                next_index += 1

            # no ready blocks
            if len(block_batch) == 0:
                continue

            # send all ready blocks as a single batch
            payload = msgspec.msgpack.encode(block_batch)
            await schan.send(payload)

            last_block_num = block_batch[-1]['this_block']['block_num']

            # maybe signal near end
            if (
                not near_end
                and
                sh_args.end_block_num - last_block_num
                <=
                ringbuf_depth
            ):
                near_end = True
                new_vars = {'near_end': True}
                await ship_portal.run(
                    update_runtime_vars, new_vars=new_vars
                )
                if perf_args.res_monitor:
                    await res_portal.run(
                        update_runtime_vars, new_vars=new_vars
                    )

                await schan.flush(new_batch_size=1)

            # maybe signal eof
            if last_block_num == sh_args.end_block_num - 1:
                await ship_portal.run(
                    update_runtime_vars, new_vars={'reached_end': True}
                )
                await schan.send_eof()
                break

    log.info('block_joiner exit')


@tractor.context
async def stage_1_decoder(
    ctx: tractor.Context,
    id: int,
    sh_args: StateHistoryArgs,
    in_token: RBToken,
    out_token: RBToken,
):
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_1_batch_size)
    global _runtime_vars
    _runtime_vars['batch_size'] = batch_size

    decoder = BlockDecoder(sh_args)

    async with (
        attach_to_ringbuf_channel(
            in_token,
            out_token,
            batch_size=batch_size
        ) as chan,
        trio.open_nursery() as n,
    ):
        listener_scope = trio.CancelScope()
        async def _runtime_vars_listener_task():
            with listener_scope:
                while True:
                    await wait_runtime_vars_update()
                    chan.batch_size = _runtime_vars['batch_size']

        n.start_soon(_runtime_vars_listener_task)

        await ctx.started(os.getpid())

        async for msg in chan:
            payload = msgspec.msgpack.decode(msg, type=PipelineMessages)

            match payload:
                case IndexedPayload():
                    result = payload.decode(type=GetBlocksResultV0)
                    await chan.send(
                        IndexedPayload(
                            index=payload.index,
                            data=decoder.decode_block_result(result)
                        ).encode()
                    )

                case ControlMsg():
                    match payload.cmd:
                        case 'flush':
                            listener_scope.cancel()
                            await chan.flush(new_batch_size=1)

    log.info(f'stage_1_decoder[{id}] exit')


@tractor.context
async def stage_0_decoder(
    ctx: tractor.Context,
    id: int,
    sh_args: StateHistoryArgs,
    in_token: RBToken,
    outputs: list[RBToken],
):
    '''
    First step of the decoding process, read antelope format
    `get_blocks_result_v0`, and decode into `GetBlocksResultV0` struct
    using `msgpack` behind the scenes.

    For each of the `block`, `deltas` & `traces` result fields, craft
    a `IndexedPayload` and round robin send to generic decoder channels.

    Also send `block_header` so that it gets proxied to `block_joiner`.

    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.stage_0_batch_size)
    global _runtime_vars
    _runtime_vars['batch_size'] = batch_size

    async with (
        attach_to_ringbuf_rchannel(
            in_token
        ) as rchan,
        open_ringbuf_fan_out(
            outputs,
            batch_size=batch_size
        ) as schan,
        trio.open_nursery() as n,
    ):
        listener_scope = trio.CancelScope()
        async def _runtime_vars_listener_task():
            with listener_scope:
                while True:
                    await wait_runtime_vars_update()
                    schan.batch_size = _runtime_vars['batch_size']

        n.start_soon(_runtime_vars_listener_task)

        await ctx.started(os.getpid())
        async for msg in rchan:
            payload = msgspec.msgpack.decode(msg, type=PipelineMessages)

            match payload:
                case IndexedPayload():
                    result = antelope_rs.abi_unpack_msgspec(
                        'std',
                        'get_blocks_result_v0',
                        payload.decode_data()
                    )
                    await schan.send(
                        IndexedPayload(
                            index=payload.index,
                            data=result
                        ).encode()
                    )

                case ControlMsg():
                    match payload.cmd:
                        case 'flush':
                            listener_scope.cancel()
                            # forward flush downstream
                            await schan.broadcast(msg)
                            await schan.flush(new_batch_size=1)

    log.info(f'stage_0_decoder[{id}] exit')


@tractor.context
async def ship_reader(
    ctx: tractor.Context,
    sh_args: StateHistoryArgs,
    outputs: list[RBToken],
):
    '''
    Handle ws connection and state history read session setup, once node begins
    sending blocks, proxy them to `out_token` ringbuf.

    Also start a background task to listen to `ack_token` ringbuf messages,
    expect only `ACK_MSG`, in case of EOF this means `block_joiner` as reached
    the configured `sh_args.end_block_num` and we must stop.
    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )
    batch_size = min(sh_args.block_range, perf_args.ws_batch_size)

    global _runtime_vars
    _runtime_vars['near_end'] = False
    _runtime_vars['reached_end'] = False
    _runtime_vars['batch_size'] = batch_size

    log.info(f'connecting to ws {sh_args.endpoint}...')

    async with (
        open_ringbuf_fan_out(
            outputs,
            batch_size=batch_size
        ) as schan,
        open_websocket_url(
            sh_args.endpoint,
            max_message_size=sh_args.max_message_size,
            message_queue_size=sh_args.max_messages_in_flight
        ) as ws,
        trio.open_nursery() as n,
    ):
        async def _runtime_vars_listener_task():
            while True:
                await wait_runtime_vars_update()
                if _runtime_vars['reached_end']:
                    await schan.broadcast_eof()
                    break

                if _runtime_vars['near_end']:
                    await schan.broadcast(
                        ControlMsg(cmd='flush').encode()
                    )
                    await schan.flush(new_batch_size=1)

                elif schan.batch_size != _runtime_vars['batch_size']:
                    schan.batch_size = _runtime_vars['batch_size']

            n.cancel_scope.cancel()

        async def msg_proxy():
            msg_index = 0
            unacked_msgs = 0
            while True:
                msg = await ws.get_message()
                unacked_msgs += 1

                payload = IndexedPayload(
                    index=msg_index,
                    data=msg[1:]
                )

                await schan.send(payload.encode())
                msg_index += 1

                if unacked_msgs >= sh_args.max_messages_in_flight:
                    await ws.send_message(
                        antelope_rs.abi_pack(
                            'std',
                            'request',
                            GetBlocksAckRequestV0(
                                num_messages=sh_args.max_messages_in_flight
                            ).to_dict()
                        )
                    )
                    unacked_msgs = 0

        # first message is ABI
        _ = await ws.get_message()

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
        log.info(f'node status: {status}')

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
        log.info(
            f'sent get blocks request, '
            f'range: {sh_args.start_block_num}-{sh_args.end_block_num}, '
            f'max_msgs_in_flight: {sh_args.max_messages_in_flight}'
        )

        n.start_soon(_runtime_vars_listener_task)
        n.start_soon(msg_proxy)
        await ctx.started(os.getpid())

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

    '''
    sh_args = StateHistoryArgs.from_dict(sh_args)
    perf_args = PerformanceOptions.from_dict(
        sh_args.backend_kwargs
    )

    stage_0_dec_amount = perf_args.decoders
    stage_1_dec_amount = int(perf_args.decoders * perf_args.stage_ratio)

    root_key = f'{os.getpid()}-open_state_history'

    common = {
        'buf_size': perf_args.buf_size
    }

    max_mem_usage = int(
        perf_args.buf_size
        *
        (
            1  # .final
            +
            stage_0_dec_amount  # stage_0_inputs
            +
            (stage_1_dec_amount * 2) # stage_0_outputs + stage_1
        )
    )
    log.info(f'max_mem_usage: {max_mem_usage:,} bytes')

    # create ring buffers
    with (
        ExitStack() as stack,
        open_ringbuf(root_key + '.final', **common) as final_token,
    ):
        # amount of decoders is dynamic, use an ExitStack
        stage_0_input_tokens = [
            stack.enter_context(
                open_ringbuf(
                    root_key + f'.stage-0-input-{i}', **common
                )
            )
            for i in range(stage_0_dec_amount)
        ]

        stage_0_in_fds = tuple(fd for token in stage_0_input_tokens for fd in token.fds)

        stage_0_output_tokens = [
            stack.enter_context(
                open_ringbuf(
                    root_key + f'.stage-0-output-{i}', **common
                )
            )
            for i in range(stage_1_dec_amount)
        ]

        stage_0_out_fds = tuple(fd for token in stage_0_output_tokens for fd in token.fds)

        stage_1_tokens = [
            stack.enter_context(
                open_ringbuf(
                    root_key + f'.stage-1-{i}', **common
                )
            )
            for i in range(stage_1_dec_amount)
        ]

        stage_1_fds = tuple(fd for token in stage_1_tokens for fd in token.fds)

        # resources are ready, spawn sub-actors
        # TODO: auto-pass fds automatically in tractor
        async with tractor.open_nursery(
            debug_mode=perf_args.debug_mode
        ) as an:

            # writes to ship_token, reads from ack_token
            ship_portal = await an.start_actor(
                'ship_reader',
                enable_modules=[__name__],
                proc_kwargs={
                    'pass_fds': stage_0_in_fds
                }
            )

            stage_0_portals = [
                await an.start_actor(
                    f'stage-0-decoder-{i}',
                    enable_modules=[__name__],
                    proc_kwargs={
                        'pass_fds': stage_0_in_fds + stage_0_out_fds
                    }
                )
                for i in range(stage_0_dec_amount)
            ]

            stage_1_portals = [
                await an.start_actor(
                    f'stage-1-decoder-{i}',
                    enable_modules=[__name__],
                    proc_kwargs={
                        'pass_fds': stage_0_out_fds + stage_1_fds
                    }
                )
                for i in range(stage_1_dec_amount)
            ]

            # reads from all decoder_out_tokens, writes to ack_token and final_token
            joiner_portal = await an.start_actor(
                'block_joiner',
                enable_modules=[__name__],
                proc_kwargs={
                    'pass_fds': final_token.fds + stage_1_fds
                }
            )

            # all sub-actors spawned, open contexts on each of them
            async with (
                ship_portal.open_context(
                    ship_reader,
                    sh_args=sh_args,
                    outputs=stage_0_input_tokens,
                ) as (ship_ctx, ship_pid),

                gather_contexts([
                    stage_0_portals[i].open_context(
                        stage_0_decoder,
                        id=i,
                        sh_args=sh_args,
                        in_token=stage_0_input_tokens[i],
                        outputs=stage_0_output_tokens[
                            int(i * perf_args.stage_ratio)
                            :
                            int((i + 1) * perf_args.stage_ratio)
                        ],
                    ) for i in range(stage_0_dec_amount)
                ]) as stage_0_ctxs,

                gather_contexts([
                    stage_1_portals[i].open_context(
                        stage_1_decoder,
                        id=i,
                        sh_args=sh_args,
                        in_token=stage_0_output_tokens[i],
                        out_token=stage_1_tokens[i],
                    ) for i in range(stage_1_dec_amount)
                ]) as stage_1_ctxs,
            ):

                final_acms = []
                if perf_args.res_monitor:

                    actor_names: list[str] = [
                        ship_portal.chan.uid[0],
                        *[
                            p.chan.uid[0]
                            for p in stage_0_portals
                        ],
                        *[
                            p.chan.uid[0]
                            for p in stage_1_portals
                        ],
                    ]

                    actor_pids: list[int] = [
                        ship_pid,
                        *[
                            pid
                            for _, pid in stage_0_ctxs
                        ],
                        *[
                            pid
                            for _, pid in stage_1_ctxs
                        ],
                    ]

                    res_monitor_portal = await an.start_actor(
                        'resource_monitor',
                        enable_modules=[__name__]
                    )
                    final_acms += [res_monitor_portal.open_context(
                        resource_monitor,
                        sh_args=sh_args,
                        actor_names=actor_names,
                        actor_pids=actor_pids
                    )]

                final_acms += [
                    joiner_portal.open_context(
                        block_joiner,
                        sh_args=sh_args,
                        inputs=stage_1_tokens,
                        out_token=final_token,
                    ),

                    attach_to_ringbuf_rchannel(final_token)
                ]

                async with gather_contexts(final_acms) as results:
                    if perf_args.res_monitor:
                        res_mon_ctx, joiner_ctx, rchan = results

                    else:
                        joiner_ctx, rchan = results

                    yield BlockReceiver(rchan, sh_args)

                    if perf_args.res_monitor:
                        res_ctx, _sent = res_mon_ctx
                        await res_ctx.wait_for_result()

                    jctx, _sent = joiner_ctx
                    await jctx.wait_for_result()

                    for ctx, _sent in stage_0_ctxs:
                        await ctx.wait_for_result()

                    for ctx, _sent in stage_1_ctxs:
                        await ctx.wait_for_result()

                    await ship_ctx.wait_for_result()

                    await an.cancel()

            log.info('root exit')

