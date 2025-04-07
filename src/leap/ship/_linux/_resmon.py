import os
from typing import Awaitable
from contextlib import asynccontextmanager as acm

import trio
import psutil
import tractor

from ._utils import import_module_path
from .structs import (
    Struct,
    PerformanceOptions,
    BatchSizeOptions,
    PerfTweakMsg
)
from ..structs import StateHistoryArgs


log = tractor.log.get_logger(__name__)


class ActorInfo(Struct):
    name: str
    pid: int
    batch_size: int
    tweak_fn: str


_actors: dict[str, ActorInfo] = {}
_tweak_fns: dict[str, Awaitable] = {}
_actor_registered = trio.Event()


@tractor.context
async def _register_actor(
    ctx: tractor.Context,
    info: ActorInfo
):
    global _actors, _actor_registered, _tweak_fns

    info = ActorInfo.from_dict(info)

    tweak_fn = import_module_path(info.tweak_fn)

    _actors[info.name] = info
    _tweak_fns[info.name] = tweak_fn
    _actor_registered.set()

    log.info(f'{info.name} actor registered')

    await ctx.started()

    async with ctx.open_stream() as stream:
        await stream.receive()

    del _tweak_fns[info.name]
    del _actors[info.name]


@acm
async def register_actor(
    batch_size: int,
    tweak_fn: str
):
    async with (
        tractor.wait_for_actor('resource_monitor') as portal,
        portal.open_context(
            _register_actor,
            info=ActorInfo(
                name=tractor.current_actor().name,
                pid=os.getpid(),
                batch_size=batch_size,
                tweak_fn=tweak_fn
            )
        ) as (ctx, _),
        ctx.open_stream(),
    ):
        yield


async def _tweak_actor(
    actor_name: str,
    opts: PerfTweakMsg
):
    global _actors, _tweak_fns
    opts = PerfTweakMsg.from_dict(opts)
    async with (
        tractor.find_actor(actor_name) as portal,
        portal.open_context(
            _tweak_fns[actor_name],
            opts=opts
        )
    ):
        ...

    if (
        opts.batch_size_options
        and
        opts.batch_size_options.new_batch_size
    ):
        _actors[actor_name].batch_size = opts.batch_size_options.new_batch_size


@tractor.context
async def _broadcast_tweak(
    ctx: tractor.Context,
    opts: PerfTweakMsg
):
    global _actors

    async with trio.open_nursery() as tn:
        await ctx.started()
        for actor in _actors.values():
            tn.start_soon(
                _tweak_actor,
                actor.name,
                opts
            )

async def broadcast_tweak(opts: PerfTweakMsg):
    async with (
        tractor.find_actor('resource_monitor') as portal,
        portal.open_context(
            _broadcast_tweak,
            opts=opts
        )
    ):
        ...


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
):
    global _actors
    perf_args = PerformanceOptions.from_dict(
        StateHistoryArgs.from_dict(sh_args).backend_kwargs
    )

    usage_upper_bound_pct = (
        perf_args.res_monitor_cpu_threshold + perf_args.res_monitor_cpu_min_delta
    )
    usage_lower_bound_pct = (
        perf_args.res_monitor_cpu_threshold - perf_args.res_monitor_cpu_min_delta
    )

    async with trio.open_nursery() as n:

        async def _monitor():
            global _actor_registered
            while True:
                if len(_actors) == 0:
                    await _actor_registered.wait()
                    _actor_registered = trio.Event()

                async with trio.open_nursery() as n:
                    for actor in _actors.values():
                        n.start_soon(_monitor_actor, actor)

        async def _monitor_actor(actor: ActorInfo):
            log.info(f'measuring cpu % usage of {actor.name}')

            try:
                cpu_usage_normalized = await monitor_pid_cpu_usage(
                    pid=actor.pid,
                    samples=perf_args.res_monitor_samples,
                    interval=perf_args.res_monitor_interval
                )

            except psutil.NoSuchProcess:
                return

            log.info(f'measured {cpu_usage_normalized:.1f} for {actor.name}')

            if (
                cpu_usage_normalized >= usage_lower_bound_pct
                and
                cpu_usage_normalized <= usage_upper_bound_pct
            ):
                log.info(f'cpu usage of {actor.name} within bounds, no update')
                return

            delta_multiplier = 1
            if cpu_usage_normalized >= usage_upper_bound_pct:
                delta_multiplier = -1

            batch_size_delta = int(
                actor.batch_size * perf_args.res_monitor_batch_delta_pct
            )
            new_batch_size = int(
                actor.batch_size + (batch_size_delta * delta_multiplier)
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

            if actor.batch_size == new_batch_size:
                log.info(
                    f'no batch_size recommendation posible for {actor.name}'
                )
                return

            await _tweak_actor(
                actor.name,
                PerfTweakMsg(
                    batch_size_options=BatchSizeOptions(
                        new_batch_size=new_batch_size
                    )
                )
            )

            log.info(
                f'batch size change for {actor.name}: '
                f'{new_batch_size}, delta: {new_batch_size - actor.batch_size}'
            )

            actor.batch_size = new_batch_size


        n.start_soon(_monitor)
        await ctx.started()
        async with ctx.open_stream() as stream:
            await stream.receive()

        n.cancel_scope.cancel()

    log.info('resource_monitor exit')
