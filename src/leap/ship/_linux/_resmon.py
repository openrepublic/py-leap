import os
from typing import Awaitable
from contextlib import asynccontextmanager as acm

import trio
import psutil
import tractor

from tractor.ipc import RingBufferSendChannel
from tractor.ipc._ringbuf._pubsub import RingBufferPublisher

from ._utils import import_module_path
from .structs import (
    Struct,
    PerformanceOptions,
    ResourceMonitorOptions,
    BatchSizeOptions,
    PerfTweakMsg
)
from ..structs import StateHistoryArgs


log = tractor.log.get_logger(__name__)

'''
Resource monitor service

'''
class ActorInfo(Struct):
    name: str
    pid: int
    batch_size: int | None
    tweak_fn: str | None


_actors: dict[str, ActorInfo] = {}
_teardown: dict[str, trio.Event] = {}
_tweak_fns: dict[str, Awaitable] = {}
_actor_registered = trio.Event()


@tractor.context
async def _register_actor(
    ctx: tractor.Context,
    info: ActorInfo
):
    global _actors, _actor_registered, _tweak_fns, _teardown

    info = ActorInfo.from_dict(info)

    tweak_fn = (
        import_module_path(info.tweak_fn)
        if info.tweak_fn else None
    )

    teardown = trio.Event()

    _actors[info.name] = info
    _tweak_fns[info.name] = tweak_fn
    _teardown[info.name] = teardown
    _actor_registered.set()

    log.info(f'{info.name} actor registered')

    await ctx.started()


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
            if actor.tweak_fn:
                tn.start_soon(
                    _tweak_actor,
                    actor.name,
                    opts
                )


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
    global _actors, _teardown
    perf_args = PerformanceOptions.from_dict(
        StateHistoryArgs.from_dict(sh_args).backend_kwargs
    )

    settings: ResourceMonitorOptions = perf_args.resmon

    usage_upper_bound_pct = (
        settings.cpu_threshold + settings.cpu_min_delta
    )
    usage_lower_bound_pct = (
        settings.cpu_threshold - settings.cpu_min_delta
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
            log.debug(f'measuring cpu % usage of {actor.name}')

            try:
                cpu_usage_normalized = await monitor_pid_cpu_usage(
                    pid=actor.pid,
                    samples=settings.cpu_samples,
                    interval=settings.cpu_interval
                )

            except psutil.NoSuchProcess:
                return

            log.debug(f'measured {cpu_usage_normalized:.1f} for {actor.name}')

            if (
                not settings.recommend_batch_size_updates
                or
                not actor.tweak_fn
            ):
                return

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
                actor.batch_size * settings.batch_delta_pct
            )
            new_batch_size = int(
                actor.batch_size + (batch_size_delta * delta_multiplier)
            )
            # make sure batch_size recommendation <= res_monitor_max_batch_size
            new_batch_size = min(
                settings.max_batch_size,
                new_batch_size
            )

            # make sure batch_size recommendation >= res_monitor_min_batch_size
            new_batch_size = max(
                settings.min_batch_size,
                new_batch_size
            )

            if actor.batch_size == new_batch_size:
                log.debug(
                    f'no batch_size recommendation posible for {actor.name}'
                )
                return

            old_batch_size = actor.batch_size

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
                f'{new_batch_size}, delta: {new_batch_size - old_batch_size}'
            )

            actor.batch_size = new_batch_size


        n.start_soon(_monitor)
        await ctx.started()

        try:
            await trio.sleep_forever()

        finally:
            # for event in _teardown.values():
            #     event.set()

            n.cancel_scope.cancel()
            log.info('resource_monitor exit')


'''
Resource monitor user helpers

'''
OutputTypes = RingBufferPublisher | RingBufferSendChannel

_output: OutputTypes | None = None


@tractor.context
async def performance_tweak(
    ctx: tractor.Context,
    opts: PerfTweakMsg
):
    global _output

    if not _output:
        raise RuntimeError(
            'called resmon.performance_tweak but _output not set'
        )

    opts = PerfTweakMsg.from_dict(opts)

    await ctx.started()

    _output.batch_size = opts.batch_size_options.new_batch_size

    if opts.batch_size_options.must_flush:
        await _output.flush()


async def register_actor(
    output: OutputTypes | None = None
):
    global _output

    if _output and output:
        raise RuntimeError(
            'expected only one call to `resmon.register_actor`'
        )

    _output = output

    async with (
        tractor.wait_for_actor('resource_monitor') as portal,
        portal.open_context(
            _register_actor,
            info=ActorInfo(
                name=tractor.current_actor().name,
                pid=os.getpid(),
                batch_size=(
                    output.batch_size
                    if output else None
                ),
                tweak_fn=(
                    __name__ + '.performance_tweak'
                    if output else None
                )
            )
        ) as (ctx, _),
    ):
        ...

async def broadcast_tweak(opts: PerfTweakMsg):
    async with (
        tractor.find_actor('resource_monitor') as portal,
        portal.open_context(
            _broadcast_tweak,
            opts=opts
        )
    ):
        ...
