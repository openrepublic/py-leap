import trio
import psutil
import tractor
import msgspec

from .structs import (
    Struct,
    PerformanceOptions,
    EndIsNearMsg,
    BatchSizeOptions,
    PerfTweakMsg,
    ForwardMsg,
    ControlMessages
)
from ..structs import StateHistoryArgs


log = tractor.log.get_logger(__name__)


class ActorInfo(Struct):
    name: str
    pid: int
    batch_size: int


_actors: list[ActorInfo] = []
_actor_registered = trio.Event()


@tractor.context
async def register_actor(
    ctx: tractor.Context,
    info: ActorInfo
):
    global _actors, _actor_registered
    await ctx.started()
    _actors.append(ActorInfo.from_dict(info))
    _actor_registered.set()


@tractor.context
async def unregister_actor(
    ctx: tractor.Context,
    actor_name: str
):
    global _actors
    await ctx.started()

    i = None
    for i, act in enumerate(_actors):
        if act.name == actor_name:
            break

    if not i:
        raise ValueError('No actor registered with name {actor_name}')

    del _actors[i]


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

    async with (
        ctx.open_stream() as control_stream,
        trio.open_nursery() as n,
    ):
        async def _control_listener_task():
            async for msg in control_stream:
                msg = msgspec.msgpack.decode(msg, type=ControlMessages)

                match msg:
                    case EndIsNearMsg():
                        break

            n.cancel_scope.cancel()

        async def _monitor():
            global _actor_registered
            while True:
                if len(_actors) == 0:
                    await _actor_registered.wait()
                    _actor_registered = trio.Event()

                async with trio.open_nursery() as n:
                    for actor in _actors:
                        n.start_soon(_monitor_actor(actor))

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

            await control_stream.send(
                ForwardMsg(
                    dst_actor=actor.name,
                    payload=PerfTweakMsg(
                        batch_size_options=BatchSizeOptions(
                            new_batch_size=new_batch_size
                        )
                    )
                ).encode()
            )

            log.info(
                f'batch size change for {actor.name}: '
                f'{new_batch_size}, delta: {new_batch_size - actor.batch_size}'
            )

            actor.batch_size = new_batch_size

        await ctx.started()
        n.start_soon(_control_listener_task)
        n.start_soon(_monitor)

    log.info('resource_monitor exit')
