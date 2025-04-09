import msgspec
from ..structs import Struct


class ResourceMonitorOptions(Struct, frozen=True):
    # send batch size updates to monitored pids
    recommend_batch_size_updates: bool = False
    # cpu measuring amount of samples to be averaged
    cpu_samples: int = 2
    # cpu measuring sample interval
    cpu_interval: float = 1.0
    # try to keep procs cpu usage above this threshold
    cpu_threshold: float = .85
    # only suggest changes to monitored procs if above
    # threshold + delta or below threshold - delta
    cpu_min_delta: float = .05
    # batch size delta % when recomending batch_size changes
    batch_delta_pct: float = .25
    # min batch size monitor can recomend
    min_batch_size: int = 1
    # max batch size monitor can recomend
    max_batch_size: int = 2000


class PerformanceOptions(Struct, frozen=True):
    # ringbuf size in bytes
    buf_size: int = 128 * 1024 * 1024
    # ringbuf batch sizes for each step of pipeline
    ws_batch_size: int = 100
    stage_0_batch_size: int = 200
    stage_1_batch_size: int = 100
    # ringbuf publisher msgs per turn
    ws_msgs_per_turn: int = 100
    stage_0_msgs_per_turn: int = 200
    # number of stage 0 decoder procs
    decoders: int = 1
    # ratio of stage 0 -> stage 1 decoder procs
    stage_ratio: int = 2
    # root tractor nursery debug_mode
    debug_mode: bool = False
    # root tractor nursery loglevel
    loglevel: str = 'warning'
    # configuration for resource_monitor actor
    resmon: ResourceMonitorOptions = ResourceMonitorOptions()

    @property
    def stage_1_decoders(self) -> int:
        '''
        Amount of stage 1 decoder processes
        '''
        return int(self.decoders * self.stage_ratio)


# main pipeline message

class IndexedPayloadMsg(
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


# control messages

class BatchSizeOptions(Struct, frozen=True):
    new_batch_size: int | None = None
    must_flush: bool = False


class PerfTweakMsg(Struct, frozen=True):
    '''
    Signal a batch_size change on the part of then pipeline
    that receives it, with optional flush

    '''
    batch_size_options: BatchSizeOptions | None = None
