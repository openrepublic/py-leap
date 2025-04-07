import msgspec
from ..structs import Struct


class PerformanceOptions(Struct, frozen=True):
    # ringbuf size in bytes
    buf_size: int = 128 * 1024 * 1024
    # ringbuf batch sizes for each step of pipeline
    ws_batch_size: int = 100
    stage_0_batch_size: int = 200
    stage_1_batch_size: int = 100
    # number of stage 0 decoder procs
    decoders: int = 1
    # ratio of stage 0 -> stage 1 decoder procs
    stage_ratio: int = 2
    # root tractor nursery debug_mode
    debug_mode: bool = False
    # root tractor nursery loglevel
    loglevel: str = 'warning'

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

class EndIsNearMsg(Struct, frozen=True, tag=True):
    '''
    Sent by joiner to indicate proximity to end block
    '''
    ...


class ReachedEndMsg(Struct, frozen=True, tag=True):
    '''
    Sent when all blocks in requested range have been read
    '''
    ...

class BatchSizeOptions(Struct, frozen=True):
    new_batch_size: int | None = None
    must_flush: bool = False


class PerfTweakMsg(Struct, frozen=True, tag=True):
    '''
    Signal a batch_size change on the part of then pipeline
    that receives it, with optional flush

    '''
    batch_size_options: BatchSizeOptions | None = None


class OutputConnectMsg(Struct, frozen=True, tag=True):
    '''
    Signal a new output for the part of the pipeline that
    receives it.

    '''
    ring_name: str


class OutputDisconnectMsg(Struct, frozen=True, tag=True):
    '''
    Signal an output close for the part of the pipeline that
    receives it.
    '''
    ring_name: str


class InputConnectMsg(Struct, frozen=True, tag=True):
    '''
    Signal a new output for the part of the pipeline that
    receives it.

    '''
    ring_name: str


class InputDisconnectMsg(Struct, frozen=True, tag=True):
    '''
    Signal an output close for the part of the pipeline that
    receives it.
    '''
    ring_name: str


class ForwardMsg(Struct, frozen=True, tag=True):
    dst_actor: str
    payload: msgspec.Raw


ControlMessages = (
    EndIsNearMsg |
    ReachedEndMsg |
    PerfTweakMsg |
    OutputConnectMsg |
    OutputDisconnectMsg |
    InputConnectMsg |
    InputDisconnectMsg |
    ForwardMsg
)
