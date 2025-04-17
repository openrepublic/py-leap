from __future__ import annotations

import msgspec
from ..structs import Struct


class PerActorLoggingOptions(Struct, frozen=True):
    root: str = 'warning'

    resmon: str = 'warning'

    ship_supervisor: str = 'warning'
    ship_reader: str = 'warning'

    decoder_stage_0: str = 'warning'
    decoder_stage_1: str = 'warning'

    joiner: str = 'warning'


class PerformanceOptions(Struct, frozen=True):
    # extra endpoints to parallel read from
    extra_endpoints: list[str] = []
    # parallel ws reader amount
    ws_readers: int = 1
    # range size per reader
    ws_range_stride: int = 100_000
    # ringbuf size in bytes
    buf_size: int = 128 * 1024 * 1024
    # ringbuf batch sizes for each step of pipeline
    ws_batch_size: int = 100
    stage_0_batch_size: int = 200
    stage_1_batch_size: int = 100
    # ringbuf publisher msgs per turn
    ws_msgs_per_turn: int = 100
    stage_0_msgs_per_turn: int = 200
    final_batch_size: int = 1
    # number of stage 0 decoder procs
    decoders: int = 1
    # ratio of stage 0 -> stage 1 decoder procs
    stage_ratio: int = 2
    # root tractor nursery debug_mode
    debug_mode: bool = False
    # logging levels
    loglevels: PerActorLoggingOptions = PerActorLoggingOptions()


# main pipeline message

class IndexedPayloadMsg(
    Struct,
    frozen=True,
    tag=True
):
    index: int
    ws_size: int
    data: msgspec.Raw

    def new_from_data(self, new_data: msgspec.Raw) -> IndexedPayloadMsg:
        return IndexedPayloadMsg(
            index=self.index,
            ws_size=self.ws_size,
            data=new_data
        )

    def decode_data(self, type=bytes) -> bytes:
        return msgspec.msgpack.decode(self.data, type=type)

    def decode(self, type=None) -> any:
        res = msgspec.msgpack.decode(
            self.decode_data()
        )
        if type:
            res = msgspec.convert(res, type=type)

        return res


class EndReachedMsg(
    Struct,
    frozen=True,
    tag=True
):
    ...


PipelineMessages = IndexedPayloadMsg | EndReachedMsg


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
