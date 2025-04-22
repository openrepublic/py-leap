from __future__ import annotations

import msgspec

from tractor.msg._codec import mk_msgpack_codec

from leap.ship.structs import Struct


class PerActorLoggingOptions(Struct, frozen=True):
    root: str = 'warning'

    ship_supervisor: str = 'warning'
    ship_readers: str = 'warning'

    filters: str = 'warning'

    full_decoders: str = 'warning'
    empty_decoders: str = 'warning'

    joiners: str = 'warning'


class PerformanceOptions(Struct, frozen=True):
    # empty get_blocks_result_v0.traces byte size
    empty_traces_len: int = 332
    # extra endpoints to parallel read from
    extra_endpoints: list[str] = []
    # parallel ws reader amount
    ws_readers: int = 1
    # range size per reader
    ws_range_stride: int = 100_000
    # ringbuf size in bytes
    buf_size: int = 128 * 1024 * 1024
    small_buf_size: int = 16 * 1024 * 1024

    ws_batch_size: int = 1000
    ws_msgs_per_turn: int = 1000

    empty_blocks_batch_size: int = 100
    empty_blocks_msgs_per_turn: int = 100
    full_blocks_batch_size: int = 1
    full_blocks_msgs_per_turn: int = 1
    final_batch_size: int = 1
    # number of stage 0 decoder procs per reader
    empty_decoders: int = 1
    full_decoders: int = 1
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
    data: msgspec.Raw
    meta: dict | None = None

    def new_from_data(
        self,
        new_data: msgspec.Raw,
        add_meta: dict | None = None
    ) -> IndexedPayloadMsg:
        if self.meta and add_meta:
            new_meta = self.meta.copy()
            new_meta.update(add_meta)

        return IndexedPayloadMsg(
            index=self.index,
            data=new_data,
            meta=new_meta if add_meta else self.meta
        )

    def unwrap(self) -> any:
        return msgspec.msgpack.decode(self.data)

    def decode(self, type=None) -> any:
        res = self.unwrap()

        if isinstance(res, bytes):
            res = msgspec.msgpack.decode(res)

        if type:
            res = msgspec.convert(res, type=type)

        return res


class StartRangeMsg(Struct, frozen=True, tag=True):
    start_msg_index: int
    start_block_num: int
    end_block_num: int
    is_last: bool

    @property
    def range_size(self) -> int:
        return self.end_block_num - self.start_block_num

    def __repr__(self):
        return (
            'StartRangeMsg: '
            f'{self.start_block_num}-{self.end_block_num}, '
            f'start msg index: {self.start_msg_index}'
        )


class EndReachedMsg(
    Struct,
    frozen=True,
    tag=True
):
    ...


PipelineMessages = IndexedPayloadMsg | StartRangeMsg | EndReachedMsg

pipeline_encoder, pipeline_decoder = mk_msgpack_codec(spec=PipelineMessages)


# ship range reader messages


class ABIUpdateMsg(Struct, frozen=True, tag=True):
    block_num: int
    account: str
    abi: bytes


ReaderControlMessages = StartRangeMsg | ABIUpdateMsg


class ReaderReadyMsg(Struct, frozen=True, tag=True):
    ...


# joiner messages


class StartRangeAckMsg(Struct, frozen=True):
    ...
