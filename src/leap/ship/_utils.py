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
from dataclasses import dataclass

from msgspec import (
    Struct,
    to_builtins
)

from leap.protocol import Name

from .structs import StateHistoryArgs


class Whitelist(Struct, frozen=True):

    inner: dict[str, list[str]] | None

    def to_dict(self):
        return to_builtins(self.inner)

    @classmethod
    def from_dict(cls, msg: dict) -> Whitelist:
        if isinstance(msg, Whitelist):
            return msg

        return Whitelist(inner=msg)

    def is_relevant(self, obj: any) -> bool:
        if self.inner is None:
            return True

        first, second = obj.whitelist_keys()

        relevant_items = self.inner.get(first, [])
        return (
            '*' in relevant_items
            or
            second in relevant_items
        )

'''
GetBlocksResultV0 filtering tools
'''

class ParseError(ValueError):
    pass


def _read_u32_le(buf: bytes, off: int) -> tuple[int, int]:
    '''
    Read little endian u32

    '''
    if off + 4 > len(buf):
        raise ParseError('truncated uint32')

    return int.from_bytes(buf[off : off + 4], 'little'), off + 4


def _read_varuint32(buf: bytes, off: int) -> tuple[int, int]:
    '''
    Read LEB128‑style varuint32

    '''
    result = shift = 0
    while True:
        if off >= len(buf):
            raise ParseError('truncated varuint32')

        b = buf[off]
        off += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break

        shift += 7
        if shift > 28:
            raise ParseError('varuint32 too large')

    return result, off


def _skip_optional_bytes(buf: bytes, off: int) -> int:
    '''
    Seek end ptr of bytes? = (
        bool True (1) + varuint32 (1-4) + bytes(varuint32 len)
        or
        bool False (1)
    )

    '''
    flag = buf[off]
    off += 1
    if flag:
        length, off = _read_varuint32(buf, off)
        if off + length > len(buf):
            raise ParseError("truncated bytes payload")
        off += length
    return off


option_len = 1
u32_len = 4
sum256_len = 32
block_pos_len = u32_len + sum256_len
maybe_block_pos_len = option_len + block_pos_len


class ResultInspector:

    def __init__(
        self,
        sh_args: StateHistoryArgs,
    ):
        self.sh_args = StateHistoryArgs.from_dict(sh_args)

        self._header_start: int = 1
        self._header_len: int = (
            block_pos_len * 2
            +
            maybe_block_pos_len * 2
        )

    def header_of(self, result: bytes) -> memoryview:
        return result[
            self._header_start:self._header_start + self._header_len
        ]

    def traces_and_deltas_of(
        self,
        result: bytes
    ) -> tuple[
        memoryview | None,
        memoryview | None
    ]:
        off = self._header_len

        # skip block bytes?
        off = _skip_optional_bytes(result, off)

        traces: memoryview | None = None
        if result[off]:
            off += 1
            length, off = _read_varuint32(result, off)
            traces = result[off:off + length]

        else:
            off += 1

        deltas: memoryview | None = None
        if result[off]:
            off += 1
            length, off = _read_varuint32(result, off)
            deltas = result[off:off + length]

        else:
            off += 1

        return traces, deltas


@dataclass
class Receiver:
    name: str
    targets: list[str]

    raw_name: bytes
    raw_targets: dict[str, bytes]

    @classmethod
    def from_whitelist(
        self,
        whitelist: dict[str, list[str]] | None
    ) -> list[Receiver]:
        if not whitelist:
            return []

        receivers = []
        for receiver, targets in whitelist.items():
            if '*' in targets:
                targets.remove('*')

            raw_name = Name.from_str(receiver).encode()
            raw_targets = {
                target: Name.from_str(target).encode()
                for target in targets
            }

            receivers.append(
                Receiver(
                    name=receiver,
                    targets=targets,
                    raw_name=raw_name,
                    raw_targets=raw_targets
                )
            )

        return receivers


class ResultFilter:

    def __init__(
        self,
        sh_args: StateHistoryArgs
    ):
        sh_args = StateHistoryArgs.from_dict(sh_args)

        self._inspector = ResultInspector(sh_args)

        self._action_receivers: list[Receiver] = Receiver.from_whitelist(sh_args.action_whitelist)

        self._action_patterns: dict[tuple[str, str], bytes] = {}

        for receiver in self._action_receivers:
            if '*' in sh_args.action_whitelist.values():
                key = (receiver.name, '*')
                self._action_patterns[key] = receiver.raw_name
                continue

            for name in receiver.targets:
                key = (receiver.name, name)
                self._action_patterns[key] = b''.join([
                    receiver.raw_name,
                    receiver.raw_name,
                    receiver.raw_targets[name]
                ])

        self._delta_receivers: list[Receiver] = Receiver.from_whitelist(sh_args.delta_whitelist)

    @property
    def inspector(self) -> ResultInspector:
        return self._inspector

    def is_relevant(self, result: bytes) -> bool:
        search = self._inspector.traces_and_deltas_of(result)
        maybe_traces, maybe_deltas = search

        if maybe_traces:
            # first segment should be LEB128 encoded traces len
            # if its \x01 that means only onblock action present
            if maybe_traces[0] == b'\x01':
                return False

            maybe_traces = bytes(maybe_traces)

            # find any action patterns
            for key, pattern in self._action_patterns.items():
                if pattern in maybe_traces:
                    return True

        if maybe_deltas:
            maybe_deltas = bytes(maybe_deltas)
            # find deltas
            for receiver in self._delta_receivers:
                recv_search = maybe_deltas.find(receiver.raw_name)
                if recv_search > -1:
                    if len(receiver.targets) == 0:
                        return True

                    maybe_target = maybe_deltas[recv_search + 8: recv_search + 16]
                    for target in receiver.targets:
                        if receiver.raw_targets[target] == maybe_target:
                            return True

        return False
