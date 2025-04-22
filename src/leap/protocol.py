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
import json
import time
import struct
from typing import (
    TypeVar,
    Generic,
)
from contextlib import asynccontextmanager as acm

import antelope_rs
from msgspec import Struct
from tractor.msg._codec import (
    mk_codec_from_spec,
    apply_codec
)
from tractor.msg._codec import (
    default_builtins,
    mk_dec_hook,
)
from tractor.msg._ops import limit_plds


Name = antelope_rs.Name
SymbolCode = antelope_rs.SymbolCode
Symbol = antelope_rs.Symbol
Asset = antelope_rs.Asset
ABI = antelope_rs.ABI


antelope_types = (
    Name,
    SymbolCode,
    Symbol,
    Asset,
    ABI
)


leap_dec_hook = mk_dec_hook(antelope_types)
leap_codec = mk_codec_from_spec(antelope_types)


@acm
async def apply_leap_codec(ctx=None):
    with apply_codec(leap_codec, ctx=ctx):
        yield


@acm
async def limit_leap_plds():
    with limit_plds(
        leap_codec.pld_spec,
        dec_hook=leap_dec_hook,
        ext_types=antelope_types + default_builtins,
    ) as pld_dec:
        yield pld_dec


def endian_reverse_u32(x: int) -> int:
    # Ensure x fits into 32 bits, then convert to little-endian bytes
    # and re-interpret in big-endian to reverse endianness.
    return int.from_bytes((x & 0xFFFFFFFF).to_bytes(4, byteorder='little'), byteorder='big')

def get_tapos_info(block_id: str) -> tuple[int, int]:
    block_id_bin = bytes.fromhex(block_id)

    # Unpack the first 16 bytes as two 64-bit little-endian integers
    hash0, hash1 = struct.unpack("<QQ", block_id_bin[:16])

    ref_block_num = endian_reverse_u32(hash0) & 0xFFFF
    ref_block_prefix = hash1 & 0xFFFFFFFF

    return ref_block_num, ref_block_prefix


def create_and_sign_tx(
    chain_id: str,
    actions: list[dict],
    abis: dict[str, ABI],
    key: str,
    max_cpu_usage_ms=255,
    max_net_usage_words=0,
    ref_block_num: int = 0,
    ref_block_prefix: int = 0
) -> dict:
    try:
        return antelope_rs.create_and_sign_tx(
            bytes.fromhex(chain_id),
            actions,
            abis,
            key,
            int(time.time() + 900),
            max_cpu_usage_ms,
            max_net_usage_words,
            ref_block_num,
            ref_block_prefix
        )

    except* antelope_rs.PanicException as e:
        from .sugar import LeapJSONEncoder
        e.add_note(
            f'while creating tx of: \n{json.dumps(actions, indent=4, cls=LeapJSONEncoder)}'
        )
        raise RuntimeError from e


T = TypeVar('T')
class GetTableRowsResponse(Struct, Generic[T]):
    rows: list[T]
    more: bool
    ram_payer: list[str] | None = None
    next_key: str | None = None

class ChainErrorResponse(Struct):
    code: int
    message: str
    error: dict
