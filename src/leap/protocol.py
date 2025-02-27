import time
import struct
from typing import TypeVar, Generic

import antelope_rs
from msgspec import Struct

Name = antelope_rs.Name
SymbolCode = antelope_rs.SymbolCode
Symbol = antelope_rs.Symbol
Asset = antelope_rs.Asset


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
    key: str,
    max_cpu_usage_ms=255,
    max_net_usage_words=0,
    ref_block_num: int = 0,
    ref_block_prefix: int = 0
) -> dict:
    return antelope_rs.create_and_sign_tx(
        bytes.fromhex(chain_id),
        actions,
        key,
        int(time.time() + 900),
        max_cpu_usage_ms,
        max_net_usage_words,
        ref_block_num,
        ref_block_prefix
    )


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
