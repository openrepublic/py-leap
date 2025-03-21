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
import enum
from base64 import b64decode
import msgspec
from msgspec import (
    Struct,
    to_builtins
)

import antelope_rs


class OutputFormats(enum.StrEnum):
    STANDARD = 'std'
    OPTIMIZED = 'optimized'


class StateHistoryArgs(Struct, frozen=True):
    endpoint: str
    start_block_num: int
    end_block_num: int = (2 ** 32) - 2
    max_messages_in_flight: int = 4000
    max_message_size: int = 512 * 1024 * 1024
    irreversible_only: bool = False
    fetch_block: bool = False
    fetch_traces: bool = False
    fetch_deltas: bool = False

    start_contracts: dict[str, bytes] = {}
    action_whitelist: dict[str, list[str]] | None = {}
    delta_whitelist: dict[str, list[str]] | None = {}

    output_format: OutputFormats = OutputFormats.OPTIMIZED
    decode_abis: bool = True
    decode_meta: bool = False

    backend_kwargs: dict = {}

    def as_msg(self):
        return to_builtins(self)

    @classmethod
    def from_msg(cls, msg: dict) -> StateHistoryArgs:
        if isinstance(msg, StateHistoryArgs):
            return msg

        return StateHistoryArgs(**msg)


class BlockPosition(Struct, frozen=True):
    block_num: int
    block_id: str


class GetBlocksResultV0(Struct, frozen=True):
    head: BlockPosition
    last_irreversible: BlockPosition
    this_block: BlockPosition
    prev_block: BlockPosition
    block: bytes | None = None
    traces: bytes | None = None
    deltas: bytes | None = None


class Block(Struct, frozen=True):
    head: BlockPosition
    last_irreversible: BlockPosition
    this_block: BlockPosition
    prev_block: BlockPosition
    block: dict | None = None
    traces: list | None = None
    deltas: list | dict | None = None

    def as_dict(self):
        return to_builtins(self)

    @classmethod
    def from_result(
        cls,
        res: GetBlocksResultV0,
        signed_block: dict | None,
        traces: list,
        deltas: list
    ) -> Block:
        return Block(
            head=res.head,
            last_irreversible=res.last_irreversible,
            this_block=res.this_block,
            prev_block=res.prev_block,
            block=signed_block,
            traces=traces,
            deltas=deltas
        )


class BlockHeader(Struct, frozen=True):
    head: BlockPosition
    last_irreversible: BlockPosition
    this_block: BlockPosition
    prev_block: BlockPosition

    @classmethod
    def from_block(
        cls,
        block: GetBlocksResultV0 | Block
    ) -> BlockHeader:
        return BlockHeader(
            head=block.head,
            last_irreversible=block.last_irreversible,
            this_block=block.this_block,
            prev_block=block.prev_block,
        )


class PermissionLevel(Struct, frozen=True):
    actor: str
    permission: str


class Action(msgspec.Struct, frozen=True):
    account: str
    name: str
    authorization: list[PermissionLevel]
    data: bytes

    def decode(self) -> dict:
        return antelope_rs.abi_unpack(
            self.account,
            self.name,
            self.data
        )

    def whitelist_keys(self) -> tuple[str, str]:
        return self.account, self.name


class AccountRow(Struct, frozen=True):
    name: str
    abi: bytes
    creation_date: str

    def as_dict(self):
        return to_builtins(self)

    @classmethod
    def from_b64(self, raw: str) -> AccountRow:
        _dtype, row = antelope_rs.abi_unpack(
            'std',
            'account',
            b64decode(raw)
        )
        return msgspec.convert(row, type=AccountRow)


class ContractRow(Struct, frozen=True):
    code: str
    scope: str
    table: str
    primary_key: int
    payer: str
    value: bytes

    def as_dict(self):
        return to_builtins(self)

    @classmethod
    def from_b64(self, raw: str) -> ContractRow:
        _dtype, row = antelope_rs.abi_unpack(
            'std',
            'contract_row',
            b64decode(raw)
        )
        return msgspec.convert(row, type=ContractRow)

    def decode(self) -> dict:
        return {
            'code': self.code,
            'scope': self.scope,
            'table': self.table,
            'primary_key': self.primary_key,
            'payer': self.payer,
            'value': antelope_rs.abi_unpack(
                self.code,
                self.table,
                self.value
            )
        }

    def whitelist_keys(self) -> tuple[str, str]:
        return self.code, self.table
