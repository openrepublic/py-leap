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
from typing import (
    Any,
)

import msgspec
from msgspec import to_builtins

import antelope_rs


class Struct(msgspec.Struct):

    def encode(self) -> bytes:
        return msgspec.msgpack.encode(self)

    def to_dict(self):
        return to_builtins(self)

    @classmethod
    def from_dict(cls, d: dict | Struct):
        if isinstance(d, cls):
            return d

        return msgspec.convert(d, type=cls)


class OutputFormats(enum.StrEnum):
    STANDARD = 'std'
    OPTIMIZED = 'optimized'


class StateHistoryArgs(Struct, frozen=True):
    endpoint: str
    start_block_num: int
    end_block_num: int = (2 ** 32) - 2
    max_messages_in_flight: int = 1000
    max_message_size: int = 10 * 1024 * 1024  # 10 mb
    irreversible_only: bool = False
    fetch_block: bool = False
    fetch_traces: bool = False
    fetch_deltas: bool = False

    start_contracts: dict[str, bytes] = {}
    action_whitelist: dict[str, list[str]] | None = {}
    delta_whitelist: dict[str, list[str]] | None = {}

    output_batched: bool = False
    output_format: OutputFormats = OutputFormats.OPTIMIZED
    output_convert: bool = True
    decode_abis: bool = True
    decode_meta: bool = False

    benchmark: bool = False
    benchmark_sample_time: float = 1.0
    benchmark_max_samples: int = 10

    backend: str = 'default'
    backend_kwargs: dict = {}

    @property
    def block_range(self) -> int:
        return self.end_block_num - self.start_block_num


class GetStatusRequestV0(
    Struct,
    frozen=True,
    tag='get_status_request_v0',
):
    ...


class BlockPosition(Struct, frozen=True):
    block_num: int
    block_id: str


class GetBlocksRequestV0(
    Struct,
    frozen=True,
    tag='get_blocks_request_v0',
):
    start_block_num: int
    end_block_num: int
    max_messages_in_flight: int
    have_positions: list[BlockPosition]
    irreversible_only: bool
    fetch_block: bool
    fetch_deltas: bool
    fetch_traces: bool

class GetBlocksAckRequestV0(
    Struct,
    frozen=True,
    tag='get_blocks_ack_request_v0'
):
    num_messages: int


class GetBlocksResultV0(
    Struct,
    frozen=True
):
    head: BlockPosition
    last_irreversible: BlockPosition
    this_block: BlockPosition
    prev_block: BlockPosition
    block: bytes | None = None
    traces: bytes | None = None
    deltas: bytes | None = None


class PackedTransaction(Struct, frozen=True):
    compression: int | None
    packed_context_free_data: bytes
    packed_trx: bytes
    signatures: list[str]


class Transaction(Struct, frozen=True):
    cpu_usage_us: int
    net_usage_words: int
    status: int
    trx: tuple[str, PackedTransaction] | PackedTransaction


class SignedBlock(Struct, frozen=True):
    action_mroot: str
    block_extensions: list
    confirmed: int
    header_extensions: list
    new_producers: list | None
    previous: str
    producer: str
    producer_signature: str
    schedule_version: int
    timestamp: str
    transaction_mroot: str
    transactions: list[Transaction]


class AccountRamDelta(Struct, frozen=True):
    account: str
    delta: int


class PermissionLevel(Struct, frozen=True):
    actor: str
    permission: str


class Act(Struct, frozen=True):
    account: str
    authorization: list[PermissionLevel]
    data: dict | str
    name: str


class AuthSequence(Struct, frozen=True):
    account: str
    sequence: int


class ActionReceipt(Struct, frozen=True):
    abi_sequence: int
    act_digest: str
    auth_sequence: list[AuthSequence]
    code_sequence: int
    global_sequence: int
    receiver: str
    recv_sequence: int


ActionReceiptGeneric = tuple[str, ActionReceipt] | ActionReceipt


class ActionTraceV0(
    Struct,
    frozen=True,
    tag='action_trace_v0'
):
    account_ram_deltas: list[AccountRamDelta]
    act: Act
    action_ordinal: int
    console: str
    context_free: bool
    creator_action_ordinal: int
    elapsed: int
    receipt: ActionReceiptGeneric
    receiver: str
    error_code: int | None = None
    except_: str | None = msgspec.field(name='except', default=None)


class ActionTraceV1(
    Struct,
    frozen=True,
    tag='action_trace_v1'
):
    '''
    Same as v0 + return_value
    '''
    account_ram_deltas: list[AccountRamDelta]
    act: Act
    action_ordinal: int
    console: str
    context_free: bool
    creator_action_ordinal: int
    elapsed: int
    receipt: ActionReceiptGeneric
    receiver: str
    return_value: bytes
    error_code: int | None = None
    except_: str | None = msgspec.field(name='except', default=None)


ActionTrace = ActionTraceV0 | ActionTraceV1


class PartialTransactionV0(
    Struct,
    frozen=True,
    tag='partial_transaction_v0'
):
    context_free_data: list
    delay_sec: int
    expiration: str
    max_cpu_usage_ms: int
    max_net_usage_words: int
    ref_block_num: int
    ref_block_prefix: int
    signatures: list[str]
    transaction_extensions: list[Any]


PartialTransaction = PartialTransactionV0


class TransactionTraceV0(
    Struct,
    frozen=True,
    tag='transaction_trace_v0'
):
    action_traces: list[ActionTrace]
    cpu_usage_us: int
    elapsed: int
    id: str
    net_usage: int
    net_usage_words: int
    partial: PartialTransaction
    scheduled: bool
    status: int
    account_ram_delta: AccountRamDelta | None = None
    error_code: Any = None
    except_: Any = None
    failed_dtrx_trace: Any = None



TraceGeneric = TransactionTraceV0 | ActionTrace


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


class AccountV0(
    Struct,
    frozen=True,
    tag='account_v0'
):
    name: str
    abi: bytes
    creation_date: str

    @classmethod
    def from_b64(self, raw: str) -> AccountV0:
        row = antelope_rs.abi_unpack(
            'std',
            'account',
            b64decode(raw)
        )
        return msgspec.convert(row, type=AccountV0)


class RawContractRowV0(
    Struct,
    frozen=True,
    tag='contract_row_v0'
):
    code: str
    scope: str
    table: str
    primary_key: int
    payer: str
    value: bytes

    def as_dict(self):
        return to_builtins(self)

    @classmethod
    def from_b64(self, raw: str) -> RawContractRowV0:
        row = antelope_rs.abi_unpack(
            'std',
            'contract_row',
            b64decode(raw)
        )
        return msgspec.convert(row, type=RawContractRowV0)

    def decode(self) -> ContractRowV0:
        return ContractRowV0(
            code=self.code,
            scope=self.scope,
            table=self.table,
            primary_key=self.primary_key,
            payer=self.payer,
            value=antelope_rs.abi_unpack(
                self.code,
                self.table,
                self.value
            )
        )

    def whitelist_keys(self) -> tuple[str, str]:
        return self.code, self.table


class AccountMetadataV0(
    Struct,
    frozen=True,
    tag='account_metadata_v0'
):
    code: str | None
    last_code_update: str
    name: str
    privileged: bool


class ContractTableV0(
    Struct,
    frozen=True,
    tag='contract_table_v0'
):
    code: str
    payer: str
    scope: str
    table: str


class ContractRowV0(
    Struct,
    frozen=True,
    tag='contract_row_v0'
):
    code: str
    scope: str
    table: str
    primary_key: int
    payer: str
    value: dict | bytes


class KeyWeight(Struct, frozen=True):
    key: str
    weight: int


class PermissionLevelWeight(Struct, frozen=True):
    permission: PermissionLevel
    weight: int


class WaitWeight(Struct, frozen=True):
    wait_sec: int
    weight: int


class Authority(Struct, frozen=True):
    threshold: int
    keys: list[KeyWeight]
    accounts: list[PermissionLevelWeight]
    waits: list[WaitWeight]


class PermissionV0(
    Struct,
    frozen=True,
    tag='permission_v0'
):
    auth: Authority
    last_updated: str
    name: str
    owner: str
    parent: str


class ResourceLimitsV0(
    Struct,
    frozen=True,
    tag='resource_limits_v0'
):
    cpu_weight: int
    net_weight: int
    owner: str
    ram_bytes: int


class UsageAccumulatorV0(
    Struct,
    frozen=True,
    tag='usage_accumulator_v0'
):
    consumed: int
    last_ordinal: int
    value_ex: int


class ResourceUsageV0(
    Struct,
    frozen=True,
    tag='resource_usage_v0'
):
    cpu_usage: UsageAccumulatorV0
    net_usage: UsageAccumulatorV0
    ram_usage: int
    owner: str


class ResourceLimitsStateV0(
    Struct,
    frozen=True,
    tag='resource_limits_state_v0'
):
    average_block_cpu_usage: UsageAccumulatorV0
    average_block_net_usage: UsageAccumulatorV0
    total_cpu_weight: int
    total_net_weight: int
    total_ram_bytes: int
    virtual_cpu_limit: int
    virtual_net_limit: int


PosibleDeltas = (
    AccountV0 |
    AccountMetadataV0 |
    PermissionV0 |
    ContractRowV0 |
    ContractTableV0 |
    ResourceUsageV0 |
    ResourceLimitsV0 |
    ResourceLimitsStateV0
)


class TableRow(Struct, frozen=True):
    data: PosibleDeltas | bytes
    present: bool


class TableDeltaV0(
    Struct,
    frozen=True,
    tag='table_delta_v0'
):
    name: str
    rows: list[TableRow]


TableDelta = TableDeltaV0


class DeltaDictionary(Struct, frozen=True):
    account: list[TableRow] = []
    account_metadata: list[TableRow] = []
    permission: list[TableRow] = []
    contract_row: list[TableRow] = []
    contract_table: list[TableRow] = []
    resource_usage: list[TableRow] = []
    resource_limits: list[TableRow] = []
    resource_limits_state: list[TableRow] = []


DeltasGeneric = list[TableDelta] | DeltaDictionary


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


class Block(BlockHeader, frozen=True):
    ws_index: int
    block: SignedBlock | None = None
    traces: list[TraceGeneric] | None = None
    deltas: DeltasGeneric | None = None
