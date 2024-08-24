#!/usr/bin/env python3

import os

from typing import OrderedDict
from functools import partial

import msgspec

from .ds import STANDARD_TYPES, DataStream


class ABIType(msgspec.Struct):
    new_type_name: str
    type: str


class ABIStructField(msgspec.Struct):
    type: str
    name: str


class ABIStruct(msgspec.Struct):
    name: str
    fields: list[ABIStructField]
    base: str | None = None


class ABIAction(msgspec.Struct):
    name: str
    type: str
    ricardian_contract: str = ''


class ABITable(msgspec.Struct):
    name: str
    type: str
    key_names: list[str]


class ABIClause(msgspec.Struct):
    id: str
    body: str


class ABIVariant(msgspec.Struct):
    name: str
    types: list[str]


class ABI(msgspec.Struct):
    version: str
    types: list[ABIType] = []
    structs: list[ABIStruct] = []
    actions: list[ABIAction] = []
    tables: list[ABITable] = []
    ricardian_clauses: list[ABIClause] = []
    error_messages: list[str] = []
    abi_extensions: list[str] = []
    variants: list[ABIVariant] = []


package_dir = os.path.dirname(__file__)

std_abi_file_path = os.path.join(package_dir, 'abis/std_abi.json')
eosio_abi_file_path = os.path.join(package_dir, 'abis/eosio.json')
eosio_token_abi_file_path = os.path.join(package_dir, 'abis/eosio.token.json')


def load_std_abi() -> ABI:
    with open(std_abi_file_path, 'r') as file:
        return msgspec.json.decode(file.read(), type=ABI)


def load_std_contracts() -> dict[str, ABI]:
    contracts = {}

    with open(eosio_abi_file_path, 'r') as file:
        contracts['eosio'] = msgspec.json.decode(file.read(), type=ABI)

    with open(eosio_token_abi_file_path, 'r') as file:
        contracts['eosio.token'] = msgspec.json.decode(file.read(), type=ABI)

    return contracts


STD_ABI = load_std_abi()

STD_CONTRACT_ABIS = load_std_contracts()


def has_array_type_mod(type_name: str) -> bool:
    return type_name.endswith('[]')

def has_optional_type_mod(type_name: str) -> bool:
    return type_name.endswith('?')

def has_extension_type_mod(type_name: str) -> bool:
    return type_name.endswith('$')

def strip_type_mods(type_name: str) -> str:
    return ''.join((
        c for c in type_name if c not in ['[', ']', '?', '$']))

def has_type_modifiers(type_name: str) -> bool:
    return (has_array_type_mod(type_name) and
            has_optional_type_mod(type_name) and
            has_extension_type_mod(type_name))


class ABITypeDescriptor:

    def __init__(
        self,
        type_name: str,
        strip_name: str,
        struct: ABIStruct | None,
        variant: ABIVariant | None
    ):
        self.type_name = type_name
        self.strip_name = strip_name
        self.struct = struct
        self.variant = variant

        self.is_array = has_array_type_mod(type_name)
        self.is_optional = has_optional_type_mod(type_name)
        self.is_extension = has_extension_type_mod(type_name)


class ABIDataStream(DataStream):

    def __init__(self, *args, abi: ABI = STD_ABI):
        super().__init__(*args)
        self.abi = abi

    def resolve_alias(self, name: str) -> str:
        return next(
            (a.type for a in self.abi.types if a.new_type_name == name),
            name
        )

    def resolve_struct(self, name: str) -> ABIStruct:
        '''Find struct and recursivly find base fields
        '''
        struct = next(
            (s for s in self.abi.structs if s.name == name),
            None
        )
        if not struct:
            raise ValueError(f'struct {name} not found in ABI')

        if isinstance(struct.base, str):
            if len(struct.base) > 0:
                base_struct = self.resolve_struct(struct.base)
                struct.fields = base_struct.fields + struct.fields
            struct.base = None

        return struct

    def resolve_variant(self, name: str) -> ABIVariant:
        variant = next(
            (v for v in self.abi.variants if v.name == name),
            None
        )
        if not variant:
            raise ValueError(f'variant {name} not found in ABI')

        return variant

    def resolve_table(self, name: str) -> ABITable:
        table = next(
            (t for t in self.abi.tables if t.name == name),
            None
        )
        if not table:
            raise ValueError(f'table {name} not found in ABI')

        return table

    def resolve_type(self, name: str) -> ABITypeDescriptor:
        sname = strip_type_mods(name)
        sname = self.resolve_alias(sname)
        struct = None
        variant = None

        if sname not in STANDARD_TYPES:
            try:
                variant = self.resolve_variant(sname)

            except ValueError:
                try:
                    struct = self.resolve_struct(sname)

                except ValueError:
                    table = self.resolve_table(sname)
                    struct = self.resolve_struct(table.type)

        return ABITypeDescriptor(name, sname, struct, variant)

    def pack_struct(self, struct_def: ABIStruct, v):
        for field in struct_def.fields:
            fname, ftype = (field.name, field.type)
            if fname in v:
                self.pack_type(ftype, v[fname])

    def unpack_struct(self, struct_def: ABIStruct) -> OrderedDict:
        res = OrderedDict()
        for field in struct_def.fields:
            fname, ftype = (field.name, field.type)
            try:
                res[fname] = self.unpack_type(ftype)

            except Exception as e:
                e.add_note(f'trying to unpack struct {struct_def.name}')
                for key, val in res.items():
                    e.add_note(f'\t\"{key}\": {val}')

                e.add_note(f'\t\"{fname}\":  <- error during unpack of this field')
                raise e

        return res

    def pack_type(self, type_name: str, v):
        desc = self.resolve_type(type_name)

        if desc.is_array:
            assert isinstance(v, list)
            amount = len(v)
            self.pack_varuint32(amount)
            for i in v:
                self.pack_type(desc.strip_name, i)
            return

        if desc.is_optional:
            self.pack_uint8(0 if v is None else 1)

        if desc.variant:
            assert isinstance(v, tuple)
            assert len(v) == 2
            sub_name, v = v
            self.pack_uint8(desc.variant.types.index(sub_name))
            desc.struct = self.resolve_struct(sub_name)

        # find right packing function
        pack_partial = None
        if desc.struct:
            pack_partial = partial(
                self.pack_struct, desc.struct)

        else:
            pack_fn = getattr(self, f'pack_{desc.strip_name}')
            pack_partial = partial(pack_fn)

        try:
            pack_partial(v)

        except Exception as e:
            e.add_note(f'trying to pack {type_name}')
            raise e


    def unpack_type(self, type_name: str):
        desc = self.resolve_type(type_name)

        if desc.is_array:
            amount = self.unpack_varuint32()
            return [
                self.unpack_type(desc.strip_name)
                for _ in range(amount)
            ]

        if desc.is_optional:
            if self.unpack_uint8() == 0:
                return None

        if desc.is_extension and self.remaining == 0:
            return None

        if desc.variant:
            i = self.unpack_uint8()
            desc.struct = self.resolve_struct(desc.variant.types[i])

        # find right unpacking function
        unpack_partial = None
        if desc.struct:
            unpack_partial = partial(
                self.unpack_struct, desc.struct)

        else:
            unpack_fn = getattr(self, f'unpack_{desc.strip_name}')
            unpack_partial = partial(unpack_fn)

        try:
            return unpack_partial()

        except Exception as e:
            e.add_note(f'trying to unpack {type_name}')
            raise e


def abi_pack(type_name: str, value, abi: ABI = STD_ABI) -> bytes:
    ds = ABIDataStream(abi=abi)
    ds.pack_type(type_name, value)
    return ds.getvalue()


def abi_unpack(
    type_name: str,
    raw: bytes,
    abi: ABI = STD_ABI,
    spec: msgspec.Struct | None = None
) -> OrderedDict:
    ds = ABIDataStream(raw, abi=abi)
    result = ds.unpack_type(type_name)
    if spec:
        result = msgspec.convert(result, type=spec)

    return result
