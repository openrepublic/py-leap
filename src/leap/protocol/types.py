#!/usr/bin/env python3

from __future__ import annotations
from dataclasses import dataclass

import re

from abc import ABC, abstractmethod
from typing import Any, List
from decimal import Decimal
from binascii import hexlify


class ABCLeapType(ABC):

    @abstractmethod
    def __str__(self) -> str:
        pass

    @staticmethod
    def _from_str(str_obj: str):
        raise BaseException(f'Called abc impl of _from_str! instance of: {cls.__name__}')

    @staticmethod
    def _from_int(int_obj: int):
        raise BaseException(f'Called abc impl of _from_int! instance of: {cls.__name__}')

    @classmethod
    def from_str(cls, obj):
        if isinstance(obj, cls):
            return obj

        elif isinstance(obj, str):
            return cls._from_str(obj)

        else:
            raise TypeError(
                f'Input must be an instance of {cls.__name__}'
                f' or a string representation of {cls.__name__}'
            )

    @classmethod
    def from_int(cls, obj):
        if isinstance(obj, cls):
            return obj

        elif isinstance(obj, int):
            return cls._from_int(obj)

        else:
            raise TypeError(
                f'Input must be an instance of {cls.__name__}'
                f' or a integer representation of {cls.__name__}'
            )


def string_to_sym_code(sym):
    ret = 0
    for i, char in enumerate(sym):
        if char >= 'A' or char <= 'Z':
            code = ord(char)
            ret |= code << 8 * i

    return ret

def sym_code_to_string(sym_code):
    ret = ''
    while sym_code != 0:
        char_code = sym_code & 0xFF  # Extract the least significant 8 bits
        ret += chr(char_code)  # Convert ASCII to character and append to string
        sym_code >>= 8  # Shift right by 8 bits to process the next character

    return ret[::-1]  # Reverse the string as the characters were processed in reverse order

@dataclass
class SymbolCode(ABCLeapType):

    _str: str
    value: int

    def __str__(self) -> str:
        return self._str

    @staticmethod
    def _from_str(sym: str):
        return SymbolCode(sym, string_to_sym_code(sym))

    @staticmethod
    def _from_int(sym: int):
        return SymbolCode(sym_code_to_string(sym), sym)


@dataclass
class Symbol(ABCLeapType):

    code: SymbolCode
    precision: int

    @property
    def unit(self) -> float:
        return 1 / (10 ** self.precision)

    def __eq__(self, other) -> bool:
        return (
            self.code == other.code and
            self.precision == other.precision
        )

    def __str__(self) -> str:
        return f'{self.precision},{self.code}'

    @staticmethod
    def _from_str(str_sym: str):
        prec, code = str_sym.split(',')
        return Symbol(SymbolCode.from_str(code), int(prec))


@dataclass
class Asset(ABCLeapType):

    amount: int
    symbol: Symbol

    def __eq__(self, other) -> bool:
        return (
            self.amount == other.amount and
            self.symbol == other.symbol
        )

    def __str__(self) -> str:
        str_amount = str(self.amount).zfill(self.symbol.precision + 1)
        if self.symbol.precision:
            return f'{str_amount[:-self.symbol.precision]}.{str_amount[-self.symbol.precision:]} {self.symbol.code}'
        return f'{str_amount} {self.symbol.code}'

    def to_decimal(self) -> Decimal:
        str_amount = str(self.amount).zfill(self.symbol.precision + 1)
        if self.symbol.precision:
            str_amount = f'{str_amount[:-self.symbol.precision]}.{str_amount[-self.symbol.precision:]}'

        return Decimal(str_amount)

    @staticmethod
    def _from_str(str_asset: str):
        numeric, sym = str_asset.split(' ')
        if '.' not in numeric:
            precision = 0
        else:
            precision = len(numeric) - numeric.index('.') - 1
            numeric = numeric.replace('.', '')

        return Asset(int(numeric), Symbol(sym, precision))

    @staticmethod
    def from_decimal(dec: Decimal, precision: int, sym: str):
        result = str(dec)
        pindex = result.index('.')
        return Asset._from_str(f'{result[:pindex + 1 + precision]} {sym}')

    @staticmethod
    def from_ints(amount: int, precision: int, sym: str):
        result = str(amount)
        if precision > 0:
            return f'{result[:-precision]}.{result[-precision:]} {sym}'
        return Asset._from_str(f'{result} {sym}')


class Name(ABCLeapType):

    _str: str
    _value: int

    def __init__(self, _str: str, _value: int):
        assert len(_str) <= 13
        assert not bool(re.compile(r'[^a-z0-9.]').search(_str))

        self._str = _str
        self._value = _value

    def __str__(self) -> str:
        return self._str

    def __int__(self) -> int:
        return self._value

    @staticmethod
    def str_to_int_name(s: str) -> int:
        """Convert name to its number repr
        """
        i = 0
        name = 0
        while i < len(s) :
            name += (char_to_symbol(s[i]) & 0x1F) << (64-5 * (i + 1))
            i += 1

        if i > 12 :
            name |= char_to_symbol(s[11]) & 0x0F

        return name

    @staticmethod
    def _from_int(n: int) -> Name:
        charmap = '.12345abcdefghijklmnopqrstuvwxyz'
        name = ['.'] * 13
        i = 0
        while i <= 12:
            c = charmap[n & (0x0F if i == 0 else 0x1F)]
            name[12-i] = c
            n >>= 4 if i == 0 else 5
            i += 1

        str_name = ''.join(name).rstrip('.')

        return Name(str_name, n)

    @staticmethod
    def _from_str(s: str) -> Name:
        i = 0
        int_name = 0
        while i < len(s):
            int_name += (char_to_symbol(s[i]) & 0x1F) << (64-5 * (i + 1))
            i += 1
        if i > 12:
            int_name |= char_to_symbol(s[11]) & 0x0F
        return Name(s, int_name)


def str_to_hex(c):
    hex_data = hexlify(bytearray(c, 'ascii')).decode()
    return int(hex_data, 16)


def char_subtraction(a, b, add):
    x = str_to_hex(a)
    y = str_to_hex(b)
    ans = str((x - y) + add)
    if len(ans) % 2 == 1:
        ans = '0' + ans
    return int(ans)


def char_to_symbol(c):
    ''' '''
    if c >= 'a' and c <= 'z':
        return char_subtraction(c, 'a', 6)
    if c >= '1' and c <= '5':
        return char_subtraction(c, '1', 1)
    return 0


@dataclass
class LeapOptional:
    value: Any
    type: str


@dataclass
class UInt8:
    num: int


@dataclass
class UInt16:
    num: int


@dataclass
class UInt32:
    num: int


@dataclass
class UInt64:
    num: int


@dataclass
class VarUInt32:
    num: int


@dataclass
class Int8:
    num: int


@dataclass
class Int16:
    num: int


@dataclass
class Int32:
    num: int


@dataclass
class Int64:
    num: int


@dataclass
class VarInt32:
    num: int


@dataclass
class Checksum160:
    hash: str

    def __str__(self) -> str:
        return self.hash


@dataclass
class Checksum256:
    hash: str

    def __str__(self) -> str:
        return self.hash


@dataclass
class ListArgument:
    list: List
    type: str


@dataclass
class PermissionLevel:
    actor: str
    permission: str

    def get_dict(self) -> dict:
        return {
            'actor': self.actor,
            'permission': self.permission
        }


@dataclass
class PermissionLevelWeight:
    permission: PermissionLevel
    weight: int

    def get_dict(self) -> dict:
        return {
            'permission': self.permission.get_dict(),
            'weight': self.weight
        }


@dataclass
class PublicKey:
    key: str

    def get(self) -> str:
        return self.key


@dataclass
class KeyWeight:
    key: PublicKey
    weight: int

    def get_dict(self) -> dict:
        return {
            'key': self.key.get(),
            'weight': self.weight
        }


@dataclass
class WaitWeight:
    wait: int
    weight: int

    def get_dict(self) -> dict:
        return {
            'wait_sec': self.wait,
            'weight': self.weight
        }


@dataclass
class Authority:
    threshold: int
    keys: List[KeyWeight]
    accounts: List[PermissionLevelWeight]
    waits: List[WaitWeight]

    def get_dict(self) -> dict:
        return {
            'threshold': self.threshold,
            'keys': [k.get_dict() for k in self.keys],
            'accounts': [a.get_dict() for a in self.accounts],
            'waits': [w.get_dict() for w in self.waits]
        }


@dataclass
class Abi:
    abi: dict

    def get_dict(self) -> dict:
        return self.abi
