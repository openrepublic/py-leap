from __future__ import annotations
from dataclasses import dataclass

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import TypeVar, Generic
from binascii import hexlify

from msgspec import Struct


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

    def __post_init__(self):
        if isinstance(self.code, str):
            self.code = SymbolCode.from_str(self.code)

    @property
    def unit(self) -> float:
        return 1 / (10 ** self.precision)

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

    def __post_init__(self):
        if isinstance(self.symbol, str):
            self.symbol = Symbol.from_str(self.symbol)

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


# name helpers

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
class Name(ABCLeapType):

    _str: str

    def __str__(self) -> str:
        return self._str

    def __int__(self) -> int:
        s = self._str
        i = 0
        int_name = 0
        while i < len(s):
            int_name += (char_to_symbol(s[i]) & 0x1F) << (64-5 * (i + 1))
            i += 1
        if i > 12:
            int_name |= char_to_symbol(s[11]) & 0x0F

        return int_name

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

        return Name(str_name)

    @staticmethod
    def _from_str(s: str) -> Name:
        return Name(s)


T = TypeVar('T')
class GetTableRowsResponse(Struct, Generic[T]):
    rows: list[T]
    more: bool
    ram_payer: list[str] | None = None
    next_key: str | None = None
