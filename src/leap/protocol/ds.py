#!/usr/bin/env python3

import io
import struct
import inspect
import binascii
import calendar
import hashlib

from base58 import b58decode, b58encode
from datetime import datetime
from collections import OrderedDict

from .types import Asset


DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def ripmed160(data):
    h = hashlib.new('ripemd160')
    h.update(data)
    return h.digest()


def char_to_symbol(c):
    if c >= ord('a') and c <= ord('z'):
        return (c - ord('a')) + 6
    if c >= ord('1') and c <= ord('5'):
        return (c - ord('1')) + 1
    return 0


def string_to_name(s):
    if len(s) > 13: raise Exception("invalid string length")
    name = 0
    i = 0
    while i < min(len(s), 12):
        name |= (char_to_symbol(ord(s[i])) & 0x1f) << (64 - 5 * (i + 1))
        i += 1
    if len(s) == 13:
        name |= char_to_symbol(ord(s[12])) & 0x0F
    return name


def name_to_string(n, strip_dots=True):
    charmap = ".12345abcdefghijklmnopqrstuvwxyz"
    s = bytearray(13 * b'.')
    tmp = n
    for i in range(13):
        c = charmap[tmp & (0x0f if i == 0 else 0x1f)]
        s[12 - i] = ord(c)
        tmp >>= (4 if i == 0 else 5)

    s = s.decode('utf8')
    if strip_dots:
        s = s.strip('.')
    return s


def string_to_symbol(precision, s):
    l = len(s)
    if l > 7: raise Exception("invalid symbol {0}".format(s))
    result = 0
    for i in range(l):
        if ord(s[i]) < ord('A') or ord(s[i]) > ord('Z'):
            raise Exception("invalid symbol {0}".format(s))
        else:
            result |= (int(ord(s[i])) << (8 * (1 + i)))

    result |= int(precision)
    return result

def symbol_code_to_uint64(sc):
    l = len(sc)
    if l > 7: raise Exception("invalid symbol code {0}".format(sc))
    result = 0
    for i in range(l):
        if ord(sc[i]) < ord('A') or ord(sc[i]) > ord('Z'):
            raise Exception("invalid symbol code {0}".format(sc))
        else:
            result |= (int(ord(sc[i])) << (8 * (i)))
    return result

def uint64_to_symbol_code(n):
    mask = 0xFF
    v = n
    res = ''
    for i in range(7):
        if v == 0: break
        res += chr(v & mask)
        v >>= 8
    return res

def symbol_to_string(symbol):
    return symbol

def asset_to_uint64_pair(a):
    parts = a.split()
    if len(parts) != 2: raise Exception("invalid asset {0}".format(a))

    nums = parts[0].split(".")
    num = ''.join(nums)
    return int(num), string_to_symbol(len(nums[1]) if len(nums) > 1 else 0, parts[1])


class DataStream():
    '''Most of this class was taken from:

    https://github.com/EOSArgentina/ueosio

    Much apreciated!!
    '''

    def __init__(self, stream=None):
        self.remaining = 0
        if not stream:
            stream = io.BytesIO()
        else:
            stream = io.BytesIO(stream)
            self.remaining = len(stream.getvalue())

        self.stream = stream

    def write(self, v):
        self.stream.write(v)
        self.remaining += len(v)

    def read(self, n):
        try:
            raw = self.stream.read(n)
            raw_len = len(raw)

            if raw_len != n:
                raise ValueError(f'Tried to read {n} but only got {raw_len}')

            self.remaining -= n
            return raw

        except Exception as e:
            e.add_note(f'stream read with {self.remaining} remaining')
            raise e

    def getvalue(self) -> bytes:
        return self.stream.getvalue()

    def pack_bool(self, v):
        self.write(b'\x01') if v else self.write(b'\x00')

    def unpack_bool(self):
        return True if self.read(1) else False

    def pack_int8(self, v):
        self.write(struct.pack("<b", v))

    def unpack_int8(self):
        return struct.unpack("<b", self.read(1))[0]

    def pack_uint8(self, v):
        self.write(struct.pack("<B", v))

    def unpack_uint8(self):
        return struct.unpack("<B", self.read(1))[0]

    def pack_int16(self, v):
        self.write(struct.pack("<h", v))

    def unpack_int16(self):
        return struct.unpack("<h", self.read(2))[0]

    def pack_uint16(self, v):
        self.write(struct.pack("<H", v))

    def unpack_uint16(self):
        return struct.unpack("<H", self.read(2))[0]

    def pack_int32(self, v):
        self.write(struct.pack("<i", v))

    def unpack_int32(self):
        return struct.unpack("<i", self.read(4))[0]

    def pack_uint32(self, v):
        self.write(struct.pack("<I", v))

    def unpack_uint32(self):
        return struct.unpack("<I", self.read(4))[0]

    def pack_int64(self, v):
        self.write(struct.pack("<q", v))

    def unpack_int64(self):
        return struct.unpack("<q", self.read(8))[0]

    def pack_uint64(self, v):
        self.write(struct.pack("<Q", v))

    def unpack_uint64(self):
        return struct.unpack("<Q", self.read(8))[0]

    def pack_int128(self, v):
        raise Exception("not implemented")

    def unpack_int128(self):
        raise Exception("not implemented")

    def pack_uint128(self, v):
        raise Exception("not implemented")

    def unpack_uint128(self):
        raise Exception("not implemented")

    def pack_varint32(self, v):
        raise Exception("not implemented")

    def unpack_varint32(self):
        raise Exception("not implemented")

    def pack_varuint32(self, v):
        val = v
        while True:
            b = val & 0x7f
            val >>= 7
            b |= ((val > 0) << 7)
            self.pack_uint8(b)
            if not val:
                break

    def unpack_varuint32(self):
        v = 0;
        b = 0;
        by = 0
        while True:
            b = self.unpack_uint8()
            v |= ((b & 0x7f) << by)
            by += 7
            if not (b & 0x80 and by < 32):
                break
        return v

    def pack_float32(self, v):
        self.write(struct.pack("<f", v))

    def unpack_float32(self):
        return struct.unpack("<f", self.read(4))[0]

    def pack_float64(self, v):
        self.write(struct.pack("<d", v))

    def unpack_float64(self):
        return struct.unpack("<d", self.read(8))[0]

    def pack_float128(self, v):
        raise Exception("not implemented")

    def unpack_float128(self):
        raise Exception("not implemented")

    def pack_time_point(self, v):
        self.pack_uint64(v)

    def unpack_time_point(self) -> int:
        return self.unpack_uint64()

    def pack_time_point_sec(self, v):
        t = int(calendar.timegm(datetime.strptime(v, '%Y-%m-%dT%H:%M:%S').timetuple()))
        self.pack_uint32(t)

    def unpack_time_point_sec(self):
        t = self.unpack_uint32()
        return datetime.fromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%S')

    def pack_block_timestamp_type(self, ms):
        t = (ms - 946684800000) // 500
        self.pack_uint32(t)

    def unpack_block_timestamp_type(self) -> int:
        return (self.unpack_uint32() * 500) + 946684800000

    def pack_account_name(self, v):
        self.pack_uint64(string_to_name(v))

    def unpack_account_name(self):
        return name_to_string(self.unpack_uint64())

    def pack_name(self, v):
        self.pack_account_name(v)

    def unpack_name(self):
        return self.unpack_account_name()

    def pack_bytes(self, v):
        if type(v) == int or type(v) == float:
            v = str(v)
        if type(v) == str: v = v.encode()
        self.pack_varuint32(len(v) if v else 0)
        if v: self.write(v)

    def unpack_bytes(self):
        l = self.unpack_varuint32()
        return self.read(l)

    def pack_string(self, v):
        if type(v) == bytearray:
            v = v.decode("utf-8")
        self.pack_bytes(v)

    def unpack_string(self):
        return self.unpack_bytes().decode('utf8')

    def pack_checksum160(self, v):
        self.pack_rd160(v)

    def unpack_checksum160(self):
        return self.unpack_rd160()

    def pack_checksum256(self, v):
        self.pack_sha256(v)

    def unpack_checksum256(self):
        return self.unpack_sha256()

    def pack_checksum512(self, v):
        raise Exception("not implemented")

    def unpack_checksum512(self):
        raise Exception("not implemented")

    def pack_public_key(self, v):
        if v.startswith("EOS"):
            data = b58decode(str(v[3:]))
            if len(data) != 33 + 4: raise Exception("invalid k1 key")
            if ripmed160(data[:-4])[:4] != data[-4:]: raise Exception("checksum failed")
            self.pack_uint8(0)
            self.write(data[:-4])
        elif v.startswith("PUB_R1_"):
            raise Exception("not implemented")
        else:
            raise Exception("invalid pubkey format")

    def unpack_public_key(self):
        t = self.unpack_uint8()
        if t == 0:
            data = self.read(33)
            data = data + ripmed160(data)[:4]
            return "EOS" + b58encode(data).decode('ascii')
        elif t == 1:
            raise Exception("not implemented")
        else:
            raise Exception("invalid binary pubkey")

    def pack_signature(self, v):

        def pack_signature_sufix(b58sig, sufix):
            data = b58decode(b58sig)
            if len(data) != 65 + 4: raise Exception("invalid {0} signature".format(sufix))
            if ripmed160(data[:-4] + sufix)[:4] != data[-4:]: raise Exception("checksum failed")
            self.pack_uint8(0)
            self.write(data[:-4])

        if v.startswith("SIG_K1_"):
            pack_signature_sufix(str(v[7:]), b"K1")
        elif v.startswith("SIG_R1_"):
            pack_signature_sufix(str(v[7:]), b"R1")
        else:
            raise Exception("invalid signature format")

    def unpack_signature(self):
        t = self.unpack_uint8()
        if t == 0:
            data = self.read(65)
            data = data + ripmed160(data + b"K1")[:4]
            return "SIG_K1_" + b58encode(data).decode("ascii")
        elif t == 1:
            raise Exception("not implemented")
        else:
            raise Exception("invalid binary signature")

    def pack_symbol(self, v):
        segments = v.split(',')
        precision = int(segments[0]) & 0xFF
        symbol_code = segments[1]
        symbol_code_as_uint64 = symbol_code_to_uint64(symbol_code)
        # shift symbol_code_to_the left so it can be bitwise OR-ed
        ret = symbol_code_as_uint64 << 8
        ret |= precision
        return self.pack_uint64(ret)

    def unpack_symbol(self):
        v = self.unpack_uint64()
        precision = v & 0xFF
        v >>= 8
        return f"{precision},{uint64_to_symbol_code(v)}"

    def pack_symbol_code(self, v):
        self.pack_uint64(symbol_code_to_uint64(v))

    def unpack_symbol_code(self):
        return uint64_to_symbol_code(self.unpack_uint64())

    def pack_asset(self, v):
        if isinstance(v, Asset):
            v = str(v)
        a, s = asset_to_uint64_pair(v)
        self.pack_int64(a)
        self.pack_uint64(s)

    def unpack_asset(self):
        amount = self.unpack_int64()
        symbol = self.unpack_symbol()
        return f"{amount},{symbol}"

    def pack_extended_asset(self, v):
        if isinstance(v, str):
            quant, contract =  v.split('@')

        else:
            quant = v['quantity']
            contract = v['contract']

        self.pack_asset(quant)
        self.pack_name(contract)

    def unpack_extended_asset(self):
        return OrderedDict([
            ("quantity", self.unpack_asset()),
            ("contract", self.unpack_name()),
        ])

    def pack_rd160(self, v):
        if isinstance(v, str):
            v = bytes.fromhex(v)
        if len(v) != 20: raise Exception("Invalid rd160 length")
        self.write(v)

    def unpack_rd160(self):
        d = self.read(20)
        return binascii.hexlify(d).decode('utf-8')

    def pack_sha256(self, v):
        if len(v) != 64: raise Exception("Invalid sha256 length")
        self.write(bytes.fromhex(v))

    def unpack_sha256(self):
        d = self.read(32)
        return binascii.hexlify(d).decode('utf-8')


STANDARD_TYPES = [
    name[5:] for name, _ in inspect.getmembers(DataStream, inspect.isfunction)
]
