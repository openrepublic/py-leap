# Most of this module was taken from:
# https://github.com/EOSArgentina/ueosio
# Much apreciated!!

import io
import os
import struct
import hashlib
import inspect
import binascii
import calendar

from copy import deepcopy
from base58 import b58decode, b58encode
from datetime import datetime
from functools import partial
from collections import OrderedDict

import msgspec

from leap.protocol.abi import ABI, ABIStruct, ABIVariant
from leap.protocol.types import Asset

from ..errors import SerializationException


package_dir = os.path.dirname(__file__)
abi_file_path = os.path.join(package_dir, 'std_abi.json')

def load_std_abi() -> ABI:
    with open(abi_file_path, 'r') as file:
        return msgspec.json.decode(file.read(), type=ABI)

STD_ABI = load_std_abi()


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
        raise Exception("not implementd")

    def unpack_int128(self):
        raise Exception("not implementd")

    def pack_uint128(self, v):
        raise Exception("not implementd")

    def unpack_uint128(self):
        raise Exception("not implementd")

    def pack_varint32(self, v):
        raise Exception("not implementd")

    def unpack_varint32(self):
        raise Exception("not implementd")

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
        raise Exception("not implementd")

    def unpack_float32(self):
        raise Exception("not implementd")

    def pack_float64(self, v):
        raise Exception("not implementd")

    def unpack_float64(self):
        raise Exception("not implementd")

    def pack_float128(self, v):
        raise Exception("not implementd")

    def unpack_float128(self):
        raise Exception("not implementd")

    def pack_time_point(self, v):
        raise Exception("not implementd")

    def unpack_time_point(self):
        raise Exception("not implementd")

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
        raise Exception("not implementd")

    def unpack_checksum512(self):
        raise Exception("not implementd")

    def pack_public_key(self, v):
        if v.startswith("EOS"):
            data = b58decode(str(v[3:]))
            if len(data) != 33 + 4: raise Exception("invalid k1 key")
            if ripmed160(data[:-4])[:4] != data[-4:]: raise Exception("checksum failed")
            self.pack_uint8(0)
            self.write(data[:-4])
        elif v.startswith("PUB_R1_"):
            raise Exception("not implementd")
        else:
            raise Exception("invalid pubkey format")

    def unpack_public_key(self):
        t = self.unpack_uint8()
        if t == 0:
            data = self.read(33)
            data = data + ripmed160(data)[:4]
            return "EOS" + b58encode(data).decode('ascii')
        elif t == 1:
            raise Exception("not implementd")
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
            raise Exception("not implementd")
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
    name[5:] for name, fn in inspect.getmembers(DataStream, inspect.isfunction)
]


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


class TypeDescriptor:

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

    def resolve_type(self, name: str) -> TypeDescriptor:
        sname = strip_type_mods(name)
        sname = self.resolve_alias(sname)
        struct = None
        variant = None

        if sname not in STANDARD_TYPES:
            try:
                variant = self.resolve_variant(sname)

            except ValueError:
                struct = self.resolve_struct(sname)

        return TypeDescriptor(name, sname, struct, variant)

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


import struct
import hashlib
import binascii

from datetime import datetime, timedelta
from cryptos import hash_to_int, encode_privkey, decode, encode, \
    hmac, fast_multiply, G, inv, N, decode_privkey, get_privkey_format, random_key, encode_pubkey, privtopub

from base58 import b58decode, b58encode


DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def is_canonical(r: int, s: int) -> bool:
    r_bytes = r.to_bytes(32, byteorder='big')
    s_bytes = s.to_bytes(32, byteorder='big')

    return not (r_bytes[0] & 0x80) \
           and not (r_bytes[0] == 0 and not (r_bytes[1] & 0x80)) \
           and not (s_bytes[0] & 0x80) \
           and not (s_bytes[0] == 0 and not (s_bytes[1] & 0x80))

def endian_reverse_u32(x):
    x = x & 0xFFFFFFFF
    return (((x >> 0x18) & 0xFF)) \
           | (((x >> 0x10) & 0xFF) << 0x08) \
           | (((x >> 0x08) & 0xFF) << 0x10) \
           | (((x) & 0xFF) << 0x18)

def get_tapos_info(block_id) -> tuple[int, int]:
    block_id_bin = bytes.fromhex(block_id)

    hash0 = struct.unpack("<Q", block_id_bin[0:8])[0]
    hash1 = struct.unpack("<Q", block_id_bin[8:16])[0]

    ref_block_num = endian_reverse_u32(hash0) & 0xFFFF
    ref_block_prefix = hash1 & 0xFFFFFFFF

    return ref_block_num, ref_block_prefix

def deterministic_generate_k_nonce(msghash, priv, nonce):
    v = b'\x01' * 32
    k = b'\x00' * 32
    priv = encode_privkey(priv, 'bin')
    msghash = encode(hash_to_int(msghash) + nonce, 256, 32)
    k = hmac.new(k, v + b'\x00' + priv + msghash, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    k = hmac.new(k, v + b'\x01' + priv + msghash, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    return decode(hmac.new(k, v, hashlib.sha256).digest(), 256)

def ecdsa_raw_sign_nonce(msghash, priv, nonce):
    z = hash_to_int(msghash)
    k = deterministic_generate_k_nonce(msghash, priv, nonce)

    r, y = fast_multiply(G, k)
    s = inv(k, N) * (z + r * decode_privkey(priv)) % N

    v, r, s = 27 + ((y % 2) ^ (0 if s * 2 < N else 1)), r, s if s * 2 < N else N - s
    if 'compressed' in get_privkey_format(priv):
        v += 4
    return v, r, s


class CannonicalSignatureError(BaseException):
    ...


def sign_hash(h, pk, max_retries=100):
    nonce = 0
    while nonce < max_retries:
        v, r, s = ecdsa_raw_sign_nonce(h, pk, nonce)
        if is_canonical(r, s):
            signature = '00%02x%064x%064x' % (v, r, s)
            break
        nonce += 1

    else:
        raise CannonicalSignatureError
    sig = DataStream(bytes.fromhex(signature)).unpack_signature()
    return sig

def sign_bytes(ds, pk):
    m = hashlib.sha256()
    m.update(ds.getvalue())
    return sign_hash(m.digest(), pk)

def sign_tx(chain_id, tx, pk):
    zeros = '0000000000000000000000000000000000000000000000000000000000000000'

    ds = DataStream()
    ds.pack_checksum256(chain_id)

    dsp = ABIDataStream()
    dsp.pack_type('transaction', tx)
    packed_trx = dsp.getvalue()

    ds.write(packed_trx)
    ds.pack_checksum256(zeros)

    sig = sign_bytes(ds, pk)
    if 'signatures' not in tx:
        tx['signatures'] = []
    tx['signatures'].append(sig)

    m = hashlib.sha256()
    m.update(packed_trx)
    tx_id = m.digest().hex()

    return tx_id, tx

def ripmed160(data):
    h = hashlib.new('ripemd160')
    h.update(data)
    return h.digest()

def gen_key_pair():
    pk   = random_key()
    wif  = encode_privkey(pk, 'wif')
    data = encode_pubkey(privtopub(pk),'bin_compressed')
    data = data + ripmed160(data)[:4]
    pubkey = "EOS" + b58encode(data).decode('utf8')
    return wif, pubkey

def get_pub_key(pk):
    data = encode_pubkey(privtopub(pk),'bin_compressed')
    data = data + ripmed160(data)[:4]
    pubkey = "EOS" + b58encode(data).decode('utf8')
    return pubkey

def build_action(action, auth, data):
    return {
        "account": action['account'],
        "name": action['name'],
        "authorization": [{
            "actor": auth['actor'],
            "permission": auth['permission']
        }],
        "data": binascii.hexlify(data).decode('utf-8')
    }

def build_transaction(expiration, ref_block_num, ref_block_prefix, actions):
    if type(actions) == dict: actions = [actions]
    return {
        "expiration": expiration,
        "ref_block_num": ref_block_num,
        "ref_block_prefix": ref_block_prefix,
        "max_net_usage_words": 0,
        "max_cpu_usage_ms": 0,
        "delay_sec": 0,
        "context_free_actions": [],
        "actions": actions,
        "transaction_extensions": [],
        "signatures": [],
        "context_free_data": []
    }

def build_push_transaction_body(signature, packed_trx):
    return {
        "signatures": [
            signature
        ],
        "compression": False,
        "packed_context_free_data": "",
        "packed_trx": packed_trx
    }

def get_expiration(now, delta=0):
    return (now + timedelta(seconds=delta)).strftime(DATE_TIME_FORMAT)

def create_tx(chain_id, block_id, expiration, privkey, fn_action_builder, params):
    ref_block_num, ref_block_prefix = get_tapos_info(block_id)

    u_tx = build_transaction(expiration, ref_block_num, ref_block_prefix, fn_action_builder(*params) )

    tx_id, signed_tx = sign_tx(chain_id, u_tx, privkey)
    return tx_id, signed_tx

# given an abi and action data, return serialized data in hexstr format
def pack_abi_data(abi: dict, action: dict) -> bytes:
    ds = ABIDataStream(abi=msgspec.convert(abi, type=ABI))
    account = action['account']
    name = action['name']
    data = action['data']

    try:
        struct = ds.resolve_struct(name)
        params = OrderedDict()
        for f in struct.fields:
            if len(data) == 0:
                break
            params[f.name] = data.pop(0)

        ds.pack_type(name, params)

        return ds.getvalue()

    except Exception as e:
        e.add_note(f'while trying to pack action data for {account}::{name}')
        breakpoint()
        raise e


def create_and_sign_tx(
    chain_id: str,
    abis: dict[str, dict],
    actions: list[dict],
    key: str,
    max_cpu_usage_ms=255,
    max_net_usage_words=0,
    ref_block_num: int = 0,
    ref_block_prefix: int = 0
) -> dict:

    tx = {
        'delay_sec': 0,
        'max_cpu_usage_ms': max_cpu_usage_ms,
        'actions': deepcopy(actions)
    }

    # package transation
    for i, action in enumerate(tx['actions']):
        account = action['account']
        abi = abis.get(account, None)
        if abi is None:
            SerializationException(f'don\'t have abi for {account}')

        tx['actions'][i]['data'] = pack_abi_data(abi, action)

    tx.update({
        'expiration': get_expiration(
            datetime.utcnow(), timedelta(minutes=15).total_seconds()),
        'ref_block_num': ref_block_num,
        'ref_block_prefix': ref_block_prefix,
        'max_net_usage_words': max_net_usage_words,
        'max_cpu_usage_ms': max_cpu_usage_ms,
        'delay_sec': 0,
        'context_free_actions': [],
        'transaction_extensions': [],
        'context_free_data': [],
    })

    # sign transaction
    _, signed_tx = sign_tx(chain_id, tx, key)

    # pack
    ds = ABIDataStream()
    ds.pack_type('transaction', signed_tx)
    packed_trx = binascii.hexlify(ds.getvalue()).decode('utf-8')
    return build_push_transaction_body(signed_tx['signatures'][0], packed_trx)
