# Most of this module was taken from:
# https://github.com/EOSArgentina/ueosio
# Much apreciated!!

from functools import partial
import io
import struct
import hashlib
import binascii
import calendar

from copy import deepcopy
from base58 import b58decode, b58encode
from datetime import datetime
from collections import OrderedDict

from leap.protocol.abi import ABI, ABIStruct, ABIVariant

from ..errors import SerializationException


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
            self.remaining = len(stream)
            stream = io.BytesIO(stream)

        self.stream = stream

    def write(self, v):
        self.stream.write(v)
        self.remaining += len(v)

    def read(self, n):
        self.remaining -= n
        return self.stream.read(n)

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

    def pack_block_timestamp_type(self, v):
        raise Exception("not implementd")

    def unpack_block_timestamp_type(self):
        raise Exception("not implementd")

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
    'bool',
    'int8', 'int16', 'int32', 'int64', 'int128',
    'uint8', 'uint16', 'uint32', 'uint64', 'uint128',
    'varint32', 'varuint32',
    'float32', 'float64', 'float128',
    'time_point', 'time_point_sec',
    'name', 'account_name',
    'bytes', 'string',
    'checksum160', 'checksum256', 'checksum512',
    'public_key', 'signature',
    'symbol', 'symbol_code', 'asset', 'extended_asset',
    'rd160', 'sha256'
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
        struct: ABIStruct | None
    ):
        self.type_name = type_name
        self.strip_name = strip_name
        self.is_array = has_array_type_mod(type_name)
        self.is_optional = has_optional_type_mod(type_name)
        self.is_extension = has_extension_type_mod(type_name)
        self.struct = struct


class ABIDataStream(DataStream):

    def __init__(self, abi: ABI, *args):
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
            base_struct = self.resolve_struct(struct.base)
            struct.fields = base_struct.fields + struct.fields

        return struct

    def resolve_type(self, name: str) -> TypeDescriptor:
        sname = strip_type_mods(name)
        sname = self.resolve_alias(sname)
        struct = None

        if sname not in STANDARD_TYPES:
            struct = self.resolve_struct(sname)

        return TypeDescriptor(name, sname, struct)

    def resolve_variant(self, name: str) -> ABIVariant:
        variant = next(
            (v for v in self.abi.variants if v.name == name),
            None
        )
        if not variant:
            raise ValueError(f'variant {name} not found in ABI')

        return variant

    def pack_type(self, desc: TypeDescriptor, v):
        if desc.is_array:
            assert isinstance(v, list)
            amount = len(v)
            desc.type_name = desc.type_name.strip('[]')
            desc.is_array = False
            self.pack_varuint32(amount)
            for i in v:
                self.pack_type(desc, i)
            return

        if desc.is_optional:
            self.pack_uint8(0 if v is None else 1)

        # find right packing function
        pack_partial = None
        if desc.struct:
            pack_partial = partial(
                self.pack_struct, desc.strip_name)

        else:
            pack_fn = getattr(self, f'pack_{desc.strip_name}')
            pack_partial = partial(pack_fn)

        pack_partial(v)


    def unpack_type(self, desc: TypeDescriptor):
        if desc.is_array:
            desc.type_name = desc.type_name.strip('[]')
            desc.is_array = False
            amount = self.unpack_varuint32()
            return [self.unpack_type(desc) for _ in range(amount)]

        if desc.is_optional:
            if self.unpack_uint8() == 0:
                return

        if desc.is_extension and self.remaining == 0:
            return None

        # find right unpacking function
        unpack_partial = None
        if desc.struct:
            unpack_partial = partial(
                self.unpack_struct, desc.strip_name)

        else:
            unpack_fn = getattr(self, f'unpack_{desc.strip_name}')
            unpack_partial = partial(unpack_fn)

        return unpack_partial()


    def pack_struct(self, name: str, v):
        sdesc = self.resolve_type(name)

        if not sdesc.struct:
            raise ValueError(f'struct {name} not found in ABI')

        for field in sdesc.struct.fields:
            fname, ftype = (field.name, field.type)
            fdesc = self.resolve_type(ftype)
            self.pack_type(fdesc, v[fname])

    def unpack_struct(self, name: str) -> OrderedDict:
        sdesc = self.resolve_type(name)

        if not sdesc.struct:
            raise ValueError(f'struct {name} not found in ABI')

        res = OrderedDict()
        for field in sdesc.struct.fields:
            fname, ftype = (field.name, field.type)
            fdesc = self.resolve_type(ftype)
            res[fname] = self.unpack_type(fdesc)

        return res

    def pack_variant(self, name: str, v: tuple):
        assert isinstance(v, tuple)
        assert len(v) == 2
        sub_name, value = v
        variant = self.resolve_variant(name)
        i = variant.types.index(sub_name)
        self.pack_uint8(i)
        self.pack_struct(sub_name, value)

    def unpack_variant(self, name: str) -> tuple[str, OrderedDict]:
        variant = self.resolve_variant(name)
        i = self.unpack_uint8()
        sub_name = variant.types[i]
        return (
            sub_name,
            self.unpack_struct(sub_name)
        )


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

    dsp = DataStream()
    dsp.pack_transaction(tx)
    packed_trx = dsp.getvalue()

    ds.write(packed_trx)
    ds.pack_checksum256(zeros)

    sig = sign_bytes(ds, pk)
    if 'signatures' not in tx:
        tx['signatures'] = []
    tx['signatures'].append(sig)

    m = hashlib.sha256()
    m.update(packed_trx)
    tx_id = binascii.hexlify(m.digest()).decode('utf-8')

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
def pack_abi_data(abi: dict, action: dict) -> str:
    ds = DataStream()
    account = action['account']
    name = action['name']
    data = action['data']

    if 'structs' not in abi:
        raise SerializationException(f'expected abi to have \"structs\" key!')

    struct = [s for s in abi['structs'] if s['name'] == name]

    if len(struct) != 1:
        raise SerializationException(f'expected only one struct def for {name}')

    struct = struct[0]
    struct_fields = {f['name']: f['type'] for f in struct['fields']}

    if isinstance(data, list):
        key_iter = iter(struct_fields.keys())
        value_iter = iter(data)

    else:
        raise SerializationException(f'only list is supported as action params container')

    for _ in range(len(data)):
        field_name = next(key_iter)
        value = next(value_iter)

        typ = struct_fields.get(field_name, None)
        if typ is None:
            raise SerializationException(
                f'expected field \"{field_name}\" to be in {list(struct_fields.keys())}')

        fn_name = typ
        pack_params = [value]

        if typ[-2:] == '[]':
            fn_name = 'array'
            pack_params = [typ[:-2], value]

        elif typ[-1] == '?':
            fn_name = 'optional'
            pack_params = [typ[:-1], value]

        elif typ[-1] == '$':
            pack_params = [typ[:-1], value]

        elif (account == 'eosio' and
                name =='setabi' and
                field_name == 'abi'):

            abi_raw = DataStream()
            abi_raw.pack_abi(value)
            pack_params = [abi_raw.getvalue()]
            fn_name = 'bytes'

        elif typ in ['name', 'asset', 'symbol']:
            pack_params = [str(value)]

        pack_fn = getattr(ds, f'pack_{fn_name}')
        pack_fn(*pack_params)

    return binascii.hexlify(
        ds.getvalue()).decode('utf-8')


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
        'context_free_data': []
    })

    # sign transaction
    _, signed_tx = sign_tx(chain_id, tx, key)

    # pack
    ds = DataStream()
    ds.pack_transaction(signed_tx)
    packed_trx = binascii.hexlify(ds.getvalue()).decode('utf-8')
    return build_push_transaction_body(signed_tx['signatures'][0], packed_trx)
