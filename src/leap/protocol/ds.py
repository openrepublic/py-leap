# Most of this module was taken from:
# https://github.com/EOSArgentina/ueosio
# Much apreciated!!

import io
import struct
import hashlib
import binascii
import calendar

from copy import deepcopy
from base58 import b58decode, b58encode
from datetime import datetime
from collections import OrderedDict

from ..errors import SerializationException

# dont use hashlib due to reliance in openssl and deprecation of ripemd160 on it
from ripemd.ripemd160 import ripemd160


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
        if not stream:
            stream = io.BytesIO()
        else:
            stream = io.BytesIO(stream)
        self.stream = stream

    def write(self, v):
        self.stream.write(v)

    def read(self, n):
        return self.stream.read(n)

    def getvalue(self):
        return bytearray(self.stream.getvalue())

    def pack_array(self, type, values):
        self.pack_varuint32(len(values))
        for v in values:
            getattr(self, 'pack_' + type)(v)

    def unpack_array(self, type):
        l = self.unpack_varuint32()
        res = []
        for i in range(l):
            res.append(getattr(self, 'unpack_' + type)())
        return res

    def pack_extension(self, v):
        self.pack_uint16(v[0])
        self.pack_bytes(v[1])

    def unpack_extension(self):
        return [
            self.unpack_uint16(),
            self.unpack_bytes()
        ]

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

    def pack_permission_level(self, v):
        self.pack_account_name(v["actor"])
        self.pack_account_name(v["permission"])

    def unpack_permission_level(self):
        return OrderedDict([
            ("actor", self.unpack_account_name()),
            ("permission", self.unpack_account_name())
        ])

    def pack_permission_level_weight(self, v):
        self.pack_permission_level(v["permission"])
        self.pack_int16(v["weight"])

    def unpack_permission_level_weight(self):
        return OrderedDict([
            ("permission", self.unpack_permission_level()),
            ("weight", self.unpack_int16())
        ])

    def pack_key_weight(self, v):
        self.pack_public_key(v["key"])
        self.pack_int16(v["weight"])

    def unpack_key_weight(self):
        return OrderedDict([
            ("key", self.unpack_public_key()),
            ("weight", self.unpack_int16())
        ])

    def pack_wait_weight(self, v):
        self.pack_uint32(v["wait_sec"])
        self.pack_int16(v["weight"])

    def unpack_wait_weight(self):
        return OrderedDict([
            ("wait_sec", self.unpack_uint32()),
            ("weight", self.unpack_int16())
        ])

    def pack_authority(self, v):
        self.pack_uint32(v["threshold"])
        self.pack_array('key_weight', v['keys'])
        self.pack_array('permission_level_weight', v['accounts'])
        self.pack_array('wait_weight', v['waits'])

    def unpack_authority(self):
        return OrderedDict([
            ("threshold", self.unpack_uint32()),
            ("keys", self.unpack_array('key_weight')),
            ("accounts", self.unpack_array('permission_level_weight')),
            ("waits", self.unpack_array('wait_weight'))
        ])

    def pack_action(self, v):
        self.pack_account_name(v["account"])
        self.pack_account_name(v["name"])
        self.pack_array("permission_level", v["authorization"])
        self.pack_bytes(bytes.fromhex(v["data"]))

    def unpack_action(self):
        return OrderedDict([
            ("account", self.unpack_account_name()),
            ("name", self.unpack_account_name()),
            ("authorization", self.unpack_array("permission_level")),
            ("data", self.unpack_bytes())
        ])

    def pack_transaction(self, v):
        self.pack_time_point_sec(v["expiration"])
        self.pack_uint16(v["ref_block_num"])
        self.pack_uint32(v["ref_block_prefix"])
        self.pack_varuint32(v["max_net_usage_words"])
        self.pack_uint8(v["max_cpu_usage_ms"])
        self.pack_varuint32(v["delay_sec"])
        self.pack_array("action", v["context_free_actions"])
        self.pack_array("action", v["actions"])
        self.pack_array("extension", v["transaction_extensions"])

    def pack_signed_transaction(self, v):
        self.pack_transaction(v)
        self.pack_array("signature", v["signatures"])
        self.pack_array("bytes", v["context_free_data"])

    def unpack_transaction(self):
        return OrderedDict([
            ("expiration", self.unpack_time_point_sec()),
            ("ref_block_num", self.unpack_uint16()),
            ("ref_block_prefix", self.unpack_uint32()),
            ("max_net_usage_words", self.unpack_varuint32()),
            ("max_cpu_usage_ms", self.unpack_uint8()),
            ("delay_sec", self.unpack_varuint32()),
            ("context_free_actions", self.unpack_array("action")),
            ("action", self.unpack_array("action")),
            ("transaction_extensions", self.unpack_array("extension")),
        ])

    def unpack_signed_transaction(self):
        r = self.unpack_transaction(self)
        r.update(OrderedDict([
            ("signatures", self.unpack_signature()),
            ("context_free_data", self.unpack_array("bytes"))
        ]))
        return r

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
            if ripemd160(data[:-4])[:4] != data[-4:]: raise Exception("checksum failed")
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
            data = data + ripemd160(data)[:4]
            return "EOS" + b58encode(data).decode('ascii')
        elif t == 1:
            raise Exception("not implementd")
        else:
            raise Exception("invalid binary pubkey")

    def pack_signature(self, v):

        def pack_signature_sufix(b58sig, sufix):
            data = b58decode(b58sig)
            if len(data) != 65 + 4: raise Exception("invalid {0} signature".format(sufix))
            if ripemd160(data[:-4] + sufix)[:4] != data[-4:]: raise Exception("checksum failed")
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
            data = data + ripemd160(data + b"K1")[:4]
            return "SIG_K1_" + b58encode(data).decode("ascii")
        elif t == 1:
            raise Exception("not implementd")
        else:
            raise Exception("invalid binary signature")

    def pack_type_def(self, v):
        self.pack_string(v["new_type_name"])
        self.pack_string(v["type"])

    def unpack_type_def(self):
        return OrderedDict([
            ("new_type_name", self.unpack_string()),
            ("type", self.unpack_string())
        ])

    def pack_optional(self, type, v):
        if v is None:
            self.pack_uint8(0)
        else:
            self.pack_uint8(1)
            getattr(self, 'pack_' + type)(v)

    def unpack_optional(self, type):
        if not self.unpack_uint8():
            return None
        return getattr(self, 'unpack_' + type)()

    def pack_field_def(self, v):
        self.pack_string(v["name"])
        self.pack_string(v["type"])

    def unpack_field_def(self):
        return OrderedDict([
            ("name", self.unpack_string()),
            ("type", self.unpack_string())
        ])

    def pack_struct_def(self, v):
        self.pack_string(v["name"])
        self.pack_string(v["base"])
        self.pack_array("field_def", v["fields"])

    def unpack_struct_def(self):
        return OrderedDict([
            ("name", self.unpack_string()),
            ("base", self.unpack_string()),
            ("fields", self.unpack_array("field_def"))
        ])

    def pack_action_def(self, v):
        self.pack_account_name(v["name"])
        self.pack_string(v["type"])
        self.pack_string(v["ricardian_contract"])

    def unpack_action_def(self):
        return OrderedDict([
            ("name", self.unpack_account_name()),
            ("type", self.unpack_string()),
            ("ricardian_contract", self.unpack_string())
        ])

    def pack_table_def(self, v):
        self.pack_account_name(v["name"])
        self.pack_string(v["index_type"])
        self.pack_array("string", v["key_names"])
        self.pack_array("string", v["key_types"])
        self.pack_string(v["type"])

    def unpack_table_def(self):
        return OrderedDict([
            ("name", self.unpack_account_name()),
            ("index_type", self.unpack_string()),
            ("key_names", self.unpack_array("string")),
            ("key_types", self.unpack_array("string")),
            ("type", self.unpack_string())
        ])

    def pack_clause_pair(self, v):
        self.pack_string(v["id"])
        self.pack_string(v["body"])

    def unpack_clause_pair(self):
        return OrderedDict([
            ("id", self.unpack_string()),
            ("body", self.unpack_string()),
        ])

    def pack_error_message(self, v):
        self.pack_uint64(v["error_code"])
        self.pack_string(v["error_msg"])

    def unpack_error_message(self):
        return OrderedDict([
            ("error_code", self.unpack_uint64()),
            ("error_msg", self.unpack_string()),
        ])

    def pack_variant(self, v):
        self.pack_string(v["name"])
        self.pack_array("string", v["types"])

    def unpack_variant(self):
        return OrderedDict([
            ("name", self.unpack_string()),
            ("types", self.unpack_array("string")),
        ])

    def pack_abi(self, v):
        self.pack_string(v["version"])
        self.pack_array("type_def", v.get("types",[]))
        self.pack_array("struct_def", v.get("structs",[]))
        self.pack_array("action_def", v.get("actions",[]))
        self.pack_array("table_def", v.get("tables",[]))
        self.pack_array("clause_pair", v.get("ricardian_clauses",[]))
        self.pack_array("error_message", v.get("error_messages",[]))
        self.pack_array("extension", v.get("abi_extensions",[]))
        self.pack_array("variant", v.get("variants",[]))

    def unpack_abi(self):
        tmp = OrderedDict([
            ("version", self.unpack_string()),
            ("types", self.unpack_array("type_def")),
            ("structs", self.unpack_array("struct_def")),
            ("actions", self.unpack_array("action_def")),
            ("tables", self.unpack_array("table_def")),
            ("ricardian_clauses", self.unpack_array("clause_pair")),
            ("error_messages", self.unpack_array("error_message")),
            ("abi_extensions", self.unpack_array("extension"))
        ])

        try:
            tmp["variants"] = self.unpack_array("variant")
        except:
            pass
        return tmp

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

    def pack_chain_id_type(self, v):
        self.pack_sha256(v)

    def unpack_chain_id_type(self):
        self.unpack_sha256()

    def pack_block_id_type(self, v):
        self.pack_sha256(v)

    def unpack_block_id_type(self):
        self.unpack_sha256()

    def pack_tstamp(self, v):
        self.pack_uint64(v)

    def unpack_tstamp(self):
        self.unpack_uint64()

    def pack_blockchain_parameters(self, v):
        self.pack_uint64(v["max_block_net_usage"])
        self.pack_uint32(v["target_block_net_usage_pct"])
        self.pack_uint32(v["max_transaction_net_usage"])
        self.pack_uint32(v["base_per_transaction_net_usage"])
        self.pack_uint32(v["net_usage_leeway"])
        self.pack_uint32(v["context_free_discount_net_usage_num"])
        self.pack_uint32(v["context_free_discount_net_usage_den"])
        self.pack_uint32(v["max_block_cpu_usage"])
        self.pack_uint32(v["target_block_cpu_usage_pct"])
        self.pack_uint32(v["max_transaction_cpu_usage"])
        self.pack_uint32(v["min_transaction_cpu_usage"])
        self.pack_uint32(v["max_transaction_lifetime"])
        self.pack_uint32(v["deferred_trx_expiration_window"])
        self.pack_uint32(v["max_transaction_delay"])
        self.pack_uint32(v["max_inline_action_size"])
        self.pack_uint16(v["max_inline_action_depth"])
        self.pack_uint16(v["max_authority_depth"])

    def pack_handshake_message(self, v):
        self.pack_uint16(v.network_version)
        self.pack_chain_id_type(v.chain_id)
        self.pack_sha256(v.node_id)
        self.pack_public_key(v.key)
        self.pack_tstamp(v.time)
        self.pack_sha256(v.token)
        self.pack_signature(v.sig)
        self.pack_string(v.p2p_address)
        self.pack_uint32(v.last_irreversible_block_num)
        self.pack_block_id_type(v.last_irreversible_block_id)
        self.pack_uint32(v.head_num)
        self.pack_block_id_type(v.head_id)
        self.pack_string(v.os)
        self.pack_string(v.agent)
        self.pack_int16(v.generation)

    def unpack_handshake_message(self):
        return [
            self.unpack_uint16(),
            self.unpack_chain_id_type(),
            self.unpack_sha256(),
            self.unpack_public_key(),
            self.unpack_tstamp(),
            self.unpack_sha256(),
            self.unpack_signature(),
            self.unpack_string(),
            self.unpack_uint32(),
            self.unpack_block_id_type(),
            self.unpack_uint32(),
            self.unpack_block_id_type(),
            self.unpack_string(),
            self.unpack_string(),
            self.unpack_int16()
        ]

    def unpack_partial_transaction(self):
        return OrderedDict([
            ("version", self.unpack_varuint32()),
            ("expiration", self.unpack_time_point_sec()),
            ("ref_block_num", self.unpack_uint16()),
            ("ref_block_prefix", self.unpack_uint32()),
            ("max_net_usage_words", self.unpack_varuint32()),
            ("max_cpu_usage_ms", self.unpack_uint8()),
            ("delay_sec", self.unpack_varuint32()),
            ("transaction_extensions", self.unpack_array('extension')),
            ("signatures", self.unpack_array('signature')),
            ("context_free_data", self.unpack_bytes()),
        ])

    def unpack_augmented_transaction_trace(self):
        return OrderedDict([
            ("version", self.unpack_varuint32()),
            ("trace_id", self.unpack_checksum256()),
            ("status", self.unpack_uint8()),
            ("cpu_usage_us", self.unpack_uint32()),
            ("net_usage_words", self.unpack_varuint32()),
            ("trace_elapsed", self.unpack_int64()),
            ("net_usage", self.unpack_uint64()),
            ("scheduled", self.unpack_uint8()),
            ("action_traces", self.unpack_array("action_trace")),
            ("account_delta", self.unpack_optional('account_delta')),
            ("trace_error", self.unpack_optional('string')),
            ("trace_ec", self.unpack_optional('uint64')),
            ("dtrx_trace", self.unpack_optional('augmented_transaction_trace')),
            ("partial_transaction", self.unpack_optional('partial_transaction'))
        ])

    def unpack_account_delta(self):
        return OrderedDict([
            ("account", self.unpack_name()),
            ("delta", self.unpack_uint64()),
        ])

    def unpack_auth_sequence(self):
        return OrderedDict([
            ("account_name", self.unpack_name()),
            ("sequence", self.unpack_uint64()),
        ])

    def unpack_action_receipt(self):
        return OrderedDict([
            ("version", self.unpack_varuint32()),
            ("receiver", self.unpack_name()),
            ("act_digest", self.unpack_checksum256()),
            ("global_sequence", self.unpack_uint64()),
            ("recv_sequence", self.unpack_uint64()),
            ("auth_sequence", self.unpack_array("auth_sequence")),
            ("code_sequence", self.unpack_varuint32()),
            ("abi_sequence", self.unpack_varuint32()),
        ])

    def unpack_action_trace(self):
        return OrderedDict([
            ("version", self.unpack_varuint32()),
            ("action_ordinal", self.unpack_varuint32()),
            ("creator_action_ordinal", self.unpack_varuint32()),
            ("receipt", self.unpack_optional('action_receipt')),
            ("receiver", self.unpack_name()),
            ("action", self.unpack_action()),
            ("context_free", self.unpack_uint8()),
            ("elapsed", self.unpack_int64()),
            ("console", self.unpack_string()),
            ("account_delta", self.unpack_array('account_delta')),
            ("error", self.unpack_optional('string')),
            ("error_code", self.unpack_optional('uint64')),
        ])

    def unpack_stat_table(self):
        return OrderedDict([
            ("supply", self.unpack_asset()),
            ("max_supply",  self.unpack_asset()),
            ("issuer", self.unpack_name()),
            ("pool1", self.unpack_extended_asset()),
            ("pool2", self.unpack_extended_asset()),
            ("fee", self.unpack_int32()),
            ("fee_contract", self.unpack_name())
        ])


    def unpack_history_context_wrapper(self):
        return OrderedDict([
            ("dummy", self.unpack_varuint32()),
            ("code",  self.unpack_name()),
            ("scope", self.unpack_name()),
            ("table", self.unpack_name()),
            ("pk", self.unpack_uint64()),
            ("payer", self.unpack_name()),
            ("value", self.unpack_bytes())
        ])

    def unpack_state_row(self):
        return OrderedDict([
            ("present", self.unpack_uint8()),
            ("bytes", self.unpack_bytes())
        ])

    def unpack_table_delta(self):
        return OrderedDict([
            ("struct_version", self.unpack_varuint32()),
            ("name", self.unpack_string()),
            ("rows", self.unpack_array("state_row"))
        ])

    def pack_block_position(self, v):
        self.pack_uint32(v['block_num'])
        self.pack_checksum256(v['block_id'])

    def unpack_block_position(self):
        return OrderedDict([
            ("block_num", self.unpack_uint32()),
            ("block_id", self.unpack_checksum256())
        ])

    def pack_get_blocks_request_v0(self, v):
        self.pack_uint32(v['start_block_num'])
        self.pack_uint32(v['end_block_num'])
        self.pack_uint32(v['max_messages_in_flight'])
        self.pack_array("block_position", v['have_positions'])
        self.pack_uint8(v['irreversible_only'])
        self.pack_uint8(v['fetch_block'])
        self.pack_uint8(v['fetch_traces'])
        self.pack_uint8(v['fetch_deltas'])

    def unpack_get_status_result_v0(self):
        return OrderedDict([
            ("head", self.unpack_block_position()),
            ("last_irreversible", self.unpack_block_position()),
            ("trace_begin_block", self.unpack_uint32()),
            ("trace_end_block", self.unpack_uint32()),
            ("chain_state_begin_block", self.unpack_uint32()),
            ("chain_state_end_block", self.unpack_uint32())
        ])

    def unpack_get_blocks_result_v0(self):
        return OrderedDict([
            ("head", self.unpack_block_position()),
            ("last_irreversible", self.unpack_block_position()),
            ("this_block", self.unpack_optional('block_position')),
            ("prev_block", self.unpack_optional('block_position')),
            ("block", self.unpack_optional('bytes')),
            ("traces", self.unpack_optional('bytes')),
            ("deltas", self.unpack_optional('bytes'))
        ])


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

def gen_key_pair():
    pk   = random_key()
    wif  = encode_privkey(pk, 'wif')
    data = encode_pubkey(privtopub(pk),'bin_compressed')
    data = data + ripemd160(data)[:4]
    pubkey = "EOS" + b58encode(data).decode('utf8')
    return wif, pubkey

def get_pub_key(pk):
    data = encode_pubkey(privtopub(pk),'bin_compressed')
    data = data + ripemd160(data)[:4]
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
