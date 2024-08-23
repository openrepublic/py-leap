#!/usr/bin/env python3

import struct
import hashlib
import binascii

from copy import deepcopy
from base58 import b58encode
from typing import OrderedDict
from cryptos import decode_privkey, hash_to_int, encode_privkey, decode, encode, \
    hmac, fast_multiply, G, inv, N, get_privkey_format, random_key, encode_pubkey, privtopub
from datetime import datetime, timedelta

import msgspec

from ..errors import SerializationException
from .abi import ABI, ABIDataStream
from .ds import DATE_TIME_FORMAT, DataStream


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
