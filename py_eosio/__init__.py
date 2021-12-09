#!/usr/bin/python3

import sys

from .__ctypes import chain, collections

sys.modules['py_eosio.chain'] = chain
sys.modules['py_eosio.chain.time'] = chain.time
sys.modules['py_eosio.chain.types'] = chain.types
sys.modules['py_eosio.chain.config'] = chain.config
sys.modules['py_eosio.chain.crypto'] = chain.crypto
sys.modules['py_eosio.chain.testing'] = chain.testing


sys.modules['py_eosio.collections'] = collections
