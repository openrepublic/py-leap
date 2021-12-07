#!/usr/bin/python3

import sys

from .__ctypes import chain, collections

sys.modules['py_eosio.chain'] = chain
sys.modules['py_eosio.collections'] = collections
