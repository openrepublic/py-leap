#!/usr/bin/python3
from ctypes import *
from pathlib import Path


"""To correctly load the shared module py_eosio.cpython.so we need
to load the additional .so dependencies in order,
"""


__bin_path = Path(__file__).parent

def find_library(pattern):
    """Globs pattern on the current binary directory and returns
    first match.
    """
    for f in __bin_path.glob(pattern):
        if not f.is_dir() and f.suffix != '.py':
            return f


pattern_list = [
    'libboost_chrono*',
    'libboost_date_time*',
    'libboost_filesystem*',
    'libboost_iostreams*',
    'libsecp256k1*',
    'libcrypto*',
    'libssl*',
    'libfc*',
    'libeosio_chain*'
]

file_whitelist = [
    find_library(pattern)
    for pattern in pattern_list
]

for file in file_whitelist:
    cdll.LoadLibrary(file)

from .py_eosio import chain, collections
