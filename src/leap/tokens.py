#!/usr/bin/env python3

from .protocol import Symbol


eos_token = Symbol('EOS', 4)
tlos_token = Symbol('TLOS', 4)

ram_token = Symbol('RAM', 0)

DEFAULT_SYS_TOKEN_SYM = tlos_token
