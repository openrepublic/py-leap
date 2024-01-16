#!/usr/bin/env python3

from .protocol import Symbol


eos_token = Symbol.from_str('4,EOS')
tlos_token = Symbol.from_str('4,TLOS')

ram_token = Symbol.from_str('0,RAM')

DEFAULT_SYS_TOKEN_CODE = tlos_token.code
DEFAULT_SYS_TOKEN_SYM = tlos_token
