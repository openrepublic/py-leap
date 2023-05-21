#!/usr/bin/env python3

from .sugar import Asset
from .tokens import sys_token, ram_token


sys_token_amount = 10000000000000
sys_token_supply = Asset(
    sys_token_amount, sys_token)

sys_token_init_issue = Asset(
    10000000000000, sys_token)

ram_supply = Asset(
    100000000000000 , ram_token)
