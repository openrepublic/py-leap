#!/usr/bin/env python3

from leap.protocol import Asset, Symbol
from leap.tokens import tlos_token


def test_asset_to_str():
    assert str(Asset(1000 * (10 ** 4), tlos_token)) == '1000.0000 TLOS'

def test_asset_from_str():
    assert Asset(1000 * (10 ** 4), tlos_token) == Asset.from_str('1000.0000 TLOS')

def test_zero_prec_asset():
    assert str(Asset(1, '0,SYS')) == '1 SYS'
