#!/usr/bin/env python3

from leap.sugar import Asset, Symbol, asset_from_str
from leap.tokens import tlos_token


def test_asset_to_str():
    assert str(Asset(1000 * (10 ** 4), tlos_token)) == '1000.0000 TLOS'

def test_asset_from_str():
    assert Asset(1000 * (10 ** 4), tlos_token) == asset_from_str('1000.0000 TLOS')

def test_zero_prec_asset():
    assert str(Asset(1, Symbol('SYS', 0))) == '1 SYS'
