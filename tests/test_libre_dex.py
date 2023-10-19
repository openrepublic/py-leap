#!/usr/bin/env python3


from leap.cleos import CLEOS
from leap.sugar import asset_from_str


def test_get_price():

    cleos = CLEOS(
        url='https://libre.quantumblok.com',
        remote='https://libre.quantumblok.com')

    res = cleos.get_table('swap.libre', 'BTCUSD', 'stat')[0]

    quant_1 = asset_from_str(res['pool1']['quantity'])
    quant_2 = asset_from_str(res['pool2']['quantity'])

    price = quant_1.to_decimal() / quant_2.to_decimal()

    breakpoint()
