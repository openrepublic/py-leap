#!/usr/bin/env python3

from leap.cleos import CLEOS
from leap.protocol import Asset


def test_get_price():

    cleos = CLEOS('https://testnet.libre.org')

    res = cleos.get_table('swap.libre', 'BTCUSD', 'stat')[0]

    quant_1 = Asset.from_str(res['pool1']['quantity'])
    quant_2 = Asset.from_str(res['pool2']['quantity'])

    price = quant_1.to_decimal() / quant_2.to_decimal()

    assert not price.is_zero()
