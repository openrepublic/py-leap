#!/usr/bin/env python3

from decimal import Decimal

from leap.cleos import CLEOS
from leap.protocol import Asset


async def test_get_price():

    cleos = CLEOS('https://testnet.libre.org')

    res = cleos.get_table('swap.libre', 'BTCUSD', 'stat')[0]

    quant_1 = Asset.from_str(res['pool1']['quantity'])
    quant_2 = Asset.from_str(res['pool2']['quantity'])

    price = quant_1.to_decimal() / quant_2.to_decimal()

    assert not price.is_zero()

    ares = (await cleos.aget_table('swap.libre', 'BTCUSD', 'stat'))[0]

    a_quant_1 = Asset.from_str(res['pool1']['quantity'])
    a_quant_2 = Asset.from_str(res['pool2']['quantity'])

    a_price = a_quant_1.to_decimal() / a_quant_2.to_decimal()

    assert price - a_price < Decimal('0.1')
