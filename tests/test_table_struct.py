import trio
from msgspec import Struct

from leap import CLEOS


def test_account_balance():

    async def main():
        class BalanceRow(Struct):
            balance: str

        cleos = CLEOS('https://testnet.telos.net')

        res = cleos.get_table(
            'eosio.token', 'eosio', 'accounts')
        assert len(res) > 0

        sres = cleos.get_table(
            'eosio.token', 'eosio', 'accounts', resp_cls=BalanceRow)
        assert len(sres) > 0

        assert res[0]['balance'] == sres[0].balance

        res = await cleos.aget_table(
            'eosio.token', 'eosio', 'accounts')
        assert len(res) > 0

        sres = await cleos.aget_table(
            'eosio.token', 'eosio', 'accounts', resp_cls=BalanceRow)
        assert len(sres) > 0

        assert res[0]['balance'] == sres[0].balance

    trio.run(main)
