#!/usr/bin/env python3

from py_eosio.sugar import random_token_symbol, string_to_name


def test_multi_sig_contract(msig_contract):
    cleos = msig_contract

    a, b = (cleos.new_account() for _ in range(2))

    owner_perms = [f'{name}@active' for name in (a, b)]
    owner_perms.sort()
    
    proposal = cleos.multi_sig_propose(
        a,
        owner_perms,
        owner_perms,
        'testcontract',
        'testmultisig',
        {
            'a': a,
            'b': b
        }
    )

    for name in (a, b):
        ec, _ = cleos.multi_sig_approve(
            a,
            proposal,
            [f'{name}@active'],
            name
        )
        assert ec == 0

    ec, _ = cleos.push_action(
        'testcontract',
        'initcfg',
        [0],
        'eosio@active'
    )
    assert ec == 0
   
    conf = cleos.get_table(
        'testcontract',
        'testcontract',
        'config'
    )[0]

    assert conf['value'] == '0'

    ec, _ = cleos.multi_sig_exec(
        a,
        proposal,
        f'{a}@active'
    )
    assert ec == 0
   
    conf = cleos.get_table(
        'testcontract',
        'testcontract',
        'config'
    )[0]

    assert int(conf['value']) == string_to_name(a) + string_to_name(b)


def test_multi_sig_transaction_ok(msig_contract):
    cleos = msig_contract
    issuer, worker = (
        cleos.new_account()
        for _ in range(2)
    )

    symbol = random_token_symbol()
    amount = '1000.00'
    max_supply = f'{amount} {symbol}'

    first_amount = 20
    second_amount = 30

    fpay_amount = f'{first_amount}.00'
    spay_amount = f'{second_amount}.00'

    total_pay = f'{first_amount + second_amount}.00 {symbol}'
    
    ec, _ = cleos.create_token(issuer, max_supply)
    assert ec == 0

    ec, _ = cleos.issue_token(issuer, max_supply, '')
    assert ec == 0

    ec, _ = cleos.transfer_token(
        issuer, worker, f'{fpay_amount} {symbol}', 'first pay')

    pay_asset = f'{spay_amount} {symbol}'

    proposal = cleos.multi_sig_propose(
        worker,
        [f'{issuer}@active'],
        [f'{issuer}@active'],
        'eosio.token',
        'transfer',
        {
            'from': issuer,
            'to': worker,
            'quantity': pay_asset,
            'memo': 'multi payment'
        }
    )

    ec, out = cleos.multi_sig_approve(
        worker,
        proposal,
        [f'{issuer}@active'],
        issuer
    )
    assert ec == 0

    ec, out = cleos.multi_sig_exec(
        worker,
        proposal,
        worker
    )
    assert ec == 0

    balance = cleos.get_balance(worker)

    assert balance
    assert balance == total_pay


def test_multi_sig_transaction_error(msig_contract):
    cleos = msig_contract
    issuer, worker = (
        cleos.new_account()
        for _ in range(2)
    )

    symbol = random_token_symbol()
    amount = '1000.00'
    max_supply = f'{amount} {symbol}'

    first_amount = 20
    second_amount = 30

    fpay_amount = f'{first_amount}.00'
    spay_amount = f'{second_amount}.00'

    ec, _ = cleos.create_token(issuer, max_supply)
    assert ec == 0

    ec, _ = cleos.issue_token(issuer, max_supply, '')
    assert ec == 0

    fpay_asset = f'{fpay_amount} {symbol}'

    ec, _ = cleos.transfer_token(
        issuer, worker, fpay_asset, 'first pay')

    proposal = cleos.multi_sig_propose(
        worker,
        [f'{issuer}@active'],
        [f'{issuer}@active'],
        'eosio.token',
        'transfer',
        {
            'from': issuer,
            'to': worker,
            'quantity': f'{spay_amount} {symbol}',
            'memo': 'multi payment'
        }
    )
    
    ec, out = cleos.multi_sig_exec(
        worker,
        proposal,
        worker
    )
    assert ec == 1

    balance = cleos.get_balance(worker)

    assert balance
    assert balance == fpay_asset
