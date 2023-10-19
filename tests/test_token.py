#!/usr/bin/env python3

from leap.sugar import random_token_symbol


def test_create(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_negative_supply(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()

    ec, res = cleos.create_token(
        creator, f'-1000.000 {random_token_symbol()}')
    assert ec == 1
    assert 'error' in res
    assert 'max-supply must be positive' in res['error']['details'][0]['message']


def test_symbol_exists(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    cleos.wait_blocks(1)

    ec, res = cleos.create_token(creator, max_supply)
    assert ec == 1
    assert 'error' in res
    assert 'token with symbol already exists' in res['error']['details'][0]['message']


def test_create_max_possible(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    amount = (1 << 62) - 1
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_max_possible_plus_one(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    amount = (1 << 62)
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    ec, res = cleos.create_token(creator, max_supply)
    assert ec == 1
    assert 'error' in res
    assert 'invalid supply' in res['error']['details'][0]['message']

def test_create_max_decimals(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    amount = 1
    decimals = 18
    sym = random_token_symbol()
    zeros = ''.join(['0' for x in range(decimals)])
    max_supply = f'{amount}.{zeros} {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.{zeros} {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_issue(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0
    issued = f'500.000 {sym}'
    ec, _ = cleos.issue_token(creator, issued, 'hola')
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == issued
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == issued

    issued = f'500.001 {sym}'
    ec, res = cleos.issue_token(creator, issued, 'hola')
    assert ec == 1
    assert 'error' in res
    assert 'quantity exceeds available supply' in res['error']['details'][0]['message']

    issued = f'-1.000 {sym}'
    ec, res = cleos.issue_token(creator, issued, 'hola')
    assert ec == 1
    assert 'error' in res
    assert 'must issue positive quantity' in res['error']['details'][0]['message']

    cleos.wait_blocks(1)

    issued = f'500.000 {sym}'
    ec, _ = cleos.issue_token(creator, issued, 'hola')
    assert ec == 0


def test_retire(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    issued = f'500.000 {sym}'
    ec, _ = cleos.issue_token(creator, issued, 'hola')
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == issued
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == issued

    ec, _ = cleos.retire_token(creator, f'200.000 {sym}', 'hola')
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'300.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == f'300.000 {sym}'

    # should fail to retire more than current supply
    ec, res = cleos.retire_token(creator, issued)
    assert ec == 1
    assert 'overdrawn balance' in res['error']['details'][0]['message']

    # transfer some tokens to friend
    friend = cleos.new_account()

    ec, _ = cleos.transfer_token(
        creator, friend, f'200.000 {sym}')
    assert ec == 0
 
    # should fail to retire since tokens are not on the issuer's balance
    ec, res = cleos.retire_token(creator, f'300.000 {sym}')
    assert ec == 1
    assert 'overdrawn balance' in res['error']['details'][0]['message']

    # give tokens back
    ec, _ = cleos.transfer_token(
        friend, creator, f'200.000 {sym}')
    assert ec == 0

    ec, _ = cleos.retire_token(creator, f'300.000 {sym}', 'hola')
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == f'0.000 {sym}'

    # try to retire with 0 balance
    ec, res = cleos.retire_token(creator, f'1.000 {sym}')
    assert ec == 1
    assert 'overdrawn balance' in res['error']['details'][0]['message']


def test_transfer(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    ec, _ = cleos.issue_token(creator, max_supply, 'hola')
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == max_supply
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == max_supply

    friend = cleos.new_account()

    ec, _ = cleos.transfer_token(
        creator, friend, f'300 {sym}')
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == f'700 {sym}'

    balance = cleos.get_balance(friend)
    assert balance == f'300 {sym}'

    ec, res = cleos.transfer_token(creator, friend, f'701 {sym}')
    assert ec == 1
    assert 'overdrawn balance' in res['error']['details'][0]['message']

    ec, res = cleos.transfer_token(creator, friend, f'-1 {sym}')
    assert ec == 1
    assert 'must transfer positive quantity' in res['error']['details'][0]['message']


def test_open(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'
    zero = f'0 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    balance = cleos.get_balance(creator)

    assert balance == None

    friend = cleos.new_account()

    ec, res = cleos.issue_token(friend, max_supply, 'hola')
    assert ec == 1
    assert 'tokens can only be issued to issuer account' in res['error']['details'][0]['message']

    ec, _ = cleos.issue_token(creator, max_supply, 'hola')
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == max_supply

    balance = cleos.get_balance(friend)
    assert balance == None

    ec, res = cleos.open_token('null', f'0,{sym}', creator)
    assert ec == 1
    assert 'owner account does not exist' in res['error']['details'][0]['message']

    ec, _ = cleos.open_token(friend, f'0,{sym}', creator)
    assert ec == 0

    balance = cleos.get_balance(friend)
    assert balance == zero

    transfered = f'200 {sym}'
    ec, _ = cleos.transfer_token(
        creator, friend, transfered)
    assert ec == 0

    balance = cleos.get_balance(friend)
    assert balance == transfered

    tester = cleos.new_account()
    ec, res = cleos.open_token(tester, '0,INVALID', creator)
    assert ec == 1
    assert 'symbol does not exist' in res['error']['details'][0]['message']

    ec, res = cleos.open_token(tester, f'1,{sym}', creator)
    assert ec == 1
    assert 'symbol precision mismatch' in res['error']['details'][0]['message']


def test_close(cleos_w_token):
    cleos = cleos_w_token
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'
    zero = f'0 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == None

    ec, _ = cleos.issue_token(creator, max_supply, 'hola')
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == max_supply

    friend = cleos.new_account()
    ec, _ = cleos.transfer_token(
        creator, friend, max_supply)
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == zero

    ec, _ = cleos.close_token(creator, f'0,{sym}')
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == None
