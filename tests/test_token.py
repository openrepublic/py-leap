#!/usr/bin/env python3

from py_eosio.sugar import random_token_symbol


def test_create(cleos):
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_negative_supply(cleos):
    creator = cleos.new_account()

    ec, out = cleos.create_token(
        creator, f'-1000.000 {random_token_symbol()}', retry=0)
    assert ec == 1
    assert 'max-supply must be positive' in out


def test_symbol_exists(cleos):
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    ec, out = cleos.create_token(creator, max_supply, retry=0)
    assert ec == 1
    assert 'token with symbol already exists' in out


def test_create_max_possible(cleos):
    creator = cleos.new_account()
    amount = 4611686018427387903
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_max_possible_plus_one(cleos):
    creator = cleos.new_account()
    amount = 4611686018427387903 + 1
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    ec, out = cleos.create_token(creator, max_supply, retry=0)
    assert ec == 1
    assert 'magnitude of asset amount must be less than 2^62' in out

def test_create_max_decimals(cleos):
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


def test_issue(cleos):
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
    ec, out = cleos.issue_token(creator, issued, 'hola')
    assert ec == 1
    assert 'quantity exceeds available supply' in out

    issued = f'-1.000 {sym}'
    ec, out = cleos.issue_token(creator, issued, 'hola')
    assert ec == 1
    assert 'must issue positive quantity' in out

    issued = f'500.000 {sym}'
    ec, _ = cleos.issue_token(creator, issued, 'hola')
    assert ec == 0


def test_retire(cleos):
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
    ec, out = cleos.retire_token(creator, issued, retry=0)
    assert ec == 1
    assert 'overdrawn balance' in out

    # transfer some tokens to friend
    friend = cleos.new_account()

    ec, _ = cleos.transfer_token(
        creator, friend, f'200.000 {sym}')
    assert ec == 0
 
    # should fail to retire since tokens are not on the issuer's balance
    ec, out = cleos.retire_token(creator, f'300.000 {sym}', retry=0)
    assert ec == 1

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
    ec, out = cleos.retire_token(creator, f'1.000 {sym}', retry=0)
    assert ec == 1
    assert 'overdrawn balance' in out


def test_transfer(cleos):
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

    ec, out = cleos.transfer_token(
        creator, friend, f'701 {sym}', retry=0)
    assert ec == 1
    assert 'overdrawn balance' in out 

    ec, out = cleos.transfer_token(
        creator, friend, f'-1 {sym}', retry=0)
    assert ec == 1
    assert 'must transfer positive quantity' in out


def test_open(cleos):
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'
    zero = f'0 {sym}'

    ec, _ = cleos.create_token(creator, max_supply)
    assert ec == 0

    balance = cleos.get_balance(creator)

    assert balance == None 

    friend = cleos.new_account()

    ec, out = cleos.issue_token(friend, max_supply, 'hola', retry=0)
    assert ec == 1
    assert 'tokens can only be issued to issuer account' in out

    ec, _ = cleos.issue_token(creator, max_supply, 'hola')
    assert ec == 0

    balance = cleos.get_balance(creator)
    assert balance == max_supply

    balance = cleos.get_balance(friend)
    assert balance == None

    ec, out = cleos.open_token('null', f'0,{sym}', creator, retry=0)
    assert ec == 1
    assert 'owner account does not exist' in out

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
    ec, out = cleos.open_token(tester, '0,INVALID', creator, retry=0)
    assert ec == 1
    assert 'symbol does not exist' in out

    ec, out = cleos.open_token(tester, f'1,{sym}', creator, retry=0)
    assert ec == 1
    assert 'symbol precision mismatch' in out


def test_close(cleos):
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
