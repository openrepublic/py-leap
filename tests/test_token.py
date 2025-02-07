#!/usr/bin/env python3

import pytest

from leap.sugar import random_token_symbol
from leap.errors import TransactionPushError


def test_create(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    cleos.create_token(creator, max_supply)

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_negative_supply(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()

    with pytest.raises(TransactionPushError) as err:
        cleos.create_token(
            creator, f'-1000.000 {random_token_symbol()}', retries=1)

    assert 'max-supply must be positive' in str(err)


def test_symbol_exists(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    cleos.create_token(creator, max_supply)

    cleos.wait_blocks(3)

    with pytest.raises(TransactionPushError) as err:
        cleos.create_token(creator, max_supply, retries=1)

    assert 'token with symbol already exists' in str(err)


def test_create_max_possible(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    amount = (1 << 62) - 1
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    cleos.create_token(creator, max_supply)

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_create_max_possible_plus_one(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    amount = (1 << 62)
    sym = random_token_symbol()
    max_supply = f'{amount} {sym}'

    with pytest.raises(TransactionPushError) as err:
        cleos.create_token(creator, max_supply, retries=1)

    assert 'invalid supply' in str(err)

def test_create_max_decimals(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    amount = 1
    decimals = 18
    sym = random_token_symbol()
    zeros = ''.join(['0' for x in range(decimals)])
    max_supply = f'{amount}.{zeros} {sym}'

    cleos.create_token(creator, max_supply)

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.{zeros} {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator


def test_issue(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    cleos.create_token(creator, max_supply)
    issued = f'500.000 {sym}'
    cleos.issue_token(creator, issued, 'hola')

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == issued
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == issued

    issued = f'500.001 {sym}'
    with pytest.raises(TransactionPushError) as err:
        cleos.issue_token(creator, issued, 'hola', retries=1)

    assert 'quantity exceeds available supply' in str(err)

    issued = f'-1.000 {sym}'
    with pytest.raises(TransactionPushError) as err:
        cleos.issue_token(creator, issued, 'hola', retries=1)

    assert 'must issue positive quantity' in str(err)

    cleos.wait_blocks(3)

    issued = f'500.000 {sym}'
    cleos.issue_token(creator, issued, 'hola')


def test_retire(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000.000 {sym}'

    cleos.create_token(creator, max_supply)

    issued = f'500.000 {sym}'
    cleos.issue_token(creator, issued, 'hola')

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == issued
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == issued

    cleos.retire_token(creator, f'200.000 {sym}', 'hola')

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'300.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == f'300.000 {sym}'

    # should fail to retire more than current supply

    with pytest.raises(TransactionPushError) as err:
        cleos.retire_token(creator, issued, retries=1)

    assert 'overdrawn balance' in str(err)

    # transfer some tokens to friend
    friend = cleos.new_account()

    cleos.transfer_token(
        creator, friend, f'200.000 {sym}')
 
    # should fail to retire since tokens are not on the issuer's balance
    with pytest.raises(TransactionPushError) as err:
        cleos.retire_token(creator, f'300.000 {sym}', retries=1)

    assert 'overdrawn balance' in str(err)

    # give tokens back
    cleos.transfer_token(
        friend, creator, f'200.000 {sym}')

    cleos.retire_token(creator, f'300.000 {sym}', 'hola')

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == f'0.000 {sym}'
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == f'0.000 {sym}'

    # try to retire with 0 balance
    with pytest.raises(TransactionPushError) as err:
        cleos.retire_token(creator, f'1.000 {sym}', retries=1)

    assert 'overdrawn balance' in str(err)


def test_transfer(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'

    cleos.create_token(creator, max_supply)

    cleos.issue_token(creator, max_supply, 'hola')

    tkn_stats = cleos.get_token_stats(sym)

    assert tkn_stats['supply'] == max_supply
    assert tkn_stats['max_supply'] == max_supply
    assert tkn_stats['issuer'] == creator

    balance = cleos.get_balance(creator)

    assert balance == max_supply

    friend = cleos.new_account()

    cleos.transfer_token(
        creator, friend, f'300 {sym}')

    balance = cleos.get_balance(creator)
    assert balance == f'700 {sym}'

    balance = cleos.get_balance(friend)
    assert balance == f'300 {sym}'

    with pytest.raises(TransactionPushError) as err:
        cleos.transfer_token(creator, friend, f'701 {sym}', retries=1)

    assert 'overdrawn balance' in str(err)

    with pytest.raises(TransactionPushError) as err:
        cleos.transfer_token(creator, friend, f'-1 {sym}', retries=1)

    assert 'must transfer positive quantity' in str(err)


def test_open(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'
    zero = f'0 {sym}'

    cleos.create_token(creator, max_supply)

    balance = cleos.get_balance(creator)

    assert balance == None

    friend = cleos.new_account()

    with pytest.raises(TransactionPushError) as err:
        cleos.issue_token(friend, max_supply, 'hola', retries=1)

    assert 'tokens can only be issued to issuer account' in str(err)

    cleos.issue_token(creator, max_supply, 'hola')

    balance = cleos.get_balance(creator)
    assert balance == max_supply

    balance = cleos.get_balance(friend)
    assert balance == None

    with pytest.raises(TransactionPushError) as err:
        cleos.open_token('null', f'0,{sym}', creator, retries=1)

    assert 'owner account does not exist' in str(err)

    cleos.open_token(friend, f'0,{sym}', creator)

    balance = cleos.get_balance(friend)
    assert balance == zero

    transfered = f'200 {sym}'
    cleos.transfer_token(
        creator, friend, transfered)

    balance = cleos.get_balance(friend)
    assert balance == transfered

    tester = cleos.new_account()
    with pytest.raises(TransactionPushError) as err:
        cleos.open_token(tester, '0,INVALID', creator, retries=1)

    assert 'symbol does not exist' in str(err)

    with pytest.raises(TransactionPushError) as err:
        cleos.open_token(tester, f'1,{sym}', creator, retries=1)

    assert 'symbol precision mismatch' in str(err)


def test_close(cleos_bs):
    cleos = cleos_bs
    creator = cleos.new_account()
    sym = random_token_symbol()
    max_supply = f'1000 {sym}'
    zero = f'0 {sym}'

    cleos.create_token(creator, max_supply)

    balance = cleos.get_balance(creator)
    assert balance == None

    cleos.issue_token(creator, max_supply, 'hola')

    balance = cleos.get_balance(creator)
    assert balance == max_supply

    friend = cleos.new_account()
    cleos.transfer_token(
        creator, friend, max_supply)

    balance = cleos.get_balance(creator)
    assert balance == zero

    cleos.close_token(creator, f'0,{sym}')

    balance = cleos.get_balance(creator)
    assert balance == None
