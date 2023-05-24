#!/usr/bin/env python3

import json

from leap.sugar import random_leap_name
from leap.cleos import default_nodeos_image

# TODO: better testing, as right now we dont have multi producer setup

def test_wrap_exec_direct(cleos):
    quantity = '10.0000 TLOS'
    worker = cleos.new_account()

    ec, tx = cleos.push_action(
        'eosio.token',
        'transfer',
        ['eosio', worker, quantity, ''],
        'eosio@active',
        dump_tx=True,
        sign=False
    )
    assert ec == 0

    tx['ref_block_num'] = 0
    tx['ref_block_prefix'] = 0
    tx['context_free_actions'] = []

    if 'eosio-2.1.0' in default_nodeos_image():
        # for eosio 2.1.0 handle diferent tx format
        tx['transaction_extensions'] = []
        tx['actions'][0]['data'] = tx['actions'][0]['hex_data']
        del tx['actions'][0]['hex_data']

    ec, tx = cleos.wrap_exec(
        'eosio.wrap',
        tx,
        dump_tx=True,
        sign=False
    )
    assert ec == 0

    tx['ref_block_num'] = 0
    tx['ref_block_prefix'] = 0
    tx['context_free_actions'] = []

    ec, out = cleos.push_transaction(tx)
    cleos.logger.info(out)
    assert ec == 0

    cleos.wait_blocks(3)

    balance = cleos.get_balance(worker)
    assert balance == quantity


# def test_wrap_exec_msig(cleos):
#     quantity = '10.0000 TLOS'
#     worker = cleos.new_account()
# 
#     ec, tx = cleos.push_action(
#         'eosio.token',
#         'transfer',
#         ['eosio', worker, quantity, ''],
#         'eosio@active',
#         dump_tx=True
#     )
#     assert ec == 0
# 
#     tx['ref_block_num'] = 0
#     tx['ref_block_prefix'] = 0
#     tx['context_free_actions'] = []
# 
#     ec, tx = cleos.wrap_exec(worker, tx, dump_tx=True)
#     assert ec == 0
# 
#     tx['ref_block_num'] = 0
#     tx['ref_block_prefix'] = 0
#     tx['context_free_actions'] = []
# 
#     proposal = cleos.multi_sig_propose_tx(
#         worker,
#         [f'{worker}@active', 'eosio@active'],
#         tx
#     )
# 
#     for name in (worker, 'eosio'):
#         ec, _ = cleos.multi_sig_approve(
#             worker,
#             proposal,
#             [f'{name}@active'],
#             name
#         )
#         assert ec == 0
# 
#     ec, _ = cleos.multi_sig_exec(
#         worker,
#         proposal,
#         f'{worker}@active'
#     )
#     assert ec == 0
# 
#     balance = cleos.get_balance(worker)
#     assert balance == quantity
