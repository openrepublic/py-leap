#!/usr/bin/env python3

h = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1e1f20'

inv_h = '201f1e1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100'

def test_wait_start(cleos_w_indextest):
    cleos = cleos_w_indextest
    print("\n" + cleos.endpoint)
    print("eosio: ", cleos.private_keys['eosio'])
    print("cindextest: ", cleos.private_keys['cindextest'])
    print("rindextest: ", cleos.private_keys['rindextest'])
    breakpoint()


def test_load_storage_only(cleos_w_indextest):
    cleos = cleos_w_indextest

    # store in rust contract
    cleos.push_action(
        'rindextest',
        'store',
        [0, h],
        'rindextest'
    )

    # store in c contract
    cleos.push_action(
        'cindextest',
        'store',
        [0, h],
        'cindextest'
    )



def test_rust_storage(cleos_w_indextest):
    cleos = cleos_w_indextest

    # read from rust
    trx = cleos.push_action(
        'rindextest',
        'rload',
        [h],
        'rindextest'
    )
    rh = trx['processed']['action_traces'][0]['console']
    assert rh == h + '\n'

    # read from c
    trx = cleos.push_action(
        'cindextest',
        'rload',
        [h],
        'cindextest'
    )
    ch = trx['processed']['action_traces'][0]['console']
    assert ch == h + '\n'


def test_c_storage(cleos_w_indextest):
    cleos = cleos_w_indextest

    # read from c
    trx = cleos.push_action(
        'cindextest',
        'cload',
        [h],
        'cindextest'
    )
    ch = trx['processed']['action_traces'][0]['console']
    assert ch == h + '\n'

    # read from rust
    trx = cleos.push_action(
        'rindextest',
        'dump',
        [],
        'rindextest'
    )
    dump = trx['processed']['action_traces'][0]['console']
    print(f'dump: {dump}')

    trx = cleos.push_action(
        'rindextest',
        'cload',
        [h],
        'rindextest'
    )
    rh = trx['processed']['action_traces'][0]['console']
    assert rh == h + '\n'


def test_wait_end(cleos_w_indextest):
    breakpoint()
