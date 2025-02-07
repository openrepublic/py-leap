import json

def test_generate(cleos_w_bootstrap):
    cleos = cleos_w_bootstrap

    with open(cleos.node_dir / 'keys.json', 'w+') as key_file:
        accounts = list(cleos.private_keys.keys())
        key_file.write(json.dumps({
            account: (cleos.private_keys[account], cleos.keys[account])
            for account in accounts
        }, indent=4))

    cleos.wait_blocks(1)
