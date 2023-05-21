#!/usr/bin/env python3

import tempfile

from pathlib import Path

from leap.sugar import *


def test_docker_move_file(cleos):
    client = cleos.client

    f_content = random_string(size=1024) 
    f_name = 'message.txt'
    f_path = Path(f_name) 
    with open(f_path, 'w') as file:
        file.write(f_content)

    docker_move_into(client, cleos.vtestnet, f_path, '/tmp')

    ec, out = cleos.run(['cat', f'/tmp/{f_name}'])
    assert ec == 0
    assert f_content == out.rstrip()

    f_path.unlink()


def test_docker_move_dir(cleos):
    client = cleos.client

    d_name = 'test_directory'
    d_path = Path(d_name)
    d_path.mkdir(exist_ok=True)
    f_content = random_string(size=1024) 
    f_name = 'message.txt'
    f_path = d_path / f_name 
    with open(f_path, 'w') as file:
        file.write(f_content)

    docker_move_into(client, cleos.vtestnet, d_path, '/tmp')

    ec, out = cleos.run(['ls', f'/tmp/{d_name}'])
    assert ec == 0
    assert f_name == out.rstrip()

    ec, out = cleos.run(['cat', f'/tmp/{d_name}/{f_name}'])
    assert ec == 0
    assert f_content == out.rstrip()

    f_path.unlink()
    d_path.rmdir()
