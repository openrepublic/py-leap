#!/usr/bin/python3

from pathlib import Path
from setuptools import setup


libs = [
    str(file) for file in Path('py_eosio').glob('**/*')
]


setup(
    name='py_eosio',
    packages=[''],
    package_dir={'': '.'},
    package_data={'': libs},
)


