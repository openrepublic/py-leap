#!/usr/bin/python3

from setuptools import setup


setup(
	name='py-eosio',
	version='0.1a6',
	author='Guillermo Rodriguez',
	author_email='guillermor@fing.edu.uy',
	packages=['py_eosio'],
	install_requires=[
        'docker',
		'pytest',
        'natsort',
        'requests'
	]
)
