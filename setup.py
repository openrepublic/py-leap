#!/usr/bin/python3

from setuptools import setup


setup(
	name='py-leap',
	version='0.1a13',
	author='Guillermo Rodriguez',
	author_email='guillermor@fing.edu.uy',
	packages=['leap'],
	install_requires=[
        'ueosio',
        'docker',
		'pytest',
        'natsort',
        'requests',
        'zstandard',
        'pytest-manual-marker'
	],
    extra_requires={
        'async': ['asks']
    }
)
