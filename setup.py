#!/usr/bin/python3

from setuptools import setup


setup(
	name='py-leap',
	version='0.1a11',
	author='Guillermo Rodriguez',
	author_email='guillermor@fing.edu.uy',
	packages=['leap'],
	install_requires=[
        'asks',
        'docker',
		'pytest',
        'natsort',
        'requests',
        'pytest-manual-marker'
	]
)
