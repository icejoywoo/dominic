#!/bin/env python
# ^_^ encoding: utf-8 ^_^

from distutils.core import setup

setup(
    name='dominic',
    version='0.0.1',
    author='Eric.W',
    author_email='icejoywoo@gmail.com',
    description="This is an simple embedded storage engine.",
    install_requires=[
        'mmhash',
        'hash_ring',
    ],
    packages=['dominic'],
)
