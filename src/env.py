#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/4/27
    @brief: 内部环境管理
"""

from __future__ import absolute_import, division, print_function, with_statement
import logging
import os
import tempfile
import time

logger = logging.getLogger('dominic.internal')


class Env(object):

    def __init__(self, **kwargs):

        if 'input_path' in kwargs:
            self._input_path = kwargs.pop('input_path')
        else:
            logger.error("Input path is empty!")

        if 'output_path' in kwargs:
            self._output_path = kwargs.pop('output_path')
        else:
            logger.error("Output path is empty!")

        if 'temp_path' in kwargs:
            self._temp_path = kwargs.pop('temp_path')
        else:
            self._temp_path = os.path.join(tempfile.gettempdir(), 'dominic_%d' % int(time.time()))

        if 'name' in kwargs:
            self._name = kwargs.pop('name')
        else:
            logger.error("name is empty!")

        if 'input_type' in kwargs:
            self._input_type = kwargs.pop('input_type')
        else:
            self._input_type = 'file'

        if 'mem_limit' in kwargs:
            self._mem_limit = kwargs.pop('mem_limit')
        else:
            self._mem_limit = 100 * 1024 * 1024

    @property
    def input_path(self):
        return self._input_path

    @property
    def output_path(self):
        return self._output_path

    @property
    def temp_path(self):
        return self._temp_path

    @property
    def name(self):
        return self._name

    @property
    def mem_limit(self):
        return self._mem_limit
