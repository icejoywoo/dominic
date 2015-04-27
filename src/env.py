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

    @property
    def input_path(self):
        return self._input_path

    @property
    def output_path(self):
        return self._output_path

    @property
    def temp_path(self):
        return self._temp_path
