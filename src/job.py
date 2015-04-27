#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/4/27
    @brief: 简易的MapReduce封装
"""

from __future__ import absolute_import, division, print_function, with_statement


class MapReduce(object):
    """ 一个简单的单机MapReduce实现
    """

    def __init__(self, env):
        self.env = env

    def map(self, line):
        """
        :param line: 一行数据
        :return: yield (key, value)的generator
        """
        raise NotImplemented("Not implemented yet.")

    def reduce(self, key, values):
        """
        :param key: map中输出的key
        :param values: 根据key聚合后的values
        :return:
        """
        raise NotImplemented("Not implemented yet.")

    def run(self):
        pass
