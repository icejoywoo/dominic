#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/4/27
    @brief: 简易的MapReduce封装
"""

from __future__ import absolute_import, division, print_function, with_statement

import env
import partitioner
import sorter
import source


class MapReduce(object):
    """ 一个简单的单机MapReduce实现
    """

    def __init__(self, _env):
        self.env = env.Env(**_env)

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

    def _map_wrapper(self, s):
        for l in s:
            for i in self.map(l):
                yield i

    def _reduce_wrapper(self, s):
        for key, values in s:
            for i in self.reduce(key, values):
                yield i

    def run(self):
        _source = source.SourceFactory(self.env).get()

        strategy = partitioner.HashSplitStrategy()
        p = partitioner.Paritioner(self.env, self._map_wrapper(_source),
                                   int(len(_source) / self.env.mem_limit) + 1,
                                   self.env.temp_path, strategy)
        strategy.init(p.output_file_paths)

        # split files
        p()

        _sorter = sorter.Sorter(p.output_file_paths)

        _sorter.sort()

        for l in self._reduce_wrapper(_sorter):
            print(l)


if __name__ == '__main__':
    import logging
    import os
    import re
    current_path = os.path.abspath(os.path.dirname(__file__))

    logging.basicConfig()

    class MyMR(MapReduce):

        def map(self, line):
            for w in re.split(r"[\(\).,? \t;\"\"\n]+", line):
                yield w, 1

        def reduce(self, key, values):
            print(key)
            yield key, sum([int(i) for i in values])

    mr = MyMR({
        'input_path': [os.path.join(current_path, '..', 'test', 'input', 'xxx')],
        'output_path': ['test/output/xxx'],
        'temp_path': [os.path.join(current_path, '..', 'test', 'temp')],
        'name': 'wordcount',
    })
    mr.run()
