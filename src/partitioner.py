#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/3/23
    @brief: 对数据源进行切分的简单实现, 当前仅支持hash策略的切割
"""

from __future__ import absolute_import, division, print_function, with_statement
import itertools
import logging
import os
import traceback
import types
try:
    import ujson as json
except ImportError:
    import warnings
    warnings.warn('Use built-in json module instead of ujson!!!')
    import json

import utils


logger = logging.getLogger("dominic.internal")


class UnsupportedTypeException(Exception):
    """ 用于表示spliter不支持的数据类型
    """
    pass


class SplitStrategy(object):
    """ 切割的策略
    """

    def __init__(self, delimiter='\0'):
        self._delimiter = delimiter
        self._output_fds = None

    def __del__(self):
        [fd.close() for fd in self._output_fds]

    def flush(self):
        for fd in self._output_fds:
            fd.flush()

    @property
    def output_fds(self):
        return self._output_fds

    def init(self, output_paths):
        """ 初始化, 注意需要先调用初始化函数
        :param output_paths:
        :return:
        """
        self._output_fds = [open(output_path, 'w') for output_path in output_paths]

    def _get_fd(self, item):
        """ 用于获取要写入的fd, 实现切分策略的地方
        :param item:
        :return:
        """
        raise NotImplemented("Not implemented yet.")

    def __call__(self, item):
        output_fd = self._get_fd(item)
        try:
            if isinstance(item, (types.ListType, types.TupleType)):
                print(self._delimiter.join(item), file=output_fd)
            elif isinstance(item, types.DictionaryType):
                print(json.dumps(item), file=output_fd)
            elif isinstance(item, types.StringTypes):
                # 注意: print会自带一个回车, 默认读入的文件数据是有\n的
                output_fd.write(utils.safeunicode(item))
            else:
                raise UnsupportedTypeException("Data type is not supported. [type={t} item={i}]"
                                               .format(t=type(item), i=item))
        except (KeyboardInterrupt, UnsupportedTypeException) as e:
            logger.warn("Unsupported action or user cancelled. [item={item} exception={exc}]"
                        .format(item=item, exc=traceback.format_exc()))
            raise e
        except:
            logger.warn("Unknown exception. [item={item} exception={exc}]"
                        .format(item=item, exc=traceback.format_exc()))


class HashSplitStrategy(SplitStrategy):
    """ hash的策略来进行数据分割
    """

    def __init__(self, key_func, delimiter='\0', hash_func=None):
        super(HashSplitStrategy, self).__init__(delimiter=delimiter)
        self._key_func = key_func
        # 默认的简单的hash方法
        self._hash_func = hash_func if hash_func else lambda x: hash(str(x))

    def _get_fd(self, item):
        key = self._key_func(item)
        hashvalue_of_key = self._hash_func(key)
        return self.output_fds[hashvalue_of_key % len(self.output_fds)]


class RRSplitStrategy(SplitStrategy):
    """ Round-Robin均衡分割策略
    """

    def __init__(self, delimiter='\0'):
        super(RRSplitStrategy, self).__init__(delimiter=delimiter)
        self._current_fd_index = 0

    def _get_fd(self, item):
        self._current_fd_index += 1
        return self.output_fds[self._current_fd_index % len(self.output_fds)]


class Paritioner(object):
    """ 切割数据源的数据
    """

    def __init__(self, source, output_count, output_paths, split_strategy, line_handler=None):
        """
        Args:
            source: 数据源, 可以通过迭代获取数据的类型即可
            output_count: 输出文件的个数
            output_paths: 输出文件的路径位置
            split_strategy: 切割策略
            line_handler: 行处理函数对象
        """
        self._source = source
        self._counter = 0
        self._line_handler = line_handler
        self._split_strategy = split_strategy
        self._is_invoked = False

        self._output_paths = output_paths
        self._output_paths_cycle = itertools.cycle(output_paths)
        self._output_count = output_count
        self._output_file_paths = [self._get_output_path(source.name)
                                   for _ in xrange(self._output_count)]

        self._check_path_existence()

    def _check_path_existence(self):
        """ 确保输出路径的存在性
        """
        for p in self._output_paths:
            utils.mkdir(p)

    @property
    def output_file_paths(self):
        return self._output_file_paths

    def _get_output_path(self, source_name):
        """ 类似Round-Robin负载均衡策略, 充分使用多个路径
        """
        self._counter += 1
        output_dir = self._output_paths_cycle.next()
        source_name = source_name.replace(' ', '_')
        return os.path.join(output_dir, "%s_%d" % (source_name, self._counter))

    def __call__(self):
        self._is_invoked = True
        for l in self._source:
            if self._line_handler:
                self._split_strategy(self._line_handler(l))
            else:
                self._split_strategy(l)
        self._split_strategy.flush()

    def __iter__(self):
        """ 迭代split后的全部数据, 没有做过多处理
        """
        if self._is_invoked:
            for file_path in self.output_file_paths:
                with open(file_path) as handle:
                    for l in handle:
                        yield l

    def __repr__(self):
        return '<Splitter source={source}>'.format(source=self._source)
