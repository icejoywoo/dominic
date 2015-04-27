#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/3/23
    @brief: 外排的sort实现
"""

from __future__ import absolute_import, division, print_function, with_statement
import heapq
import logging
import os
import traceback

logger = logging.getLogger("dominic.internal")


class Sorter(object):
    """ 用于打文件排序的实现
    """

    def __init__(self, file_paths, key_func, delimiter='\0', file_is_sorted=False):
        """
        Args:
            key_func: 获取key的函数, 输入为一条数据, 对于文件来说是一行文本
        """
        self._file_is_sorted = file_is_sorted
        if file_is_sorted:
            self._sorted_file_paths = file_paths
        else:
            self._sorted_file_paths = []
            self._file_paths = file_paths
        self._delimiter = delimiter
        self._key_func = key_func

        # 记录排序后的总行数和总大小
        self._total_number = 0
        self._total_size = 0

    def sort(self):
        """ 对文件进行排序, 排序的方法是按照给定的key_func来进行, 会进行内存中的排序
        内存的使用情况, 就和文件的大小相关了, 存为list, 内存占用会比文件本身大一些
        """
        if self._file_is_sorted:
            return True
        else:
            logger.debug("Start to sort files. [files={files}]".format(files=self._file_paths))
            for f in self._file_paths:
                sorted_file_path = '%s.sorted' % f
                logger.debug("Start to sort file. [original={file_path} sorted={sorted_file_path}]"
                             .format(file_path=f, sorted_file_path=sorted_file_path))
                with open(f) as handle:
                    content = handle.readlines()
                    content.sort(key=self._key_func)

                # o_前缀在这里的含义是表示original
                o_line_number = len(content)
                o_file_size = os.stat(f).st_size
                self._total_number += o_line_number
                self._total_size += o_file_size
                logger.debug("File info. [original={file_path} line_number={line_number} "
                             "size={size}]"
                             .format(file_path=f, line_number=o_line_number, size=o_file_size))

                with open(sorted_file_path, 'w') as out:
                    out.writelines(content)
                logger.debug("Succeed to sort file. [original={file_path} "
                             "sorted={sorted_file_path}]"
                             .format(file_path=f, sorted_file_path=sorted_file_path))
                self._sorted_file_paths.append(sorted_file_path)

            logger.debug("Finish to sort all files. [total_line_number={line_number} "
                         "total_size={size}]"
                         .format(line_number=self._total_number, size=self._total_size))
            self._file_is_sorted = True
            return True

    def _build_file_iterator(self, f):
        """ 构建一个file的迭代器, 返回key, 原始的line
        :param f: 文件fd
        :return: generator, 通过key_func获取的key和原始的line
        """
        with open(f) as fd:
            for l in fd:
                try:
                    yield self._key_func(l), l
                except:
                    logger.warn("Unknown exception.[line={line} exception={exc}]"
                                .format(line=l, exc=traceback.format_exc()))

    def __repr__(self):
        return '<Sorter id={_id}>'.format(_id=id(self))

    def __iter__(self):
        """ 需要先调用sort来进行排序, 才可以进行遍历
        """
        if self._file_is_sorted:
            emitted_counter = 0
            iterables = [self._build_file_iterator(f) for f in self._sorted_file_paths]
            for _, value in heapq.merge(*iterables):
                emitted_counter += 1
                if emitted_counter % 100000 == 0:
                    logger.debug("Emit item in sorter. [sorter={sorter} emitted={emitted} "
                                 "process={process:.2f}%]"
                                 .format(sorter=self, emitted=emitted_counter,
                                         process=100.0 * emitted_counter / self._total_number))
                yield value
            logger.debug("Exhauseted iterator in sorter. [total_line_number={line_number} "
                         "total_size={size}]"
                         .format(line_number=self._total_number, size=self._total_size))
        else:
            raise ValueError("Not sorted.")
