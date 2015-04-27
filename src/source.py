#!/bin/env python
# ^_^ encoding: utf-8 ^_^
"""
    @author: icejoywoo@gmail.com
    @date: 2015/3/23
    @brief: 几种数据源的实现, 存储的一些封装, 让不同格式的数据有统一的展现形式
"""

from __future__ import absolute_import, division, print_function, with_statement
import itertools
import logging
import os
import traceback
import urlparse


logger = logging.getLogger("dominic.internal")


class Source(object):
    """ 一个获取数据的源, 抽象封装几种方法用来支持多种源的扩展
    """

    def __init__(self, name, line_handler=None):
        """
        Args:
            line_handler: function, 处理行的函数, 接受一个参数line, 返回处理后的结果(tuple或list)
                          line的类型根据不同的数据源有所不同
        """
        self._line_handler = line_handler
        self._current_size = 0
        self._current_length = 0
        self._name = name

    @property
    def length(self):
        """ 获取源的数据个数, 例如文件的行数等
        """
        raise NotImplemented("Not implemented yet.")

    @property
    def size(self):
        """ 获取源的大小, 例如文件的大小
        """
        raise NotImplemented("Not implemented yet.")

    def _iterate(self):
        """ 返回下一条数据, 实现iterator, 如果没有数据了请raise StopIteration
        """
        raise NotImplemented("Not implemented yet.")

    def _get_size(self, line):
        """ 返回本次处理的字节数
        """
        raise NotImplemented("Not implemented yet.")

    @property
    def name(self):
        """ 数据源的名字
        """
        return self._name

    @property
    def process(self):
        """ 显示进度
        """
        return self.current_size * 100.0 / len(self)

    @property
    def process_details(self):
        """ 当前处理进度的详情
        """
        return {
            'current_size': self.current_size,
            'current_length': self.current_length,
            'total_size': len(self),
            'total_length': None,
        }

    @property
    def current_size(self):
        """ 当前处理了的字节数
        """
        return self._current_size

    @property
    def current_length(self):
        """ 当前处理了的条目数或行数
        """
        return self._current_length

    def __len__(self):
        """ 需要子类选择一个方式来实现, 用于表示处理的进度
        """
        raise NotImplemented("Not implemented yet.")

    def __str__(self):
        return '<Source name={name}>'.format(name=self._name)

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        """ 通过迭代器获取内部数据
        """
        for line in self._iterate():
            self._current_size += self._get_size(line)
            self._current_length += 1
            if self._current_length % 100000 == 0:
                logger.debug("Iterate the source. [source={source} length={length} "
                             "process={process:.2f}%]"
                             .format(source=self, length=self._current_length,
                                     process=self.process))
            if self._line_handler:
                try:
                    yield self._line_handler(line)
                except TypeError:
                    logger.warn("The return arguments of line_handler does not match the "
                                "column names. [line={line} exception={exc}]"
                                .format(exc=traceback.format_exc(), line=line))
                except KeyboardInterrupt as e:
                    logger.warn("User cancelled. [line={line} exception={exc}]"
                                .format(exc=traceback.format_exc(), line=line))
                    raise e
                except:
                    logger.warn("Unknown exception. [line={line} exception={exc}]"
                                .format(exc=traceback.format_exc(), line=line))
            else:
                yield line
        logger.debug("Finish to iterate source. [source={source} length={length} "
                     "process={process:.2f}%]"
                     .format(source=self, length=self._current_length, process=self.process))


class FileSource(Source):
    """ 文件源封装
    """

    def __init__(self, name, file_paths, line_handler=None):
        super(FileSource, self).__init__(name=name, line_handler=line_handler)
        self._file_paths = file_paths
        self._file_handles = []
        self._file_size = 0
        for file_path in file_paths:
            if os.path.isfile(file_path):
                self._file_handles.append(open(file_path, 'r'))
                self._file_size += os.stat(file_path).st_size
            else:
                raise ValueError("File does not exist. [file_path={file_path}]"
                                 .format(file_path=file_path))

    def __del__(self):
        for handle in self._file_handles:
            handle.close()

    @property
    def size(self):
        return self._file_size

    def __len__(self):
        return self.size

    def __str__(self):
        return '<FileSource name={name} file_paths={file_paths}>'\
            .format(name=self._name, file_paths=self._file_paths)

    def _iterate(self):
        """ 使用file按行迭代
        """
        return itertools.chain(*self._file_handles)

    def _get_size(self, line):
        return len(line)


class MongoDBSource(Source):
    """ MongoDB数据源
    """

    def __init__(self, name, mongo_uri, collection, database=None, query=None, line_handler=None):
        """
        Args:
            name: 数据源名称
            mongo_uri: mongodb的连接字符串
                参考: http://docs.mongodb.org/manual/reference/connection-string/
            database: 这项配置比mongo_uri中的database优先级高, 二者都存在的时候, 以这项配置为准
            collection: 集合名称
            query: 是针对collecton.find这个方法的参数来进行配置, 可以配置
                参考: https://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        import pymongo
        super(MongoDBSource, self).__init__(name=name, line_handler=line_handler)
        self._mongo_uri = mongo_uri
        self._mongo_client = pymongo.MongoClient(self._mongo_uri)

        parsed_uri = urlparse.urlparse(mongo_uri)
        if database:
            self._database = database
        else:
            self._database = parsed_uri.path[1:]
        self._collection = collection
        self._db = self._mongo_client[self._database]
        if query is None:
            query = {}
        _filter = query.pop('filter', {})
        projection = query.pop('projection', {})
        self._cursor = self._db[self._collection].find(_filter, projection,
                                                       no_cursor_timeout=True, **query)

        self._count = self._cursor.count(with_limit_and_skip=True)

        # get collstats for the collection
        collstats = self._db.command('collstats', self._collection)
        self._avg_obj_size = collstats['avgObjSize']
        self._total_size = self._avg_obj_size * self._count

    def __del__(self):
        self._cursor.close()

    def __len__(self):
        return self._total_size

    def __str__(self):
        return '<MongoDBSource name={name} uri={file_paths} collection={coll} db={db}>'\
            .format(name=self._name, uri=self._mongo_uri, coll=self._collection, db=self._database)

    def _iterate(self):
        return self._cursor

    @property
    def size(self):
        return self._total_size

    def _get_size(self, _):
        return self._avg_obj_size


class SourceFactory(object):
    """ 根据env来构建对应的数据源
    """

    mappings = {
        'file': FileSource,
        'mongo': MongoDBSource,
    }

    def __init__(self, env):
        self._env = env
        self._builder = self.mappings[self._env._input_type]
        self._kwargs = getattr(self, '_get_%s_kwargs' % self._env._input_type)()

    def _get_file_kwargs(self):
        return {
            'name': self._env.name,
            'file_paths': self._env.input_path,
        }

    def _get_mongo_kwargs(self):
        return {
            'name': self._env.name,
            'mongo_uri': self._env.mongo_uri,
            'collection': self._env.collection,
            'database': self._env.database,
            'query': self._env.query,
        }

    def get(self):
        return self._builder(**self._kwargs)
