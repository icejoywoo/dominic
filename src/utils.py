#!/bin/env python
# ^_^ encoding: utf-8 ^_^
""" Utility functions.
    @author: wujiabin
    @date: 2015.4.22
"""

from __future__ import absolute_import, division, print_function, with_statement
import logging
import os


logger = logging.getLogger('dominic.internal')


def limited_once(func):
    """ 限制一个方法 & 函数只被调用一次
    """

    func.__called = False

    def wrapper(*args, **kargs):
        logger.debug('func_name: %r, callled: %r,' % (func.__name__, func.__called), args, kargs)
        if func.__called:
            return None
        else:
            func.__called = True
            return func(*args, **kargs)

    return wrapper


def safeunicode(obj, encoding='utf-8'):
    r"""s
    Converts any given object to unicode string.

        >>> safeunicode('hello')
        u'hello'
        >>> safeunicode(2)
        u'2'
        >>> safeunicode('\xe1\x88\xb4')
        u'\u1234'
    """
    t = type(obj)
    if t is unicode:
        return obj
    elif t is str:
        return obj.decode(encoding, 'ignore')
    elif t in [int, float, bool]:
        return unicode(obj)
    elif hasattr(obj, '__unicode__') or isinstance(obj, unicode):
        try:
            return unicode(obj)
        except Exception as e:
            return u""
    else:
        return str(obj).decode(encoding, 'ignore')


def mkdir(p):
    try:
        os.makedirs(p)
    except:
        pass
