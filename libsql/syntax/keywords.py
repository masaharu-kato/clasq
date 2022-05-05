"""
    Keywords
"""

from enum import Enum
from tkinter import CASCADE
from typing import Union


class KeywordABC:
    """ Keyword abstract class """

    @classmethod
    def _make(cls, val):
        if isinstance(val, cls):
            return val
        if isinstance(val, str):
            return cls(val.encode().upper())
        if isinstance(val, bytes):
            return cls(val.upper())
        raise TypeError('Invalid type %s (%s)' % (type(val), val))


class OrderType(KeywordABC, Enum):
    ASC = b'ASC'
    DESC = b'DESC'

    @classmethod
    def make(cls, val) -> 'OrderType':
        if val is True:
            return OrderType.ASC
        if val is False:
            return OrderType.DESC
        return super()._make(val)


class JoinType(KeywordABC, Enum):
    INNER = b'INNER'
    LEFT  = b'LEFT'
    RIGHT = b'RIGHT'
    OUTER = b'OUTER'
    CROSS = b'CROSS'

    @classmethod
    def make(cls, val) -> 'JoinType':
        return super()._make(val)


class ReferenceOption(KeywordABC, Enum):
    RESTRICT = b'RESTRICT'
    CASCADE = b'CASCADE'
    SET_NULL = b'SET_NULL'
    NO_ACTION = b'NO_ACTION'

    @classmethod
    def make(cls, val) -> 'ReferenceOption':
        return super()._make(val)

OrderLike = Union[OrderType, bool, bytes, str]
JoinLike = Union[JoinType, bytes, str]
RefOptionLike = Union[ReferenceOption, bytes, str]
