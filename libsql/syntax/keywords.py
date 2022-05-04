"""
    Keywords
"""

from enum import Enum
from tkinter import CASCADE
from typing import Union


class KeywordABC:
    """ Keyword abstract class """


class OrderType(KeywordABC, Enum):
    ASC = b'ASC'
    DESC = b'DESC'


class JoinType(KeywordABC, Enum):
    INNER = b'INNER'
    LEFT  = b'LEFT'
    RIGHT = b'RIGHT'
    OUTER = b'OUTER'
    CROSS = b'CROSS'


class ReferenceOption(KeywordABC, Enum):
    RESTRICT = b'RESTRICT'
    CASCADE = b'CASCADE'
    SET_NULL = b'SET_NULL'
    NO_ACTION = b'NO_ACTION'


JoinLike = Union[JoinType, bytes, str]

def make_join_type(val: JoinLike):
    if isinstance(val, JoinType):
        return val
    if isinstance(val, str):
        return JoinType(val.encode().upper())
    if isinstance(val, bytes):
        return JoinType(val.upper())
    raise TypeError('Invalid type %s (%s)' % (type(val), val))
