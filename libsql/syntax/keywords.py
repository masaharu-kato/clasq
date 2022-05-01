"""
    Keywords
"""

from enum import Enum
from tkinter import CASCADE


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


class Value(KeywordABC, Enum):
    """ Where operations """
    NULL = b'NULL'
    NOT_NULL = b'NOT NULL'
    EVAL_TRUE = b'EVAL_TRUE'
    EVAL_FALSE = b'EVAL_FALSE'
    EVAL_FALSE_OR_NULL = b'EVAL_FALSE_OR_NULL'
