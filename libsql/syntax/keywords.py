"""
    Keywords
"""

from enum import Enum


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

