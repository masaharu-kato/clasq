"""
    Keywords
"""

from enum import Enum


class OrderType(Enum):
    ASC = 'ASC'
    DESC = 'DESC'


class JoinType(Enum):
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    OUTER = 'OUTER'
    CROSS = 'CROSS'
