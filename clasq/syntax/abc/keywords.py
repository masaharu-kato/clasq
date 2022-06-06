"""
    Keywords
"""
from __future__ import annotations
from abc import ABCMeta
from enum import Enum, EnumMeta

from .query import QueryABC, QueryDataABC


class ABCEnumMeta(EnumMeta, ABCMeta):
    """ EnumMeta + ABCMeta """

class KeywordABC(QueryABC, Enum, metaclass=ABCEnumMeta):
    """ Keyword abstract class """

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        qd.append_one(self.value)

    @classmethod
    def _make(cls, val):
        if isinstance(val, cls):
            return val
        if isinstance(val, str):
            return cls(val.encode().upper())
        if isinstance(val, bytes):
            return cls(val.upper())
        raise TypeError('Invalid type %s (%s)' % (type(val), val))


class OrderType(KeywordABC):
    ASC = b'ASC'
    DESC = b'DESC'

    @classmethod
    def make(cls, val) -> OrderType:
        if val is True:
            return OrderType.ASC
        if val is False:
            return OrderType.DESC
        return super()._make(val)


class JoinType(KeywordABC):
    INNER = b'INNER'
    LEFT  = b'LEFT'
    RIGHT = b'RIGHT'
    OUTER = b'OUTER'
    CROSS = b'CROSS'

    @classmethod
    def make(cls, val) -> JoinType:
        return super()._make(val)


class ReferenceOption(KeywordABC):
    RESTRICT = b'RESTRICT'
    CASCADE = b'CASCADE'
    SET_NULL = b'SET_NULL'
    NO_ACTION = b'NO_ACTION'

    @classmethod
    def make(cls, val) -> ReferenceOption:
        return super()._make(val)

OrderTypeLike = OrderType | bool | bytes | str
JoinLike = JoinType | bytes | str
RefOptionLike = ReferenceOption | bytes | str
