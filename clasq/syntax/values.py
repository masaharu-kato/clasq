"""
    Values
"""
from __future__ import annotations
from typing import TYPE_CHECKING, get_args

from .abc.query import QueryABC
from . import sql_values

if TYPE_CHECKING:
    from .query_data import QueryData

class NullType(QueryABC):
    """ SQL NULL Type """

    def append_to_query_data(self, qd: QueryData) -> None:
        qd.append(b'NULL')


NULL = NullType()

ValueType = sql_values.SQLNotNullValue | NullType


def is_value_type(value) -> bool:
    return isinstance(value, get_args(ValueType))
