"""
    Values
"""
from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, get_args
import datetime

from .query import QueryABC, QueryDataABC


Date = datetime.date
Time = datetime.time
DateTime = datetime.datetime

DateLike: TypeAlias = Date | DateTime
TimeLike: TypeAlias = DateTime | Time

SQLNotNullValue: TypeAlias = bool | int | float | bytes | str | Date | Time | DateTime
SQLValue: TypeAlias = SQLNotNullValue | None  # type: ignore

# QueryParamType: TypeAlias = SQLValue 

def is_value_type(value) -> bool:
    return isinstance(value, get_args(SQLValue))
