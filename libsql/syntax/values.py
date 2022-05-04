"""
    Values
"""
from enum import Enum
from typing import Union, get_args
import datetime

from .keywords import KeywordABC

Date = datetime.date
Time = datetime.time
DateTime = datetime.datetime

DateLike = Union[Date, DateTime]
TimeLike = Union[DateTime, Time]

class Value(KeywordABC, Enum):
    """ Special values """
    NULL = b'NULL'
    # NOT_NULL = b'NOT NULL'
    # EVAL_TRUE = b'EVAL_TRUE'
    # EVAL_FALSE = b'EVAL_FALSE'
    # EVAL_FALSE_OR_NULL = b'EVAL_FALSE_OR_NULL'

NULL = Value.NULL


ValueType = Union[bool, int, float, bytes, str, Date, Time, DateTime, Value]

def is_value_type(value) -> bool:
    return isinstance(value, get_args(ValueType))
