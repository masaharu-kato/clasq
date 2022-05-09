"""
    SQL Values
"""
from typing import Union

import datetime

Date = datetime.date
Time = datetime.time
DateTime = datetime.datetime

DateLike = Union[Date, DateTime]
TimeLike = Union[DateTime, Time]

SQLNotNullValue = Union[bool, int, float, bytes, str, Date, Time, DateTime]
SQLValue = Union[SQLNotNullValue, None]
