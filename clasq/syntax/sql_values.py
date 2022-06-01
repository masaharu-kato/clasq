"""
    SQL Values
"""
from __future__ import annotations

import datetime

Date = datetime.date
Time = datetime.time
DateTime = datetime.datetime

DateLike = Date | DateTime
TimeLike = DateTime | Time

SQLNotNullValue = bool | int | float | bytes | str | Date | Time | DateTime
SQLValue = SQLNotNullValue | None  # type: ignore
