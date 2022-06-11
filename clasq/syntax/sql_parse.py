"""

"""
from __future__ import annotations
import re
import datetime
import typing
from .abc.data_types import AnyDataType, DataTypeABC
from . import data_types as dt

DATA_TYPE_FROM_SQL_BASE_NAME: dict[bytes, type[DataTypeABC]] = {}
for cls in dt.ALL_TYPES:
    name = cls.get_sql_base_name()
    if name not in DATA_TYPE_FROM_SQL_BASE_NAME:
        DATA_TYPE_FROM_SQL_BASE_NAME[name] = cls

__RE_SQL_TYPE = re.compile(r'(?P<base>\w+)\s*(\((?P<args>[\w\s,]*)\))?\s*(?P<base_options>\w+)?')
__RE_SQL_INT = re.compile(r'-?\d+')

def data_type_from_sql(sql: str) -> type[DataTypeABC]:
    if m := __RE_SQL_TYPE.fullmatch(sql):
        base_name: bytes = m['base'].encode('ascii').upper()
        if base_options := m['base_options']:
            base_name += b' ' + base_options.encode('ascii').upper()
        try:
            base_type = DATA_TYPE_FROM_SQL_BASE_NAME[base_name]  # type: ignore
        except KeyError as e:
            raise ValueError('Unknown sql type base name', base_name) from e
        params = tuple(__parse_sql_arg(s) for s in m['args'].split(',')) if m['args'] else ()
        literals = tuple(typing.Literal[v] for v in params) # type: ignore
        if params:
            try:
                return base_type[literals] 
            except TypeError:  # Not a generic type, invalid arguments etc.
                pass
        return base_type  
    # raise ValueError('Invalid syntax of SQL.', sql)
    return AnyDataType


def __parse_sql_arg(text: str) -> int | str:
    text = text.strip()
    if __RE_SQL_INT.fullmatch(text):
        return int(text)
    return text


def make_sql_type(typelike: type) -> type[DataTypeABC]:

    _origin = typing.get_origin(typelike)

    # if typelike is _type_ | None or None | _type_ (Union or Optional)
    if _origin is typing.Union:
        _args = typing.get_args(typelike)
        if (len(_args) < 2 and (_args[0] is None or _args[1] is None)):
            if (_base := _args[0] if _args[0] is not None else _args[1]) is not None:
                if isinstance(_base, type) or typing.get_origin(_base):
                    return Nullable[_base]  # type: ignore

    elif isinstance(typelike, type):
        if issubclass(typelike, DataTypeABC):
            return typelike

        if typelike is datetime.datetime:
            return dt.DateTime
        if typelike is datetime.date:
            return dt.Date
        if typelike is datetime.time:
            return dt.Time
        if typelike is str:
            return dt.Text
        if typelike is bytes:
            return dt.Blob
        if typelike is float:
            return dt.Double
        if typelike is int:
            return dt.Int
        if typelike is bool:
            return dt.Bool

    raise ValueError('Invalid type for SQL type.', typelike)
