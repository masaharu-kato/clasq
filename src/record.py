""" Data record module """

from typing import get_type_hints, Type, Dict, Optional
from functools import lru_cache
from . import sqltypebases as stb

class Record(stb.Record):
    """ Record class """
    __tablename__ = None

    @classmethod
    @lru_cache
    def tablename(cls) -> str:
        return cls.__tablename__ or f't_{cls.__name__.lower()}'

    @classmethod
    @lru_cache
    def column_types(cls) -> Dict[str, Type]:
        return {
            **stb.RecordEnv.common_columns,
            **{cn: stb.RecordEnv.final_type(ct) for cn, ct in get_type_hints(cls).items()},
        }
    
    @classmethod
    @lru_cache
    def create_table_sql(cls, *, exist_ok:bool=True) -> str:
        sql = ''
        if exist_ok:
            sql = f'DROP TABLE IF EXISTS `{cls.tablename()}`;\n'
        sql += f'CREATE TABLE `{cls.tablename()}`(\n' + ',\n'.join(
            f'  `{colname}` {coltype.__type_sql__()}'
            for colname, coltype in cls.column_types().items()
        ) + '\n)'
        return sql


@lru_cache
def get_record_classes(module):
    for name in dir(module):
        o = getattr(module, name)
        if isinstance(o, type) and issubclass(o, Record) and o is not Record:
            yield o

@lru_cache
def all_create_tables_sql(module) -> str:
    sql = ''
    for record_class in get_record_classes(module):
        sql += record_class.create_table_sql() + ';\n'
    return sql
