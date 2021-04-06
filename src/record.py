""" Data record module """

from typing import get_type_hints, Type, Dict
from functools import lru_cache
from .view import DataView
from . import sqltypebases as stb


class Record(stb.Record):
    """ Record class """
    __tablename__ = None
    _dv: DataView

    @classmethod
    @lru_cache
    def _tablename(cls) -> str:
        """ Get table name of this record class """
        return cls.__tablename__ or f't_{cls.__name__.lower()}'

    @classmethod
    @lru_cache
    def _raw_column_types(cls) -> Dict[str, Type]:
        """ Get list of column name and types """
        return {
            name: t for name, t in get_type_hints(cls).items()
            if stb.SQLTypeEnv.is_compatible_type(t) and (len(name) and name[0] != '_')
        }

    @classmethod
    @lru_cache
    def _column_types(cls) -> Dict[str, Type]:
        """ Get list of column name and types (converted as a subclass of SQLType) """
        return {
            cn: stb.SQLTypeEnv.actual_type(ct, ensure_nullable=True)
            for cn, ct in cls._raw_column_types().items()
        }
    
    @classmethod
    @lru_cache
    def _create_table_sql(cls, *, exist_ok:bool=True) -> str:
        """ Get create table SQL """
        sql = ''
        if exist_ok:
            sql = f'DROP TABLE IF EXISTS `{cls._tablename()}`;\n'
        sql += f'CREATE TABLE `{cls._tablename()}`(\n' + ',\n'.join(
            f'  `{colname}` {coltype.__type_sql__()}'
            for colname, coltype in cls._column_types().items()
        ) + '\n)'
        return sql


    def __init__(self, dv, *args, **kwargs):
        # TODO: Implementation
        self._dv = dv
        for i, name, ctype in enumerate(self._column_types()):
            in_v = len(args) > i
            in_k = name in kwargs
            if in_v and in_k:
                raise RuntimeError('Duplicate value on key #{}:{}'.format(i, name))
            if in_v or in_k:
                raw_value = args[i] if in_v else kwargs[name]
            else:
                if not ctype.has_default_value():
                    raise RuntimeError('Missing value on key #{}:{} (no default values)'.format(i, name))
                raw_value = ctype.default_value()
            value = ctype(raw_value) if raw_value is not None else None
            setattr(self, name, value)


    @property
    def _db(self):
        return self._dv.db

    @property
    def _table(self):
        return self._db.table(self._tablename())

    @property
    def _raw_tables(self) -> DataView:
        return self._dv.new.tables

    @property
    def _tables(self) -> DataView:
        return self._raw_tables[self._table == self._id]
    


@lru_cache
def get_record_classes(module):
    """ Get all record classes in a specified module (python script file) """
    for name in dir(module):
        if len(name) and name[0] is not '_':
            o = getattr(module, name)
            if isinstance(o, type) and stb.is_child_class(o, Record):
                yield o

@lru_cache
def all_create_tables_sql(module) -> str:
    """ Get all create table sqls in a specified module """
    sql = ''
    for record_class in get_record_classes(module):
        sql += record_class._create_table_sql() + ';\n'
    return sql
