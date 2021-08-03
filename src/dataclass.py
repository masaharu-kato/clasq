""" Data record module """

from typing import List, get_type_hints, Type, Dict, Optional
from functools import lru_cache
from .view import DataView
from . import sqltypebases as stb
from . import sqltypes as st
from . import schema


class Record(stb.Record):
    """ Record class """
    __tablename__ = None
    _dv: DataView

    @classmethod
    @lru_cache
    def _tablename(cls) -> str:
        """ Get table name of this record class """
        return cls.__tablename__ or 't_' + cls.__name__.split('.')[-1].lower()

    @classmethod
    @lru_cache
    def _raw_column_types(cls) -> Dict[str, Type]:
        """ Get list of column name and types """
        coltypes = {}
        for name, t in get_type_hints(cls).items():
            if not name or name[0] == '_':
                continue
            if not stb.SQLTypeEnv.is_compatible_column_type(t):
                raise RuntimeError('Type of column `{}` ({}) is not compatible.'.format(name, t))
            coltypes[name] = t
        return coltypes

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
    def _table(cls) -> schema.Table:
        """ Get table object (in schema module) """
        return schema.Table(
            cls._tablename(),
            [
                schema.Column(
                    colname,
                    coltype.__type_sql__(),
                    not_null = issubclass(coltype, st.NotNull),
                    is_primary = issubclass(coltype, st.PrimaryKey),
                    comment = coltype.get_comment(),
                    link_column_refs = [coltype.get_foreign_key()._tablename()] if coltype.has_foreign_key() else None,
                )
                for colname, coltype in cls._column_types().items()
            ],
            record_class = cls
        )


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
    def _raw_tables(self) -> DataView:
        return self._dv.new.tables

    @property
    def _tables(self) -> DataView:
        return self._raw_tables[self._table == self._id]
    

class RootRecord(Record):
    """ Root record (for common columns) """


class Database:
    __dbname__ : Optional[str] = None

    @classmethod
    @lru_cache
    def dbname(cls):
        """ Get database name of this record class """
        return cls.__dbname__ or 'db_' + cls.__name__.split('.')[-1].lower()

    @classmethod
    @lru_cache
    def table_by_names(cls) -> Dict[str, Type]:
        """ Get list of table name and types """
        tbltypes = {}
        for name, t in get_type_hints(cls).items():
            if not name or name[0] == '_':
                continue
            if not stb.SQLTypeEnv.is_compatible_table_type(t):
                raise RuntimeError('Type of table `{}` ({}) is not compatible.'.format(name, t))
            tbltypes[name] = stb.SQLTypeEnv.table_basetype(t)
        return tbltypes

    @classmethod
    def tables(cls) -> List[Type]:
        return list(tbltypes.values())

    @classmethod
    @lru_cache
    def _db(cls) -> schema.Database:
        """ Get all create table sqls in a specified module """
        return schema.Database(
            cls.dbname(),
            [reccls._table() for reccls in cls.tables()],
            finalize=True
        )
