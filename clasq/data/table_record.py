"""
    Table Record classes
"""
from __future__ import annotations
from typing import get_type_hints

from ..schema.abc.table import TableABC, TableReferenceABC, TableArgs
from ..schema.abc.column import TableColumnABC, TableColumnArgs
from ..schema.table import Table
from ..schema.column import TableColumn
from ..syntax.abc.object import NameLike, ObjectName
from ..syntax.data_types import Int
from ..syntax.sql_parse import make_sql_type
from ..utils.name_conversion import camel_to_snake

from .abc.record import RecordABC
from .database import DatabaseClass


class _TableClassMeta(type, TableReferenceABC):
    """ Table Metaclass """
    

class TableClassABC(RecordABC):
    """ Table Class ABC """

class TableClass(TableClassABC, metaclass=_TableClassMeta):
    """ Table class """

    __table_name: ObjectName
    __table_obj: TableABC

    @classmethod
    def _get_entity(cls) -> TableABC:
        return cls.__table_obj

    @classmethod
    def _get_dest_table(cls) -> TableABC:
        """ Override for TableReferenceABC """
        return cls._get_entity()

    @classmethod
    def _get_name(cls) -> ObjectName:
        return cls.__table_name

    def __init_subclass__(cls, *, db: type[DatabaseClass], name: NameLike | None = None) -> None:
        
        if cls is TableClass:
            raise RuntimeError('TableClass is not specialized.')

        super().__init_subclass__()

        db_obj = db.get_entity()
        
        if name is not None:
            cls.__table_name = ObjectName(name)
        if cls.__table_name is None:
            cls.__table_name = ObjectName(camel_to_snake(cls.__name__))

        col_args: list[TableColumnArgs] = []

        for hint_name, type_hint in get_type_hints(cls).items():
            if hint_name[0:1] == '_': # TODO: fix ColumnDef for `id`
                continue
            # print('hint_name, type_hint =', hint_name, type_hint)

            nullable = False
            fkey_dest_table = None
            col_type: type[TableColumnABC]

            if not isinstance(type_hint, type):
                raise TypeError('Type hint must be a type.', type_hint)

            if issubclass(type_hint, TableColumnABC):
                col_name = hint_name
                col_type = type_hint

            elif issubclass(type_hint, TableClassABC):
                col_name = '%s_id' % hint_name
                col_type = TableColumn[Int]  # type: ignore
                fkey_dest_table = type_hint

            else:
                data_type = make_sql_type(type_hint)
                col_name = hint_name
                col_type = TableColumn[data_type]  # type: ignore

            # Get a default value
            if hasattr(cls, hint_name):
                default_value = getattr(cls, hint_name)
            else:
                default_value = None

            col_args.append(TableColumnArgs(col_name, col_type, default=default_value))

        cls.__table_obj = Table(db_obj, TableArgs(cls._get_name(), col_args))
        
        db_obj.append_table(cls.__table_obj)

        for column in cls.__table_obj.iter_table_columns():
            setattr(cls, str(column.name), column)

    def __new__(cls, *args, **kwargs):
        if cls is TableClass:
            raise RuntimeError('Cannot instantiate a TableClass directly.')
        return super().__new__()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

# class AbstractTableClass(TableClass, db=None):
#     """ Abstract table class, not a final table class """
    
#     def __init_subclass__(cls, *, db: type[DatabaseClass], name: NameLike | None = None) -> None:
#         if cls is not AbstractTableClass and AbstractTableClass not in cls.__bases__:
#             return super().__init_subclass__(db=db, name=name)


# class TableClassWithID(AbstractTableClass, db=None):
#     """ Table class with `id` column"""

#     id: AutoIncrementPrimaryTableColumn[Int]    

