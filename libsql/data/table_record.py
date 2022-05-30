"""
    Table Record classes
"""
from abc import ABC, abstractmethod, abstractproperty
from typing import TYPE_CHECKING, Dict, Generic, List, Mapping, Optional, Type, TypeVar, get_args, get_origin, get_type_hints

from ..schema.table import Table, TableArgs
from ..schema.column_abc import TableColumnABC
from ..schema.column import ColumnArgs
from ..schema.sqltypes import Int, make_sql_type
from ..schema.sqltype_abc import SQLTypeABC
from ..syntax.exprs import ExprLike
from ..syntax.values import NULL
from ..utils.name_conversion import camel_to_snake

from .record_abc import RecordABC
from .database import DatabaseClass

if TYPE_CHECKING:
    from ..syntax.exprs import ExprObjectABC
    from ..syntax.object_abc import ObjectName
    from ..syntax.object_abc import NameLike



class TableClass(RecordABC, DatabaseClass):
    """ Table class """
    _table_obj: Table

    @classmethod
    def _table_name(cls) -> 'NameLike':
        if cls is TableClass:
            raise RuntimeError('TableClass is not specialized.')
        return camel_to_snake(cls.__name__)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        col_args = [ColumnArgs('id', Int, primary=True, auto_increment=True)]

        for hint_name, type_hint in get_type_hints(cls).items():
            if hint_name[0:1] == '_':
                continue

            print('hint_name, type_hint =', hint_name, type_hint)

            nullable = False
            fkey_dest_table = None
            col_type: Type[SQLTypeABC]

            # Get an actual column type
            if get_origin(type_hint) is ColumnDef:
                type_args = get_args(type_hint)
                assert len(type_args) == 1
                actual_type_like = type_args[0]
            else:
                actual_type_like = type_hint

            print('actual_type_like=', actual_type_like)

            if isinstance(actual_type_like, type) and issubclass(actual_type_like, TableClass):
                col_name = '%s_id' % hint_name
                col_type = Int
                fkey_dest_table = actual_type_like

            else:
                actual_type = make_sql_type(actual_type_like)
                print('actual type=', actual_type)
                actual_type_origin = get_origin(actual_type) or actual_type
                # Calc a name and type for column object
                if not issubclass(actual_type_origin, SQLTypeABC):
                    raise TypeError('Invalid type of column.', actual_type)

                col_name = hint_name
                col_type = actual_type

            # Get a default value
            default_value: Optional[ExprLike] = None
            if hasattr(cls, hint_name):
                _v = getattr(cls, hint_name)
                default_value = NULL if _v is None else _v
                if default_value is NULL:
                    nullable = True

            col_args.append(ColumnArgs(name=col_name, sql_type=col_type, default=default_value, nullable=nullable))

        cls._table_obj = Table(cls._db_obj, TableArgs(cls._table_name(), *col_args))
            
    @property
    def table(self) -> 'Table':
        """ Get a parent table object """
        return self._table_obj

    def __new__(cls, *args, **kwargs):
        if cls is TableClass:
            raise RuntimeError('Cannot instantiate a TableClass directly.')
        return super().__new__()
    

T = TypeVar('T')
class ColumnDef(TableColumnABC, Generic[T]):
    """ Column Definition """
