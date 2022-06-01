"""
    Table Record classes
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Generic, Type, TypeVar, get_args, get_origin, get_type_hints


from ..schema.abc.table import TableReferenceABC
from ..schema.abc.column import TableColumnABC
from ..schema.abc.sqltype import SQLTypeABC
from ..schema.table import Table, TableArgs
from ..schema.column import ColumnArgs
from ..schema.sqltypes import Int, make_sql_type
from ..syntax.exprs import ExprLike
from ..syntax.values import NULL
from ..utils.name_conversion import camel_to_snake

from .abc.record import RecordABC
from .database import DatabaseClass

if TYPE_CHECKING:
    from ..syntax.abc.object import NameLike


class _TableClassMeta(type, TableReferenceABC):
    """ Table Metaclass """


class TableClassABC(RecordABC):
    """ Table Class ABC """


class TableClass(TableClassABC, metaclass=_TableClassMeta):
    """ Table class """

    _table_name: str | None = None
    __table_obj: Table

    id: ColumnDef

    @classmethod
    def get_entity(cls) -> Table:
        return cls.__table_obj

    @classmethod
    def _get_table_name(cls) -> NameLike:
        if cls is TableClass:
            raise RuntimeError('TableClass is not specialized.')
        if cls._table_name:
            return cls._table_name
        return camel_to_snake(cls.__name__)

    def __init_subclass__(cls, *, db: Type['DatabaseClass'], name: str | None = None) -> None:
        super().__init_subclass__()
        db_obj = db.get_entity()
        if name is not None:
            cls._table_name = name

        col_args = [ColumnArgs('id', Int, primary=True, auto_increment=True)]

        for hint_name, type_hint in get_type_hints(cls).items():
            if hint_name[0:1] == '_' or hint_name == 'id': # TODO: fix ColumnDef for `id`
                continue
            # print('hint_name, type_hint =', hint_name, type_hint)

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
            # print('actual_type_like=', actual_type_like)

            if isinstance(actual_type_like, type) and issubclass(actual_type_like, TableClassABC):
                col_name = '%s_id' % hint_name
                col_type = Int
                fkey_dest_table = actual_type_like

            else:
                actual_type = make_sql_type(actual_type_like)
                # print('actual type=', actual_type)
                actual_type_origin = get_origin(actual_type) or actual_type
                # Calc a name and type for column object
                if not issubclass(actual_type_origin, SQLTypeABC):
                    raise TypeError('Invalid type of column.', actual_type)

                col_name = hint_name
                col_type = actual_type

            # Get a default value
            default_value: ExprLike | None = None
            if hasattr(cls, hint_name):
                _v = getattr(cls, hint_name)
                default_value = NULL if _v is None else _v
                if default_value is NULL:
                    nullable = True

            col_args.append(ColumnArgs(name=col_name, sql_type=col_type, default=default_value, nullable=nullable))

        cls.__table_obj = Table(db_obj, TableArgs(cls._get_table_name(), *col_args))
        
        db_obj.append_table(cls.__table_obj)

        for column in cls.__table_obj.iter_table_columns():
            setattr(cls, str(column.name), column)

    def __new__(cls, *args, **kwargs):
        if cls is TableClass:
            raise RuntimeError('Cannot instantiate a TableClass directly.')
        return super().__new__()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
    

T = TypeVar('T')
class ColumnDef(TableColumnABC, Generic[T]):
    """ Column Definition """
