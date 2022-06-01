"""
    Table classes
"""
from __future__ import annotations

from ..syntax.abc.query import iter_objects
from ..syntax.abc.object import ObjectABC, ObjectName
from .abc.table import TableArgs, TableABC
from .column import FrozenOrderedNamedViewColumnSet, TableColumn
from .view import NamedView, ViewFinal
from .abc.database import DatabaseABC

# class Table(NamedViewABC, ViewWithColumns, Object): # <-- super() is not working correctly on these base classes
class Table(TableABC, NamedView, ViewFinal):
    """ Table Expr """

    def __init__(self, database: DatabaseABC, args: TableArgs):
        self.__database = database
        self.__name = ObjectName(args.name)

        # if self.__name in database:
        #     raise ObjectNameAlreadyExistsError('Table name already exists.', self.__name)

        super().__init__(FrozenOrderedNamedViewColumnSet(
            TableColumn(self, colargs) for colargs in args.column_args))
        
        self.__refs = args.refs
        self.__primary_keys = [self.get_table_column(c) for c in (args.primary_key or ())]
        self.__unique_columns = [self.get_table_column(c) for c in (args.unique or ())]

    def _refresh_select_from_query(self) -> None:
        pass  # Do nothing

    def get_name(self) -> ObjectName:
        """ Get a view name 
            (Override from `ObjectABC`) """
        return self.__name

    @property
    def _view_name_or_none(self) -> ObjectName | None:
        """ Get a view name 
            (Override from `ViewABC`) """
        return self.__name

    @property
    def _database_or_none(self) -> DatabaseABC | None:
        """ Get a parent Database object 
            (Override from `ViewABC`)
        """
        return self.__database

    @property
    def _primary_keys(self):
        return self.__primary_keys

    @property
    def _unique_columns(self):
        return self.__unique_columns


def iter_tables(*exprs: ObjectABC | None):
    for e in iter_objects(*exprs):
        if isinstance(e, TableColumn):
            if e.table_or_none is not None:
                yield e.table
        elif isinstance(e, Table):
            yield e
