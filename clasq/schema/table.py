"""
    Table classes
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Collection

from clasq.errors import ObjectNotFoundError, ReferenceResolveError
from clasq.schema.abc.column import TableColumnABC

from ..syntax.abc.query import iter_objects
from ..syntax.abc.object import NameLike, ObjectABC, ObjectName
from .abc.table import TableABC, TableReferenceABC, TableArgs
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
            colarg.column_type(self, colarg.name, default=colarg.default) for colarg in args.column_args)  # type: ignore
        )
        
        # self.__refs = args.refs
        # self.__primary_keys = [self.get_table_column(c) for c in (args.primary_key or ())]
        # self.__unique_columns = [self.get_table_column(c) for c in (args.unique or ())]

    def _refresh_select_from_query(self) -> None:
        pass  # Do nothing

    @property
    def _name(self) -> ObjectName:
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

    # @property
    # def _primary_keys(self):
    #     return self.__primary_keys

    # @property
    # def _unique_columns(self):
    #     return self.__unique_columns


class TableReference(TableReferenceABC):
    """ Reference of Table """
    def __init__(self, database: DatabaseABC, table_name: NameLike) -> None:
        super().__init__()
        self.__database = database
        self.__table_name = ObjectName(table_name)
        self.__dest_table: TableABC | None = None

    def _database_or_none(self) -> DatabaseABC | None:
        return self.__database

    def _get_name(self):
        """ Get a table name to reference 
            (Override for `TableReferenceABC`)
        """
        return self.__table_name

    def _get_dest_table(self):
        """ Get a entity table object
            (Override for `TableReferenceABC`)
        """
        if self.__dest_table is None:
            try:
                self.__dest_table = self.__database.get_table(self.__table_name)
            except ObjectNotFoundError as e:
                raise ReferenceResolveError(
                    'Failed to resolve reference.', self.__database, self.__table_name) from e
        return self.__dest_table


def iter_tables(*exprs: ObjectABC | None):
    for e in iter_objects(*exprs):
        if isinstance(e, TableColumn):
            yield e.table
        elif isinstance(e, Table):
            yield e
