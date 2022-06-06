"""
    Column abstract classes
"""
from __future__ import annotations
from abc import abstractmethod, abstractproperty
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from ...syntax.abc.data_types import DataTypeABC
from ...syntax.abc.exprs import ExprObjectABC, OrderedExprObjectABC, QueryLike, TypedExprObjectABC
from ...syntax.abc.keywords import OrderTypeLike
from ...syntax.abc.object import NameLike, ObjectName, ObjectWithNamePropABC
from ...syntax.abc.query import QueryDataABC
from ...syntax.exprs import Expr, OrderedExprObject

if TYPE_CHECKING:
    from .fkey_ref import ForeignKeyReference
    from .view import NamedViewABC
    from .table import TableABC


class ColumnABC(ExprObjectABC, Expr):
    """ Column ABC (without type) """
    @property
    @abstractmethod
    def view(self) -> NamedViewABC:
        """ Get a belonging BaseView object """

    @property
    def _database(self):
        """ Get a parent Database object """
        return self.view._database

    @property
    def _con(self):
        """ Get a database server connection """
        return self.view._con

    def _ordered(self, order: OrderTypeLike) -> OrderedExprObjectABC:
        return OrderedExprObject(self, order)


DT = TypeVar('DT', bound=DataTypeABC)
class TypedColumnABC(ColumnABC, TypedExprObjectABC[DT], Generic[DT]):
    """ Column ABC (with type) """


class ColumnReferenceABC(ColumnABC):
    """ Column Reference ABC """

    @abstractproperty
    def _dest_column(self) -> ColumnABC:
        """ Get a entity column object """

    @property
    def view(self) -> NamedViewABC:
        """ Get a belonging BaseView object """
        return self._dest_column.view


class TypedColumnReferenceABC(ColumnReferenceABC, TypedColumnABC[DT], Generic[DT]):
    """ Column Reference ABC """

    @abstractproperty
    def _dest_typed_column(self) -> TypedColumnABC[DT]:
        """ Get a entity column object """

    @property
    def _dest_column(self) -> ColumnABC:
        """ Get a entity column object
            (Override for `ColumnReferenceABC`)
        """
        return self._dest_typed_column


class NamedViewColumnABC(ColumnABC, ObjectWithNamePropABC):
    """ """
    @property
    def _name_with_view(self) -> ObjectName:
        return self.view._view_name + self.name

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """
        qd.append(self.view, b'.', self.name)

class TypedNamedViewColumnABC(NamedViewColumnABC, TypedColumnABC[DT], Generic[DT]):
    """ Column object which belonging to the BaseView object """


class NamedViewColumnReferenceABC(ColumnReferenceABC, NamedViewColumnABC):
    """ Reference of Column object which belonging to the BaseView object """


class TypedNamedViewColumnReferenceABC(NamedViewColumnReferenceABC, TypedColumnReferenceABC[DT], TypedNamedViewColumnABC[DT], Generic[DT]):
    """ Reference of Column object which belonging to the BaseView object """


@dataclass
class TableColumnArgs:
    """ Properties to create a new columns """
    name: NameLike
    column_type: type[TableColumnABC]
    default: DataTypeABC | None = None
    reference: ForeignKeyReference | None = None


class TableColumnABC(NamedViewColumnABC):
    """ Column in the Table on the Database
        (Abstract class)
    """

    @property
    @abstractmethod
    def table(self) -> TableABC:
        """ Get a parent Table object """

    @property
    def view(self) -> NamedViewABC:
        """ Get a belonging BaseView object 
            (Override for `NamedViewColumnABC`)
        """
        return self.table

    @abstractproperty
    def default_value(self) -> DataTypeABC:
        """ Get a defalut value """

    @abstractproperty
    def is_unique(self)-> bool:
        """ Is unique or not """

    @abstractproperty
    def is_primary(self)-> bool:
        """ Is primary or not """

    @abstractproperty
    def is_auto_increment(self) -> bool:
        """ Is auto increment or not """
        
    @property
    def _query_for_create_table(self) -> QueryLike:
        return (
            self._name, self._data_type.sql,
            (b'DEFAULT', self.default_value) if self.default_value else (),
            ((b'PRIMARY', b'KEY') if self.is_primary else b'UNIQUE') if self.is_unique else (),
            b'AUTO_INCREMENT' if self.is_auto_increment else (),
            # self._reference,
        )

class TypedTableColumnABC(TableColumnABC, TypedNamedViewColumnABC[DT], Generic[DT]):
    """ Table Column expression """


class TableColumnReferenceABC(NamedViewColumnReferenceABC, TableColumnABC):
    """ Table Column expression """

    @property
    @abstractmethod
    def _dest_table_column(self) -> TableColumnABC:
        """ Get a destination of Table Column ABC """

    @property
    def table(self) -> TableABC:
        """ Get a parent Table object """
        return self._dest_table_column.table

    @property
    def default_value(self):
        """ Get a defalut value """
        return self._dest_table_column.default_value

    @property
    def is_unique(self):
        """ Is unique or not """
        return self._dest_table_column.is_unique

    @property
    def is_primary(self):
        """ Is primary or not """
        return self._dest_table_column.is_primary

    @property
    def is_auto_increment(self):
        """ Is auto increment or not """
        return self._dest_table_column.is_auto_increment

    @property
    def _dest_column(self) -> ColumnABC:
        """ Override for ColumnReferenceABC """
        return self._dest_table_column

    @property
    def _query_for_create_table(self) -> QueryLike:
        """ Get a query data for CREATE TABLE """
        return self._dest_table_column._query_for_create_table


class TypedTableColumnReferenceABC(TableColumnReferenceABC, TypedNamedViewColumnReferenceABC[DT], TypedTableColumnABC[DT], Generic[DT]):
    """ Table Column expression """

    @property
    @abstractmethod
    def _dest_typed_table_column(self) -> TypedTableColumnABC[DT]:
        """ Get a destination of Table Column ABC """

    @property
    def _dest_table_column(self) -> TableColumnABC:
        return self._dest_typed_table_column
