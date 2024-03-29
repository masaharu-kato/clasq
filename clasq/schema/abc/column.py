"""
    Column abstract classes
"""
from __future__ import annotations
from abc import abstractproperty
from typing import TYPE_CHECKING, Type

from ...syntax.exprs import ExprABC, ExprObjectABC
from ...syntax.abc.object import ObjectName, ObjectWithNamePropABC
from ...syntax.query_data import QueryData
from ...syntax.keywords import OrderType
from ...syntax.errors import ObjectNotSetError

if TYPE_CHECKING:
    from ...syntax.exprs import ExprObjectABC
    from ..table import Table
    from .view import NamedViewABC, BaseViewABC
    from .sqltype import SQLTypeABC

class ColumnABC(ExprObjectABC):
    """ Column ABC """

    @abstractproperty
    def _named_view(self) -> NamedViewABC:
        """ Get a belonging BaseView object 
        """

    @abstractproperty
    def _sql_type(self) -> Type['SQLTypeABC']:
        """ Get a type for SQL """

    @property
    def _sql_type_name(self) -> bytes:
        """ Get a type name for SQL """
        return self._sql_type.sql_type_name

    @property
    def _database(self):
        """ Get a parent Database object """
        return self._named_view._database

    @property
    def _con(self):
        """ Get a database server connection """
        return self._named_view._con


class NamedViewColumnABC(ColumnABC, ObjectWithNamePropABC):
    """ Column object which belonging to the BaseView object """

    @property
    def _name_with_view(self) -> ObjectName:
        return self._named_view._view_name + self.name

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """
        qd.append(self._named_view, b'.', self.name)


class TableColumnABC(NamedViewColumnABC):
    """ Table Column expression """

    @abstractproperty
    def table_or_none(self) -> Table | None:
        """ Get a parent Table object if exists """

    @abstractproperty
    def is_nullable(self) -> bool:
        """ is nullable or not """

    @abstractproperty
    def default_value(self):
        """ Get a defalut value """

    @abstractproperty
    def is_unique(self):
        """ Is unique or not """

    @abstractproperty
    def is_primary(self):
        """ Is primary or not """

    @abstractproperty
    def is_auto_increment(self):
        """ Is auto increment or not """

    @abstractproperty
    def query_for_create_table(self) -> QueryData:
        """ Get a query data for CREATE TABLE """

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC

    @property
    def original_column(self) -> ExprABC:
        return self

    @property
    def table(self) -> Table:
        if self.table_or_none is None:
            raise ObjectNotSetError('Table is not set.')
        return self.table_or_none

    @property
    def base_view(self) -> BaseViewABC:
        return self.table

    @property
    def _named_view(self) -> NamedViewABC:
        """ Get a belonging BaseView object 
            (Override from `NamedViewColumnABC`)
        """
        return self.table