"""
    Column abstract classes
"""
from abc import abstractproperty
from typing import TYPE_CHECKING, Optional, Type

from ..syntax.exprs import ObjectABC, ExprObjectABC
from ..syntax.object_abc import ObjectName
from ..syntax.query_data import QueryData
from ..syntax.keywords import OrderType
from ..syntax.errors import ObjectNotSetError

if TYPE_CHECKING:
    from ..syntax.exprs import ExprABC
    from .view import NamedViewABC, BaseViewABC
    from .table import Table
    from .sqltype_abc import SQLTypeABC

class ColumnABC(ExprObjectABC):
    """ Column ABC """

    @abstractproperty
    def named_view(self) -> 'NamedViewABC':
        """ Get a belonging BaseView object 
        """

    @abstractproperty
    def sql_type(self) -> Type['SQLTypeABC']:
        """ Get a type for SQL """

    @property
    def sql_type_name(self) -> bytes:
        """ Get a type name for SQL """
        return self.sql_type.sql_type_name

    @property
    def database(self):
        """ Get a parent Database object """
        return self.named_view.database

    @property
    def cnx(self):
        """ Get a database server connection """
        return self.named_view.cnx


class NamedViewColumnABC(ColumnABC, ObjectABC):
    """ Column object which belonging to the BaseView object """

    @property
    def name_with_view(self) -> ObjectName:
        return self.named_view.name + self.name

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """
        qd.append(self.named_view, b'.', self.name)


class TableColumnABC(NamedViewColumnABC):
    """ Table Column expression """

    @abstractproperty
    def table_or_none(self) -> Optional['Table']:
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
    def original_column(self) -> 'ExprABC':
        return self

    @property
    def table(self) -> 'Table':
        if self.table_or_none is None:
            raise ObjectNotSetError('Table is not set.')
        return self.table_or_none

    @property
    def base_view(self) -> 'BaseViewABC':
        return self.table

    @property
    def named_view(self) -> 'NamedViewABC':
        """ Get a belonging BaseView object 
            (Override from `NamedViewColumnABC`)
        """
        return self.table