"""
    Column classes
"""
from typing import TYPE_CHECKING, Iterator, Optional

from ..syntax.object_abc import Name, ObjectABC
from ..syntax.exprs import ExprABC, NamedExpr, NamedExprABC
from ..syntax.keywords import OrderType, OrderLike, ReferenceOption
from ..syntax.query_abc import iter_objects
from ..syntax.query_data import QueryData
from ..syntax import sqltypes
from ..syntax import errors

if TYPE_CHECKING:
    from .view import ViewABC
    from .table import Table, ForeignKeyReference

class Column(NamedExpr):
    """ Column expression """

    def __init__(self,
        name: Name,
        sql_type: Optional[sqltypes.SQLType] = None,
        *,
        view : Optional['ViewABC'] = None,
        table: Optional['Table'] = None,
        not_null: bool = False,
        default = None, 
        # comment: Optional[str] = None,
        unique: bool = False,
        primary: bool = False,
        auto_increment: bool = False,
        ref_column: Optional['Column'] = None,
        ref_on_delete: Optional[ReferenceOption] = None,
        ref_on_update: Optional[ReferenceOption] = None,
        ref_index_name: Optional[Name] = None
    ):
        super().__init__(name)
        self._sql_type = sqltypes.make_sql_type(sql_type) if sql_type is not None else None
        self._not_null = not_null or primary
        self._view = view
        self._table = table
        self._default_value = default
        # self._comment = comment
        self._is_unique = unique
        self._is_primary = primary
        self._is_auto_increment = auto_increment
        self._reference: Optional['ForeignKeyReference'] =  None

        if ref_column is not None:
            self.set_reference(
                ref_column,
                on_update=ref_on_update,
                on_delete=ref_on_delete,
                index_name=ref_index_name,
            )

    @property
    def sql_type(self) -> Optional[sqltypes.SQLType]:
        return self._sql_type

    @property
    def is_not_null_type(self) -> bool:
        return self._not_null

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC

    @property
    def original_expr(self) -> ExprABC:
        return self

    @property
    def view_or_none(self):
        return self._view

    @property
    def table_or_none(self):
        return self._table

    @property
    def view(self) -> 'ViewABC':
        if self._view is None:
            return self.table
        return self._view

    @property
    def table(self) -> 'Table':
        if self._table is None:
            raise errors.ObjectNotSetError('Table is not set.')
        return self._table

    @property
    def database(self):
        return self.table.database

    @property
    def cnx(self):
        return self.table.cnx

    @property
    def default_value(self):
        return self._default_value

    # @property
    # def comment(self):
    #     return self._comment

    @property
    def is_unique(self):
        return self._is_unique

    @property
    def is_primary(self):
        return self._is_primary

    @property
    def is_auto_increment(self):
        return self._is_auto_increment

    def set_table(self, table: 'Table') -> None:
        """ Set a table object """
        if self._table is not None:
            raise errors.ObjectAlreadySetError('Table already set.')
        self._table = table

    def set_reference(self, 
        column: 'Column',
        *,
        on_delete: Optional[ReferenceOption] = None,
        on_update: Optional[ReferenceOption] = None,
        index_name: Optional[Name] = None
    ) -> None:
        """ Set foreign key reference from this column """
        if self._reference is not None:
            raise errors.ObjectAlreadySetError('Foreign key reference is already set.')
        self._reference = ForeignKeyReference(
            self,
            column,
            on_update=on_update,
            on_delete=on_delete,
            name=index_name,
        )

    def q_order(self) -> tuple:
        return (self, self.order_type)

    def append_to_query_data(self, qd: QueryData) -> None:
        if self.table_or_none is not None:
            qd.append(self.table, b'.')
        super().append_to_query_data(qd)

    def ordered(self, order: OrderLike):
        return OrderedColumn(self, OrderType.make(order))

    def __pos__(self):
        """ Get a ASC ordered expression """
        return OrderedColumn(self, OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return OrderedColumn(self, OrderType.DESC)

    def __repr__(self):
        if self._table is not None:
            return 'Col(%s.%s)' % (str(self.table), str(self))
        return 'Col(%s)' % str(self)

    @property
    def query_for_select_column(self) -> QueryData:
        return QueryData(self)
    
    @property
    def query_for_create_table(self) -> QueryData:
        return QueryData(
            self.name, b' ',
            sqltypes.get_type_sql(self.sql_type).encode(),
            (b'NOT', b'NULL') if self.is_not_null_type else None,
            (b'DEFAULT', self.default_value) if self.default_value else None,
            b'AUTO_INCREMENT' if self.is_auto_increment else None,
            b'UNIQUE' if self.is_unique else None,
            (b'PRIMARY', b'KEY') if self.is_primary else None,
            self._reference,
        )


class OrderedColumn(NamedExprABC):
    """ Ordered Column Expr """
    def __init__(self, expr: Column, order: OrderType):
        self._original_expr = expr
        self._order_kind = order

    @property
    def name(self):
        return self._original_expr.name

    @property
    def original_expr(self) -> ExprABC:
        """ Get a original expr """
        return self._original_expr

    @property
    def order_type(self) -> OrderType:
        return self._order_kind

    def iter_objects(self) -> Iterator[ObjectABC]:
        return self._original_expr.iter_objects()

    def append_to_query_data(self, qd: QueryData) -> None:
        return self._original_expr.append_to_query_data(qd)

    @property
    def query_for_select_column(self) -> QueryData:
        return self._original_expr.query_for_select_column


def iter_columns(*exprs: Optional[ObjectABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, Column):
            yield e
