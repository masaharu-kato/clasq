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


class TableColumnRef:
    def __init__(self,
        database: 'Database',
        table_like: Union[NameLike, 'Table'],
        column_name: NameLike
    ) -> None:
        self.database = database
        self.table_like = table_like
        self.column_name = column_name

    def resolve(self) -> 'TableColumn':
        return self.database.table(self.table_like).table_column(self.column_name)


class TableColumnArgs:
    def __init__(self,
        name: NameLike,
        sql_type: Optional[sqltypes.SQLType] = None,
        *,
        not_null: bool = False,
        default = None, 
        unique: bool = False,
        primary: bool = False,
        auto_increment: bool = False,
        ref_column: Optional[Union[TableColumnRef, 'TableColumn']] = None,
        ref_on_delete: Optional[ReferenceOption] = None,
        ref_on_update: Optional[ReferenceOption] = None,
        ref_index_name: Optional[NameLike] = None
    ):
        self.name = name
        self.sql_type = sql_type
        self.not_null = not_null
        self.default = default
        self.unique = unique
        self.primary = primary
        self.auto_increment = auto_increment
        self.ref_column = ref_column
        self.ref_on_delete = ref_on_delete
        self.ref_on_update = ref_on_update
        self.ref_index_name = ref_index_name


class TableColumn(ColumnABC, Object):
    """ Table Column expression """

    def __init__(self, table: 'Table', args: TableColumnArgs):
        ColumnABC.__init__(self)
        Object.__init__(self, args.name)

        self._sql_type = sqltypes.make_sql_type(args.sql_type) if args.sql_type is not None else None
        self._not_null = args.not_null or args.primary
        self._table = table
        self._default_value = args.default
        self._is_unique = args.unique
        self._is_primary = args.primary
        self._is_auto_increment = args.auto_increment
        self._reference: Optional['ForeignKeyReference'] =  None

        if args.ref_column is not None:
            self._reference = ForeignKeyReference(
                self,
                args.ref_column.resolve() if isinstance(args.ref_column, TableColumnRef) else args.ref_column,
                on_update=args.ref_on_update,
                on_delete=args.ref_on_delete,
                name=args.ref_index_name,
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
