"""
    Schema expression classes
"""
from abc import abstractproperty
from typing import Optional, Tuple

from .keywords import OrderType
from .expr_abc import ExprABC
from .expr_type import ExprType
from .query_data import QueryData


class ObjectExprABC(ExprType):
    """ Column expression """
    def __init__(self, name):
        assert isinstance(name, bytes)
        self._name = name

    @property
    def name(self):
        return self._name

    def __bytes__(self):
        return self._name

    def __str__(self):
        return self._name.decode()

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def name_stmt_bytes(self) -> bytes:
        assert not b'`' in self._name
        return b'`' + self._name + b'`'


class OrderedColumnExprABC(ExprType):
    """ Ordered Column Expr ABC """

    @abstractproperty
    def order_kind(self) -> OrderType:
        """ Get a order kind of this column """

    @abstractproperty
    def column_expr(self) -> ExprABC:
        """ Get a original column expr """

    def order_expr(self):
        return OrderedColumnOrderedExpr(self)


class ColumnExpr(ObjectExprABC, OrderedColumnExprABC):
    """ Column expression """

    @property
    def order_kind(self) -> OrderType:
        return OrderType.ASC

    @property
    def column_expr(self) -> ExprABC:
        return self

    @property
    def table(self) -> Optional['TableExpr']:
        return None

    @property
    def name_stmt_bytes(self) -> bytes:
        return (self.table.name_stmt_bytes + b'.' if self.table else b'') + super().name_stmt_bytes

    def __repr__(self):
        return 'Col(%s)' % str(self)


class OrderedColumnExpr(OrderedColumnExprABC):
    """ Ordered Column Expr """
    def __init__(self, column: ColumnExpr, order: OrderType):
        self._column = column
        self._order = order

    @property
    def order_kind(self) -> OrderType:
        return self._order

    @property
    def column_expr(self) -> ExprABC:
        return self._column


class OrderedColumnOrderedExpr(ExprType):
    def __init__(self, column: OrderedColumnExprABC):
        self._column = column

    @property
    def column(self):
        return self._column

    @property
    def name_stmt_bytes(self) -> bytes:
        return self._column.name_stmt_bytes + b' ' + self._column.order_kind.value


class TableExpr(ObjectExprABC):
    """ Table Expr """

    def __repr__(self):
        return 'Table(%s)' % str(self)

    def column(self, name: bytes):
        return TableColumnExpr(self, name)

    def col(self, name: bytes):
        return self.column(name)
        
    def __getitem__(self, name: bytes):
        return self.column(name)
        
    def column_def_expr(self):
        return TableAllColumnsExpr(self)


class TableAllColumnsExpr(ExprType):
    """ Table all columns expr """
    def __init__(self, table: TableExpr):
        super().__init__()
        self._table = table

    @property
    def table(self):
        return self._table

    @property
    def name_stmt_bytes(self):
        return self._table.name_stmt_bytes + b'.*'


class TableColumnExpr(ColumnExpr):
    def __init__(self, table: TableExpr, name: bytes):
        super().__init__(name)
        self._table = table

    @property
    def table(self) -> Optional[TableExpr]:
        return self._table


class Aliased(ObjectExprABC):
    def __init__(self, expr: ExprType, name: bytes) -> None:
        super().__init__(name)
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    def column_def_expr(self) -> 'AliasedDef':
        return AliasedDef(self)


class AliasedDef(ExprType):
    def __init__(self, aliased: Aliased) -> None:
        super().__init__()
        self._aliased = aliased

    @property
    def aliased(self):
        return self._aliased

    @property
    def name_stmt_bytes(self) -> bytes:
        return self._aliased.expr.name_stmt_bytes + b' AS ' + self._aliased.name_stmt_bytes
