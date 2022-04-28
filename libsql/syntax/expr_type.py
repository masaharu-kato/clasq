"""
    Definition ExprType class and subclasses
"""
from abc import abstractproperty
from typing import List, Optional, Tuple

from mysqlx import Column
from .expr_class import ExprABC, Func, FuncABC

from .operators import OP
from .functions import Math
from .keywords import OrderType


class ExprType(ExprABC):
    """ Expression abstract class """

    def __add__(self, y):
        """ Addition operator """
        return OP.ADD.call(self, y)

    def __sub__(self, y):
        """ Minus operator """
        return OP.SUB.call(self, y)

    def __mul__(self, y):
        """ Multiplication operator """
        return OP.MUL.call(self, y)

    def __truediv__(self, y):
        """ Division operator """
        return OP.DIV.call(self, y)

    def __floordiv__(self, y):
        """ Integer division operator """
        return OP.INTDIV.call(self, y)

    def __mod__(self, y):
        """ Modulo operator """
        return OP.MOD.call(self, y)

    # def __divmod__(self, y):
    #     return self.formula('divmod', y)

    # def __pow__(self, y):
    #     return self.formula('**', y)

    # def __lshift__(self, y):
    #     """ Left shift operator """
    #     return OP.BIT_LSHIFT.call(self, y)

    # def __rshift__(self, y):
    #     """ Right shift operator """
    #     return OP.BIT_RSHIFT.call(self, y)

    def __and__(self, y):
        """ Logical AND operator """
        return OP.AND.call(self, y)

    def __xor__(self, y):
        """ Bitwise XOR operator """
        return OP.XOR.call(self, y)

    def __or__(self, y):
        """ Logical OR operator """
        return OP.OR.call(self, y)

    def __eq__(self, y):
        """ Equal operator """
        return OP.EQ.call(self, y)

    def __ne__(self, y):
        """ Not equal operator """
        return OP.NOT_EQ.call(self, y)

    def __lt__(self, y):
        """ Less than operator """
        return OP.LT.call(self, y)

    def __le__(self, y):
        """ Less than or equal operator """
        return OP.LT_EQ.call(self, y)

    def __gt__(self, y):
        """ Greater than operator """
        return OP.GT.call(self, y)

    def __ge__(self, y):
        """ Greater than or equal operator """
        return OP.GT_EQ.call(self, y)
        
    def __radd__(self, y):
        return self.rfunc(OP.ADD, y)

    def __rsub__(self, y):
        return self.rfunc(OP.SUB, y)

    def __rmul__(self, y):
        return self.rfunc(OP.MUL, y)

    def __rtruediv__(self, y):
        return self.rfunc(OP.DIV, y)

    def __rfloordiv__(self, y):
        return self.rfunc(OP.INTDIV, y)

    def __rmod__(self, y):
        return self.rfunc(OP.MOD, y)

    # def __rdivmod__(self, y):
    #     return self.rfunc('divmod', y)

    # def __rpow__(self, y):
    #     return self.rfunc('**', y)

    # def __rlshift__(self, y):
    #     return self.rfunc(OP.BIT_LSHIFT, y)

    # def __rrshift__(self, y):
    #     return self.rfunc(OP.BIT_RSHIFT, y)

    def __rand__(self, y):
        return self.rfunc(OP.AND, y)

    def __rxor__(self, y):
        return self.rfunc(OP.XOR, y)

    def __ror__(self, y):
        return self.rfunc(OP.OR, y)

    def __neg__(self):
        return OP.MINUS.call(self)

    def __pos__(self):
        return self

    # def __invert__(self):
    #     return OP.BIT_INV.call(self)
        
    def __abs__(self):
        return Math.ABS.call(self)

    def __ceil__(self):
        return Math.CEIL.call(self)

    def __floor__(self):
        return Math.FLOOR.call(self)

    def __trunc__(self):
        return Math.TRUNCATE.call(self)

    # def __int__(self):
    #     return self.infunc('int')

    # def __float__(self):
    #     return self.infunc('float')

    # def __round__(self):
    #     return self.infunc('round')

    # def __complex__(self):
    #     return self.infunc('complex')

    # def __bool__(self):
    #     return self.self('bool')
    # def __str__(self):
    #     return self.self('str')
    # def __bytes__(self):
    #     return self.self('bytes')
    # def __len__(self):
    #     return self.self('len')


class Expr(ExprType):
    """ Expression objerct with any value """
    def __init__(self, val):
        self._v = val.v if isinstance(val, Expr) else val

    @property
    def v(self):
        return self._v

    def __repr__(self):
        return repr(self._v)

    def get_statement_data(self) -> Tuple[bytes, list]:
        return b'', [self._v]


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

    @property
    def name_expr(self) -> bytes:
        assert not '`' in self._name
        return b'`' + self._name + b'`'

    def get_statement_data(self) -> Tuple[bytes, list]:
        return self.name_expr(), []


class OrderedColumnExprABC(ExprType):
    """ Ordered Column Expr ABC """

    def order_kind(self) -> OrderType:
        """ Get a order kind of this column """

    @abstractproperty
    def column_expr(self) -> 'ColumnExpr':
        """ Get a original column expr """


class ColumnExpr(ObjectExprABC, OrderedColumnExprABC):
    """ Column expression """

    def order_kind(self) -> OrderType:
        return OrderType.ASC

    @property
    def column_expr(self) -> 'ColumnExpr':
        return self

    @property
    def table(self) -> Optional['TableExpr']:
        return None

    @property
    def column_expr(self) -> bytes:
        return (self.name_expr + b'.' if self.table else b'') + self.name_expr

    def __repr__(self):
        return 'Col(%s)' % str(self)


class OrderedColumnExpr(OrderedColumnExprABC):
    """ Ordered Column Expr """
    def __init__(self, column: ColumnExpr, order: OrderType):
        self._column = column
        self._order = order

    def order_kind(self) -> OrderType:
        return self._order

    @property
    def column_expr(self) -> 'ColumnExpr':
        return self._column


class TableExpr(ObjectExprABC):
    """ Table Expr """

    def __repr__(self):
        return 'Table(%s)' % str(self)

    def column(self, name: bytes):
        return TableColumnExpr(self, name)
        
    @property
    def column_expr(self) -> bytes:
        return self.name_expr + b'.*'


class TableColumnExpr(ColumnExpr):
    def __init__(self, table: TableExpr, name: bytes):
        super().__init__(name)
        self._table = table

    @property
    def table(self) -> Optional[TableExpr]:
        return self._table

