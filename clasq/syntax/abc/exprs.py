"""
    Expression abstract classses
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Collection, Generic, Iterable, Iterator, TypeAlias, TypeVar

from ...utils.typed_generic import TypedGeneric, TypedGenericABC
from .values import SQLValue, is_value_type
from .keywords import OrderType, OrderTypeLike
from .object import ObjectABC, ObjectName
from .query import QueryABC, QueryDataABC
from .data_types import DataTypeABC, AnyDataType

class ExprABC(ABC):
    """ Expression abstract class """

    @property
    def _data_type(self) -> type[DataTypeABC]:
        """ Get a data type
            (Default Implementation)
        """
        return AnyDataType
    
    @abstractmethod
    def _call_func(self, funcname, *args):
        """ Call function """
        raise NotImplementedError()
        
    @abstractmethod
    def _call_op(self, opname, *args):
        """ Call operator with arguments """
        raise NotImplementedError()

    def _call_uop(self, opname):
        """ Call unary operator """
        return self._call_op(opname)

    def _call_bop(self, opname, y):
        """ Call binary operator """
        return self._call_op(opname, self, y)

    def _rcall_bop(self, opname, y):
        """ Reversely call binary operator """
        return self._call_op(opname, y, self)
    
    def _call_ufunc(self, funcname):
        """ Call function """
        return self._call_func(funcname, self)

    def __add__(self, y):
        """ Addition operator """
        return self._call_bop('ADD', y)

    def __sub__(self, y):
        """ Subtraction operator """
        return self._call_bop('SUB', y)

    def __mul__(self, y):
        """ Multiplication operator """
        return self._call_bop('MUL', y)

    def __truediv__(self, y):
        """ Division operator """
        return self._call_bop('DIV', y)

    def __floordiv__(self, y):
        """ Integer division operator """
        return self._call_bop('INTDIV', y)

    def __mod__(self, y):
        """ Modulo operator """
        return self._call_bop('MOD', y)

    def __and__(self, y):
        """ Logical AND operator """
        return self._call_bop('AND', y)

    def __xor__(self, y):
        """ Logical XOR operator """
        return self._call_bop('XOR', y)

    def __or__(self, y):
        """ Logical OR operator """
        return self._call_bop('OR', y)

    def __eq__(self, y):
        """ Equal operator """
        # print('ExprABC.__eq__() called:', repr(self), repr(y))
        return self._call_bop('EQ', y)

    def __ne__(self, y):
        """ Not equal operator """
        return self._call_bop('NOT_EQ', y)

    def __lt__(self, y):
        """ Less than operator """
        return self._call_bop('LT', y)

    def __le__(self, y):
        """ Less than or equal operator """
        return self._call_bop('LT_EQ', y)

    def __gt__(self, y):
        """ Greater than operator """
        return self._call_bop('GT', y)

    def __ge__(self, y):
        """ Greater than or equal operator """
        return self._call_bop('GT_EQ', y)
        
    def __radd__(self, y):
        """ Addition operator (reverse) """
        return self._rcall_bop('ADD', y)

    def __rsub__(self, y):
        """ Subtraction operator (reverse) """
        return self._rcall_bop('SUB', y)

    def __rmul__(self, y):
        """ Multiplication operator (reverse) """
        return self._rcall_bop('MUL', y)

    def __rtruediv__(self, y):
        """ Division operator (reverse) """
        return self._rcall_bop('DIV', y)

    def __rfloordiv__(self, y):
        """ Integer division operator (reverse) """
        return self._rcall_bop('INTDIV', y)

    def __rmod__(self, y):
        """ Modulo operator (reverse) """
        return self._rcall_bop('MOD', y)

    def __rand__(self, y):
        """ Logical AND operator (reverse) """
        return self._rcall_bop('AND', y)

    def __rxor__(self, y):
        """ Logical XOR operator (reverse) """
        return self._rcall_bop('XOR', y)

    def __ror__(self, y):
        """ Logical OR operator (reverse) """
        return self._rcall_bop('OR', y)

    def __neg__(self):
        """ Minus (negative) operator """
        return self._call_uop('MINUS')

    def __pos__(self):
        """ Plus (positive) operator """
        return self

    def __invert__(self):
        """ NOT operator """
        return self._call_uop('NOT')
        
    def __abs__(self):
        """ ABS function """
        return self._call_ufunc('ABS')

    def __ceil__(self):
        """ CEIL function """
        return self._call_ufunc('CEIL')

    def __floor__(self):
        """ FLOOR function """
        return self._call_ufunc('FLOOR')

    def __round__(self, ndigits: int = 0):
        return self._call_func('ROUND', ndigits)

    def __trunc__(self):
        """ TRUNCATE function """
        return self._call_ufunc('TRUNCATE_0')

    def and_(self, expr):
        """ AND operator """
        return self._call_bop('AND', expr)

    def xor(self, expr):
        """ XOR operator """
        return self._call_bop('XOR', expr)

    def or_(self, expr):
        """ OR operator """
        return self._call_bop('OR', expr)

    def nulleq(self, expr):
        """ NULL safe equal operator """
        return self._call_bop('NULL_EQ', expr)

    def is_null(self):
        """ IS NULL operator """
        return self._call_bop('IS', None)

    def is_not_null(self):
        """ IS NOT NULL operator """
        return self._call_bop('IS_NOT', None)

    def in_(self, *exprs):
        """ IN operator """
        return self._call_op('IN', self, *exprs)

    def like(self, expr):
        """ LIKE operator """
        return self._call_bop('LIKE', expr)

    def regexp(self, expr):
        """ REGEXP operator """
        return self._call_bop('REGEXP', expr)

    def between(self, expr1, expr2):
        """ BETWEEN operator """
        return self._call_op('BETWEEN', expr1, expr2)

    def not_(self):
        """ NOT operator """
        return self._call_uop('NOT')

    def abs(self):
        """ call ABS function """
        return self._call_ufunc('ABS')

    def ceil(self):
        """ call CEIL function """
        return self._call_ufunc('CEIL')

    def floor(self):
        """ call FLOOR function """
        return self._call_ufunc('FLOOR')

    def round(self, ndigits: int = 0):
        """ call ROUND function """
        return self._call_func('ROUND', ndigits)

    def truncate(self, ndigits: int | None = None):
        """ call TRUNCATE function """
        if ndigits is None:
            return self._call_ufunc('TRUNCATE_0')
        return self._call_func('TRUNCATE', ndigits)

    def avg(self):
        """ call AVG function """
        return self._call_ufunc('AVG')

    def count(self):
        """ call COUNT function """
        return self._call_ufunc('COUNT')

    def max(self):
        """ call MAX function """
        return self._call_ufunc('MAX')

    def min(self):
        """ call MIN function """
        return self._call_ufunc('MIN')

    def stddev(self):
        """ call STDDEV function """
        return self._call_ufunc('STDDEV')

    def sum(self):
        """ call SUM function """
        return self._call_ufunc('SUM')

    def variance(self):
        """ call VARIANCE function """
        return self._call_ufunc('VARIANCE')
        
    @abstractmethod
    def aliased(self, alias: bytes | str) -> AliasedExprABC:
        """ Make a aliased object """

    def as_(self, alias: bytes | str) -> AliasedExprABC:
        """ Make a aliased object
            (Synonym of `aliased` method)
        """
        return self.aliased(alias)

ExprLike: TypeAlias = ExprABC | SQLValue

DT = TypeVar('DT', bound=DataTypeABC)
class TypedExprABC(ExprABC, TypedGenericABC, Generic[DT]):
    """ Expression abstract class """

    @property
    def _data_type(self) -> type[DataTypeABC]:
        return DT@self  # type: ignore

def is_expr_like(value) -> bool:
    return isinstance(value, ExprABC) or is_value_type(value)


class QueryExprABC(ExprABC, QueryABC):
    """ Query and Expr ABC """


class TypedQueryExprABC(QueryExprABC, TypedExprABC[DT], Generic[DT]):
    """ Query and Expr ABC (with type) """


# class Expr(TypedQueryExprABC[DT], Generic[DT]):
#     """ Expression objerct with any value """
#     def __init__(self, val):
#         if not (isinstance(val, ExprABC) or is_value_type(val)):
#             raise ObjectArgTypeError('Invalid value type %s: (%s)' % (type(val), repr(val)))
#         self._v = val.v if isinstance(val, Expr) else val

#     @property
#     def v(self):
#         """ Get a value of this expression """
#         return self._v

#     def __repr__(self):
#         return repr(self._v)

#     def _append_to_query_data(self, qd: QueryDataABC) -> None:
#         qd.append_values(self._v)


QueryArgName = int | str

class QueryArgABC(QueryExprABC):
    """ Query argument (Placeholder) object """

    @property
    @abstractmethod
    def _name(self) -> QueryArgName:
        """ Get an argument name """

    @property
    @abstractmethod
    def _default_or_none(self) -> SQLValue | None:
        """ Get a default value if exists """

    @property
    def has_default(self) -> bool:
        return self._default_or_none is not None

    @property
    def default(self) -> SQLValue:
        assert self._default_or_none is not None
        return self._default_or_none

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        qd.append_value(self)

    def is_same_arg(self, arg: QueryArgABC) -> bool:
        return self._name == arg._name and self._default_or_none == arg._default_or_none

    def __str__(self) -> str:
        return str(self._name)

    def __repr__(self) -> str:
        return 'Arg(%s, default=%s)' % (str(self), repr(self._default_or_none))


QueryParamOrArg: TypeAlias = SQLValue | QueryArgABC
QueryLike: TypeAlias = QueryParamOrArg | ExprABC | QueryABC | tuple | Iterable
QueryArgParams: TypeAlias = Collection[SQLValue] | dict[QueryArgName, SQLValue]


# VT = TypeVar('VT', bound=DataTypeABC)
class ExprObjectABC(QueryExprABC, ObjectABC):
    """ Object with expressions (Abstract class) (without type) """
    
    @property
    def _select_column_query(self) -> QueryLike:
        """ Get a query for SELECT column """
        return self  # Default Implementation
    
    @property
    def _order_type(self) -> OrderType:
        """ Return a order kind (ASC or DESC) """
        return OrderType.ASC  # Default Implementation

    @property
    def _ordered_query(self) -> QueryLike:
        return (self, self._order_type)

    @abstractmethod
    def _ordered(self, order: OrderTypeLike) -> OrderedExprObjectABC:
        """ Get a ordered column object from this column """
        # return OrderedExprObject(self, order)

    @property
    def _non_ordered(self) -> ExprObjectABC:
        """ Get a non-ordered (original) column """
        return self  # Default Implementation

    def __pos__(self):
        """ Get a ASC ordered expression """
        return self._ordered(OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return self._ordered(OrderType.DESC)    


class TypedExprObjectABC(ExprObjectABC, TypedQueryExprABC[DT], Generic[DT]):
    """ Object with expressions (Abstract class) (with type) """


class OrderedExprObjectABC(ExprObjectABC):
    """ Ordered Column Expr """

    @property
    @abstractmethod
    def _non_ordered(self) -> ExprObjectABC:
        """ Get a non-ordered original object 
            (Abstract class)
        """

    @property
    @abstractmethod
    def _order_type(self) -> OrderType:
        """ Get a order type 
            (Abstract class)
        """

    @property
    def _select_column_query(self) -> QueryLike:
        return self._non_ordered._select_column_query

    @property
    def _name(self) -> ObjectName:
        """ Get a object name (Override from `ObjectABC`) """
        return self._non_ordered._name

    def _iter_objects(self) -> Iterator[ObjectABC]:
        return self._non_ordered._iter_objects()

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """
        return self._non_ordered._append_to_query_data(qd)

    def __repr__(self) -> str:
        return 'Ordered[%s](%s)' % (
            ('+' if self._order_type == OrderType.ASC
            else '-' if self._order_type == OrderType.DESC else '?'),
            repr(self._non_ordered))


class AliasedExprABC(ExprObjectABC):

    @property
    @abstractmethod
    def _expr(self) -> ExprABC:
        """ Get a expression to be aliased """

    @property
    def _select_column_query(self) -> QueryLike:
        return (self._expr, b'AS', self._name)

    def _iter_objects(self) -> Iterator['ObjectABC']:
        if isinstance(self._expr, QueryABC):
            yield from self._expr._iter_objects()

    def __repr__(self):
        return 'Aliased[%s](%s)' % (self._name, repr(self._expr))
