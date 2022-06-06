"""
    Definition Computable class and subclasses
"""
from __future__ import annotations
from typing import Any, Generic, Iterator, TypeVar


from ..utils.keyset import FrozenKeySetABC, FrozenOrderedKeySetABC, KeySetABC, OrderedKeySetABC
from ..utils.class_vars_list import ClassVarsList
from .abc.data_types import DataTypeABC
from .abc.exprs import AliasedExprABC, ExprABC, ExprObjectABC, OrderedExprObjectABC, QueryArgABC, QueryArgName, QueryLike, TypedQueryExprABC, ExprLike, is_expr_like
from .abc.keywords import OrderType, OrderTypeLike
from .abc.object import NameLike, ObjectABC, Object, ObjectName
from .abc.query import QueryABC, QueryDataABC
from .abc.values import SQLValue, is_value_type
from ..errors import ObjectArgTypeError, ObjectArgNumError
from .abc.func import FuncABC, FuncCallABC, FuncWithNoArgsABC


class Expr(ExprABC):
    """ Expr (with operator/function call) """

    def _call_func(self, funcname, *args):
        return BasicFuncs._get(funcname).call(*args) 

    def _call_op(self, funcname, *args):
        return OPs._get(funcname).call(*args)

    def aliased(self, alias: bytes | str) -> AliasedExprABC:
        return AliasedExpr(self, alias)


DT = TypeVar('DT', bound=DataTypeABC)
class ExprValue(TypedQueryExprABC[DT], Expr, Generic[DT]):
    """ Expression objerct with any value """
    def __init__(self, val):
        if not (isinstance(val, ExprABC) or is_value_type(val)):
            raise ObjectArgTypeError('Invalid value type %s: (%s)' % (type(val), repr(val)))
        self._v = val.v if isinstance(val, ExprValue) else val

    @property
    def v(self):
        """ Get a value of this expression """
        return self._v

    def __repr__(self):
        return repr(self._v)

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        qd.append_values(self._v)


class NoneExprType(Expr):
    """ Expression objerct with any value """

    def __repr__(self):
        return 'NoneExpr'

    def __add__(self, y):
        return y

    def __sub__(self, y):
        return y

    def __mul__(self, y):
        return y

    def __and__(self, y):
        return y

    def __xor__(self, y):
        return y

    def __or__(self, y):
        return y
        
    def __radd__(self, y):
        return y

    def __rsub__(self, y):
        return y

    def __rmul__(self, y):
        return y

    def __rand__(self, y):
        return y

    def __rxor__(self, y):
        return y

    def __ror__(self, y):
        return y

    def __bool__(self):
        return False

NoneExpr = NoneExprType()


class OrderedExprObject(OrderedExprObjectABC, Expr):
    """ Ordered Column Expr """
    def __init__(self, expr_obj: ExprObjectABC, order: OrderTypeLike):
        self.__expr_obj = expr_obj
        self.__order_type = OrderType.make(order)

    @property
    def _non_ordered(self) -> ExprObjectABC:
        return self.__expr_obj

    @property
    def _order_type(self) -> OrderType:
        return self.__order_type

    def _ordered(self, order: OrderTypeLike) -> OrderedExprObjectABC:
        return OrderedExprObject(self._non_ordered, order)


class ExprObject(ExprObjectABC, Expr, Object):
    """ Object with expressions """

    def _ordered(self, order: OrderTypeLike) -> OrderedExprObjectABC:
        return OrderedExprObject(self, order)


class ExprObjectSet(KeySetABC[NameLike, ExprObjectABC]):
    """ Set of ExprObjectABC objects """

    def _key(self, obj: ExprObjectABC) -> ObjectName:
        return obj._name

    def _key_or_none(self, obj) -> NameLike | None:
        return obj._name if isinstance(obj, ExprObjectABC) else None


class FrozenExprObjectSet(FrozenKeySetABC[NameLike, ExprObjectABC]):
    """ Frozen set of ExprObjectABC objects """

    def _key(self, obj: ExprObjectABC) -> ObjectName:
        return obj._name

    def _key_or_none(self, obj) -> ObjectName | None:
        return obj._name if isinstance(obj, ExprObjectABC) else None


class OrderedExprObjectSet(ExprObjectSet, OrderedKeySetABC[NameLike, ExprObjectABC]):
    """ Ordered set of ExprObjectABC objects """


class FrozenOrderedExprObjectSet(FrozenExprObjectSet, FrozenOrderedKeySetABC[NameLike, ExprObjectABC]):
    """ Frozen ordered set of ExprObjectABC objects """


class AliasedExpr(AliasedExprABC, Expr, Object):
    def __init__(self, expr: ExprABC, name: NameLike) -> None:
        super().__init__(name)
        self.__expr = expr

    @property
    def _expr(self) -> ExprABC:
        return self.__expr

    def _ordered(self, order: OrderTypeLike) -> OrderedExprObjectABC:
        return OrderedExprObject(self, order)

    @property
    def _select_column_query(self) -> QueryLike:
        return (self._expr, b'AS', self.name)

    def _iter_objects(self) -> Iterator['ObjectABC']:
        if isinstance(self._expr, QueryABC):
            yield from self._expr._iter_objects()

    def __repr__(self):
        return 'Aliased[%s](%s)' % (self.name, repr(self.expr))


class QueryArg(QueryArgABC, Expr):
    """ Query argument (Placeholder) object """

    def __init__(self, name: QueryArgName, *, default: SQLValue | None = None) -> None:
        super().__init__()
        self.__name = name
        self.__default_or_none = default

    @property
    def _name(self) -> QueryArgName:
        """ Get an argument name """
        return self.__name

    @property
    def _default_or_none(self) -> SQLValue | None:
        """ Get a default value if exists """
        return self.__default_or_none


class _FuncWithNoArgsABC(FuncWithNoArgsABC):
    """ Function """

    def _func_call(self, *nn_args: ExprLike) -> FuncCallABC:
        return FuncCall(self, *nn_args)


class Func(_FuncWithNoArgsABC):
    """ Function """

    def __init__(self,
        name: bytes, argsets: list | None = None,
        returntype=None
    ):
        super().__init__(name, returntype)
        self._argsets: list[tuple[type, ...]] = (
            [argset if isinstance(argset, tuple) else (argset,) for argset in argsets]
            if argsets is not None else [])

    @property
    def argtypes(self):
        return self._argsets

    def check_args(self, args: tuple[ExprLike, ...]) -> None:
        """ Check the arguments """
        if self._argsets:
            if not any(len(argset) == len(args) for argset in self._argsets):
                raise ObjectArgNumError('Invalid number of arguments.', self._argsets, args)
        # TODO: Check argument types

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        qd.append(self.raw_name + b'(', list(args), b')')

    def __repr__(self):
        return str(self) + '(' + ','.join(repr(t) for t in self._argsets) if self._argsets else '' + ')'

    def repr_with_args(self, args: tuple[ExprLike, ...]) -> str:
        """ Get a statement data of this function with a given list of arguments (Override) """
        return str(self) + '(' + ', '.join(map(repr, args)) + ')'


class FuncWithNoArgs(_FuncWithNoArgsABC):
    """ Func with no arguments """

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert not args
        qd.append(self.raw_name)

    def check_args(self, args: tuple[ExprLike, ...]) -> None:
        if args:
            raise ObjectArgNumError('Function `%s` takes no arguments.' % self.name)


class FuncCall(FuncCallABC, Expr): 
    """ General expression class """
    def __init__(self, func: FuncABC, *args: ExprLike):
        """ init """
        if not isinstance(func, FuncABC):
            raise ObjectArgTypeError('Invalid function type.')
        self._func = func

        for i, arg in enumerate(args, 1):
            if not is_expr_like(arg): # or isinstance(arg, QueryABC) ?
                raise ObjectArgTypeError('Argument #%d: Invalid type. (all args: %s)' % (i, ', '.join(map(repr, args))))
        self._args: tuple[ExprLike, ...] = args

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args


class OpABC(Func):
    """ Operator """
    def __init__(self, name: bytes, argtypes: list | None = None, returntype=None, priority: int | None = None):
        super().__init__(name, argtypes, returntype)
        self._priority = priority


class UnaryOp(OpABC):
    """ Unary Opertor """
    
    def __init__(self, name: bytes, plv: int | None = None):
        super().__init__(name, [Any], Any, plv)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)

        Args:
            qd (QueryData): QueryData object to be appended
            args (tuple[ExprLike, ...]): Argument values for the function
        """
        assert len(args) == 1
        qd.append(self.name.raw_name, args[0])


class BinaryOp(OpABC):
    """ Binary Operator """

    def call(self, *args: ExprLike) -> ExprABC:
        nn_args = [arg for arg in args if not isinstance(arg, NoneExprType)]
        if not nn_args:
            return NoneExpr
        if len(nn_args) == 1:
            arg = nn_args[0]
            if isinstance(arg, ExprABC):
                return arg
            return ExprValue(arg)
        return super()._func_call(*nn_args)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)
        """
        assert len(args) >= 2
        qd.append(b'(') 
        qd.append_joined(args, sep=self.name.raw_name)
        qd.append(b')')
        # TODO: Priority

    def _proc_values(self, *args):
        # if not len(args):
        #     raise ObjectArgNumError('No arguments.')
        # if len(args) == 1:
        #     return (self._proc_value(args[0]), NoneExpr)
        # return tuple(map(self._proc_value, args))
        return map(self._proc_value, args)

    def _proc_value(self, arg):
        return arg


class CalcBinaryOp(BinaryOp):
    """ Calculation Binary Operator """
    
    def __init__(self, name: bytes, plv: int | None = None):
        super().__init__(name, None, ExprABC, plv)


class CompareBinaryOp(BinaryOp):
    """ Compare Binary Operator """
    
    def __init__(self, name: bytes, plv: int | None = None):
        super().__init__(name, [(ExprABC, ExprABC)], bool | None, plv)


class OpEQ(CompareBinaryOp):

    def __init__(self):
        super().__init__(name=b'=', plv=11)

    def call(self, *args) -> FuncCall:
        return OpEqCall(self, *self._proc_values(*args))


class OpNotEQ(CompareBinaryOp):

    def __init__(self):
        super().__init__(name=b'!=', plv=11)

    def call(self, *args) -> FuncCall:
        return OpNotEqCall(self, *self._proc_values(*args))


class OpEqCall(FuncCall):
    def __bool__(self) -> bool:
        return self.args[0] is self.args[1]


class OpNotEqCall(FuncCall):
    def __bool__(self) -> bool:
        return self.args[0] is not self.args[1]


class OpIN(BinaryOp):
    """ IN Operator """
    def __init__(self):
        super().__init__(b'IN', [ExprABC, ExprABC], bool | None, 11)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) >= 2
        qd.append(b'(', args[0], b'IN', b'(', list(args[1:]), b')', b')')
        # TODO: Priority


class OpBETWEEN(BinaryOp):
    """ BETWEEN AND Operator """
    def __init__(self):
        super().__init__(b'BETWEEN', [ExprABC, ExprABC, ExprABC], bool | None, 11)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 3
        qd.append(b'(', args[0], b'BETWEEN', args[1], b'AND', args[2], b')')
        # TODO: Priority


class OpCASE(BinaryOp):
    """ CASE WHEN Operator """
    def __init__(self):
        super().__init__(b'CASE', [ExprABC, ExprABC, ExprABC], ExprABC, 12)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 2 or len(args) == 3
        qd.append(
            b'(', b'CASE', b'WHEN', args[0], b'THEN', args[1],
            (b'ELSE', args[2]) if len(args) == 3 else (),
            b'END', b')'
        )
        # TODO: Priority


class TRUNCATE_0(Func):
    """ TRUNCATE Operator (ndigits = 0) """
    def __init__(self):
        super().__init__(b'TRUNCATE', [float], float)

    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 1
        qd.append(b'TRUNCATE(', args[0], b',', b'0', b')')
        # TODO: Priority


class OPs(ClassVarsList[OpABC]):
    """ Definition of operators """

    ADD      = CalcBinaryOp(b'+'  , plv=7)
    SUB      = CalcBinaryOp(b'-'  , plv=7)
    MUL      = CalcBinaryOp(b'*'  , plv=6)
    DIV      = CalcBinaryOp(b'/'  , plv=6)
    MOD      = CalcBinaryOp(b'%'  , plv=6)
    MOD_     = CalcBinaryOp(b'MOD', plv=6)
    INTDIV   = CalcBinaryOp(b'DIV', plv=6)

    EQ       = OpEQ()
    IS       = CompareBinaryOp(b'IS'    , plv=11)
    IS_NOT   = CompareBinaryOp(b'IS NOT', plv=11)
    LT       = CompareBinaryOp(b'<'     , plv=11)
    LT_EQ    = CompareBinaryOp(b'<='    , plv=11)
    NULL_EQ  = CompareBinaryOp(b'<=>'   , plv=11)
    GT       = CompareBinaryOp(b'>'     , plv=11)
    GT_EQ    = CompareBinaryOp(b'>='    , plv=11)
    NOT_EQ   = OpNotEQ()
    NOT_EQ_  = CompareBinaryOp(b'<>'    , plv=11)

    BIT_AND_OP  = CalcBinaryOp(b'&' , plv=9)
    BIT_OR_OP   = CalcBinaryOp(b'|' , plv=10)
    BIT_XOR_OP  = CalcBinaryOp(b'^' , plv=5)
    BIT_RSHIFT  = CalcBinaryOp(b'>>', plv=8)
    BIT_LSHIFT  = CalcBinaryOp(b'<<', plv=8)

    AND  = CalcBinaryOp(b'AND' , plv=14)
    AND_ = CalcBinaryOp(b'&&'  , plv=14)
    OR   = CalcBinaryOp(b'OR'  , plv=16)
    OR_  = CalcBinaryOp(b'||'  , plv=16)
    XOR  = CalcBinaryOp(b'XOR' , plv=15)
    
    IN   = OpIN()
    LIKE = CompareBinaryOp(b'LIKE')
    RLIKE  = CompareBinaryOp(b'RLIKE', plv=11)
    REGEXP = CompareBinaryOp(b'REGEXP', plv=11)
    SOUNDS_LIKE = CompareBinaryOp(b'SOUNDS_LIKE', plv=11)

    COLLATE = BinaryOp(b'COLLATE', [ExprABC, ExprABC], ExprABC, 2)
    
    BETWEEN = OpBETWEEN()
    CASE    = OpCASE()
    
    MINUS    = UnaryOp(b'-', plv=4)

    BIT_INV  = UnaryOp(b'~', plv=4)
    NOT_OP   = UnaryOp(b'!', plv=3)

    NOT      = UnaryOp(b'NOT', plv=13)
    BINARY   = UnaryOp(b'BINARY', plv=2)

    JSON_EXTRACT_OP = BinaryOp(b'->' , [ExprABC, ExprABC], str)
    JSON_UNQUOTE_OP = BinaryOp(b'->>', [ExprABC, ExprABC], str)

    MEMBER_OF = BinaryOp(b'MEMBER OF', [ExprABC, ExprABC], bool | None, 11)


class BasicFuncs(ClassVarsList[Func]):
    """ Definition of basic functions """

    ABS      = Func(b'ABS'     , [float], float)
    CEIL     = Func(b'CEIL'    , [float], float)
    FLOOR    = Func(b'FLOOR'   , [float], float)
    ROUND    = Func(b'ROUND'   , [float, (float, int)], float)
    TRUNCATE = Func(b'TRUNCATE', [(float, int)], float)
    TRUNCATE_0 = TRUNCATE_0()

    AVG = Func(b'AVG')
    COUNT = Func(b'COUNT')
    MAX = Func(b'MAX')
    MIN = Func(b'MIN')
    STDDEV = Func(b'STDDEV')
    SUM = Func(b'SUM')
    VARIANCE = Func(b'VARIANCE')
