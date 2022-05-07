"""
    Definition Computable class and subclasses
"""
from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterator, List, Optional, Tuple, Union, Type

from .query_abc import QueryABC
from .query_data import QueryData
from .keywords import OrderType, OrderLike
from .values import NULL, ValueType, is_value_type
from .object_abc import Name, ObjectABC, Object
from . import errors


class FuncABC(ABC):
    """ Function ABC """

    def __init__(self, name: bytes):
        assert isinstance(name, bytes)
        self._name = name

    def call(self, *args: 'ExprLike') -> 'ExprABC':
        """ Returns an expression representing a call to this function

        Returns:
            FuncExpr: Function call expression
        """
        nn_args = [arg for arg in args if not isinstance(arg, NoneExprType)]
        if not nn_args:
            return NoneExpr
        return self._call_nn(*nn_args)

    def _call_nn(self, *nn_args: 'ExprLike') -> 'ExprABC':
        return FuncCall(self, *nn_args)

    def __call__(self, *args) -> 'ExprABC':
        """ Returns an expression representing a call to this function
            Synonym of `call` method.

        Returns:
            FuncExpr: Function call expression
        """
        return self.call(*args)

    @property
    def name(self) -> bytes:
        """ Returns a function name

        Returns:
            bytes: Function name
        """
        return self._name

    def __bytes__(self):
        """ Returns a function name
            Synonym of `name` method.

        Returns:
            bytes: Function name
        """
        return self.name

    def __str__(self) -> str:
        """ Returns a function name as string

        Returns:
            str: Function name
        """
        return self.name.decode()

    @abstractmethod
    def append_query_data_with_args(self, qd:QueryData, args: Tuple['ExprLike', ...]) -> None:
        """ Append a statement data of this function with a given list of arguments
            (Abstract method)
        
        Args:
            qd (QueryData): QueryData object to be appended
            args (list): Argument values for the function
        """

    @abstractmethod
    def repr_with_args(self, args: Tuple['ExprLike', ...]) -> str:
        """ Get a statement data of this function with a given list of arguments """


class NoArgsFuncABC(FuncABC):
    """ Func with no arguments """
    def __init__(self, name: bytes, returntype=None):
        super().__init__(name)
        self._returntype = returntype

    @property
    def returntype(self):
        return self._returntype

    def repr_with_args(self, args: Tuple['ExprLike', ...]) -> str:
        """ Get a statement data of this function with a given list of arguments
            (Overrided from `FuncABC` class)
        """
        return str(self)


class Func(NoArgsFuncABC):
    """ Function """
    def __init__(self,
        name: bytes, argsets: Optional[list] = None,
        returntype=None
    ):
        super().__init__(name, returntype)
        self._argsets: List[Tuple[Type, ...]] = (
            [argset if isinstance(argset, tuple) else (argset,) for argset in argsets]
            if argsets is not None else [])

    @property
    def argtypes(self):
        return self._argsets

    def check_args(self, args: Tuple['ExprLike', ...]) -> None:
        """ Check the arguments """
        if self._argsets:
            if not any(len(argset) == len(args) for argset in self._argsets):
                raise errors.ObjectArgNumError('Invalid number of arguments.', self._argsets, args)
        # TODO: Check argument types


    def append_query_data_with_args(self, qd: QueryData, args: Tuple['ExprLike', ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        qd.append(self._name + b'(', list(args), b')')

    def __repr__(self):
        return str(self) + '(' + ','.join(repr(t) for t in self._argsets) if self._argsets else '' + ')'

    def repr_with_args(self, args: Tuple['ExprLike', ...]) -> str:
        """ Get a statement data of this function with a given list of arguments (Override) """
        return str(self) + '(' + ', '.join(map(repr, args)) + ')'


class NoArgsFunc(NoArgsFuncABC):
    """ Func with no arguments """

    def append_query_data_with_args(self, qd: QueryData, args: Tuple['ExprLike', ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert not args
        qd.append(self._name)


class OpABC(Func):
    """ Operator """
    def __init__(self, name: bytes, argtypes: Optional[list] = None, returntype=None, priority: Optional[int] = None):
        super().__init__(name, argtypes, returntype)
        self._priority = priority


class UnaryOp(OpABC):
    """ Unary Op """
    
    def __init__(self, name: bytes, plv: Optional[int] = None):
        super().__init__(name, [Any], Any, plv)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple['ExprLike', ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)

        Args:
            qd (QueryData): QueryData object to be appended
            args (Tuple[ExprLike, ...]): Argument values for the function
        """
        assert len(args) == 1
        qd.append(self._name, args[0])


class BinaryOp(OpABC):
    """ Binary Operator """

    def _call_nn(self, *nn_args: 'ExprLike') -> 'ExprABC':
        if len(nn_args) == 1:
            arg = nn_args[0]
            if isinstance(arg, ExprABC):
                return arg
            return Expr(arg)
        return super()._call_nn(*nn_args)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple['ExprLike', ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)
        """
        assert len(args) >= 2
        qd.append(b'(') 
        qd.append_joined(args, sep=self.name)
        qd.append(b')')
        # TODO: Priority

    def _proc_values(self, *args):
        # if not len(args):
        #     raise errors.ObjectArgNumError('No arguments.')
        # if len(args) == 1:
        #     return (self._proc_value(args[0]), NoneExpr)
        # return tuple(map(self._proc_value, args))
        return map(self._proc_value, args)

    def _proc_value(self, arg):
        if arg is None:
            return NULL
        return arg


class CalcBinaryOp(BinaryOp):
    """ Calculation Binary Operator """
    
    def __init__(self, name: bytes, plv: Optional[int] = None):
        super().__init__(name, None, ExprABC, plv)


class CompareBinaryOp(BinaryOp):
    """ Compare Binary Operator """
    
    def __init__(self, name: bytes, plv: Optional[int] = None):
        super().__init__(name, [(ExprABC, ExprABC)], Optional[bool], plv)


class OpEQ(CompareBinaryOp):

    def __init__(self):
        super().__init__(name=b'=', plv=11)

    def call(self, *args) -> 'FuncCall':
        return OpEqCall(self, *self._proc_values(*args))


class OpNotEQ(CompareBinaryOp):

    def __init__(self):
        super().__init__(name=b'!=', plv=11)

    def call(self, *args) -> 'FuncCall':
        return OpNotEqCall(self, *self._proc_values(*args))


class ExprABC(ABC):
    """ Expression abstract class """

    def __add__(self, y):
        """ Addition operator """
        return OP.ADD.call(self, y)

    def __sub__(self, y):
        """ Subtraction operator """
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
        """ Logical XOR operator """
        return OP.XOR.call(self, y)

    def __or__(self, y):
        """ Logical OR operator """
        return OP.OR.call(self, y)

    def __eq__(self, y):
        """ Equal operator """
        # print('ExprABC.__eq__() called:', repr(self), repr(y))
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
        """ Addition operator (reverse) """
        return OP.ADD.call(y, self)

    def __rsub__(self, y):
        """ Subtraction operator (reverse) """
        return OP.SUB.call(y, self)

    def __rmul__(self, y):
        """ Multiplication operator (reverse) """
        return OP.MUL.call(y, self)

    def __rtruediv__(self, y):
        """ Division operator (reverse) """
        return OP.DIV.call(y, self)

    def __rfloordiv__(self, y):
        """ Integer division operator (reverse) """
        return OP.INTDIV.call(y, self)

    def __rmod__(self, y):
        """ Modulo operator (reverse) """
        return OP.MOD.call(y, self)

    # def __rdivmod__(self, y):
    #     return self.rfunc('divmod', y)

    # def __rpow__(self, y):
    #     return self.rfunc('**', y)

    # def __rlshift__(self, y):
    #     return self.rfunc(OP.BIT_LSHIFT, y)

    # def __rrshift__(self, y):
    #     return self.rfunc(OP.BIT_RSHIFT, y)

    def __rand__(self, y):
        """ Logical AND operator (reverse) """
        return OP.AND.call(y, self)

    def __rxor__(self, y):
        """ Logical XOR operator (reverse) """
        return OP.XOR.call(y, self)

    def __ror__(self, y):
        """ Logical OR operator (reverse) """
        return OP.OR.call(y, self)

    def __neg__(self):
        """ Minus (negative) operator """
        return OP.MINUS.call(self)

    def __pos__(self):
        """ Plus (positive) operator """
        return self

    def __invert__(self):
        """ NOT operator """
        return OP.NOT.call(self)
        
    def __abs__(self):
        """ ABS function """
        return BasicFunc.ABS.call(self)

    def __ceil__(self):
        """ CEIL function """
        return BasicFunc.CEIL.call(self)

    def __floor__(self):
        """ FLOOR function """
        return BasicFunc.FLOOR.call(self)

    def __round__(self, ndigits: int = 0):
        return BasicFunc.ROUND.call(self, ndigits)

    def __trunc__(self):
        """ TRUNCATE function """
        return BasicFunc.TRUNCATE_0.call(self)

    # def __int__(self):
    #     return self.infunc('int')

    # def __float__(self):
    #     return self.infunc('float')

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

    def and_(self, expr):
        """ AND operator """
        return OP.AND.call(self, expr)

    def xor(self, expr):
        """ XOR operator """
        return OP.XOR.call(self, expr)

    def or_(self, expr):
        """ OR operator """
        return OP.OR.call(self, expr)

    def nulleq(self, expr):
        """ NULL safe equal operator """
        return OP.NULL_EQ.call(self, expr)

    def is_null(self):
        """ IS NULL operator """
        return OP.IS.call(self, NULL)

    def is_not_null(self):
        """ IS NOT NULL operator """
        return OP.IS_NOT.call(self, NULL)

    def in_(self, *exprs):
        """ IN operator """
        return OP.IN.call(self, *exprs)

    def like(self, expr):
        """ LIKE operator """
        return OP.LIKE.call(self, expr)

    def regexp(self, expr):
        """ REGEXP operator """
        return OP.REGEXP.call(self, expr)

    def between(self, expr1, expr2):
        """ BETWEEN operator """
        return OP.BETWEEN.call(self, expr1, expr2)

    def not_(self):
        """ NOT operator """
        return OP.NOT.call(self)

    def abs(self):
        """ call ABS function """
        return BasicFunc.ABS.call(self)

    def ceil(self):
        """ call CEIL function """
        return BasicFunc.CEIL.call(self)

    def floor(self):
        """ call FLOOR function """
        return BasicFunc.FLOOR.call(self)

    def round(self, ndigits: int = 0):
        """ call ROUND function """
        return BasicFunc.ROUND.call(self, ndigits)

    def truncate(self, ndigits: Optional[int] = None):
        """ call TRUNCATE function """
        if ndigits is None:
            return BasicFunc.TRUNCATE_0.call(self)
        return BasicFunc.TRUNCATE.call(self, ndigits)

    def avg(self):
        """ call AVG function """
        return BasicFunc.AVG.call(self)

    def count(self):
        """ call COUNT function """
        return BasicFunc.COUNT.call(self)

    def max(self):
        """ call MAX function """
        return BasicFunc.MAX.call(self)

    def min(self):
        """ call MIN function """
        return BasicFunc.MIN.call(self)

    def stddev(self):
        """ call STDDEV function """
        return BasicFunc.STDDEV.call(self)

    def sum(self):
        """ call SUM function """
        return BasicFunc.SUM.call(self)

    def variance(self):
        """ call VARIANCE function """
        return BasicFunc.VARIANCE.call(self)
        
    def aliased(self, alias: Union[bytes, str]) -> 'AliasedExpr':
        """ Make a aliased object """
        return AliasedExpr(self, alias)

    def as_(self, alias: Union[bytes, str]) -> 'AliasedExpr':
        """ Make a aliased object
            (Synonym of `aliased` method)
        """
        return self.aliased(alias)

ExprLike = Union[ExprABC, ValueType]

def is_expr_like(value) -> bool:
    return isinstance(value, ExprABC) or is_value_type(value)


class QueryExprABC(ExprABC, QueryABC):
    """ Query and Expr ABC """


class Expr(QueryExprABC):
    """ Expression objerct with any value """
    def __init__(self, val):
        if not (isinstance(val, ExprABC) or is_value_type(val)):
            raise errors.ObjectArgTypeError('Invalid value type %s: (%s)' % (type(val), repr(val)))
        self._v = val.v if isinstance(val, Expr) else val

    @property
    def v(self):
        """ Get a value of this expression """
        return self._v

    def __repr__(self):
        return repr(self._v)

    def append_query_data(self, qd: QueryData) -> None:
        qd.append_values(self._v)


class NoneExprType(ExprABC):
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


class NamedExprABC(QueryExprABC, ObjectABC):
    """ ColumnLike Object ABC """
    
    @abstractproperty
    def query_for_select_column(self) -> QueryData:
        """ Get a query for SELECT """

    @abstractproperty
    def original_expr(self) -> ExprABC:
        """ Get a original expr """
    
    @abstractproperty
    def order_type(self) -> OrderType:
        """ Return a order kind (ASC or DESC) """

    # def append_query_data(self, qd: QueryData) -> None:
    #     if self.original_expr is not self:
    #         return self.original_expr.append_query_data(qd)
    #     return super().append_query_data(qd)

    def q_order(self) -> tuple:
        return (self.original_expr, self.order_type)

    def ordered(self, order: OrderLike):
        return OrderedNamedExpr(self, OrderType.make(order))

    def __pos__(self):
        """ Get a ASC ordered expression """
        return OrderedNamedExpr(self, OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return OrderedNamedExpr(self, OrderType.DESC)


class NamedExpr(Object, NamedExprABC):
    """ ColumnLike Object """

    @property
    def query_for_select_column(self) -> QueryData:
        return QueryData(self.name)

    @property
    def original_expr(self) -> ExprABC:
        return self

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC


class AliasedExpr(NamedExpr):
    def __init__(self, expr: ExprABC, name: Name) -> None:
        super().__init__(name)
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    @property
    def query_for_select_column(self) -> QueryData:
        return QueryData(self._expr, b'AS', self)

    def iter_objects(self) -> Iterator['ObjectABC']:
        if isinstance(self._expr, QueryABC):
            yield from self._expr.iter_objects()


class OrderedNamedExpr(NamedExprABC):
    """ Ordered Aliased Expr """
    def __init__(self, expr: NamedExprABC, order: OrderType):
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
        """ Get a order kind """
        return self._order_kind

    @property
    def query_for_select_column(self) -> QueryData:
        return self._original_expr.query_for_select_column

    def append_query_data(self, qd: QueryData) -> None:
        return self._original_expr.append_query_data(qd)

    def iter_objects(self) -> Iterator['ObjectABC']:
        return self._original_expr.iter_objects()



class FuncCall(QueryExprABC): 
    """ General expression class """
    def __init__(self, func: FuncABC, *args: ExprLike):
        """ init """
        if not isinstance(func, FuncABC):
            raise errors.ObjectArgTypeError('Invalid function type.')
        self._func = func

        for i, arg in enumerate(args, 1):
            if not is_expr_like(arg): # or isinstance(arg, QueryABC) ?
                raise errors.ObjectArgTypeError('Argument #%d: Invalid type. (all args: %s)' % (i, ', '.join(map(repr, args))))
        self._args: Tuple[ExprLike, ...] = args

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    def append_query_data(self, qd: QueryData) -> None:
        self._func.append_query_data_with_args(qd, self._args)

    def iter_objects(self) -> Iterator['ObjectABC']:
        for arg in self._args:
            if isinstance(arg, QueryABC):
                yield from arg.iter_objects()

    def __repr__(self):
        return self._func.repr_with_args(self._args)


class OpEqCall(FuncCall):
    def __bool__(self) -> bool:
        return self.args[0] is self.args[1]


class OpNotEqCall(FuncCall):
    def __bool__(self) -> bool:
        return self.args[0] is not self.args[1]


class OpIN(BinaryOp):
    """ IN Operator """
    def __init__(self):
        super().__init__(b'IN', [ExprABC, ExprABC], Optional[bool], 11)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) >= 2
        qd.append(b'(', args[0], b'IN', b'(', list(args[1:]), b')', b')')
        # TODO: Priority


class OpBETWEEN(BinaryOp):
    """ BETWEEN AND Operator """
    def __init__(self):
        super().__init__(b'BETWEEN', [ExprABC, ExprABC, ExprABC], Optional[bool], 11)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 3
        qd.append(b'(', args[0], b'BETWEEN', args[1], b'AND', args[2], b')')
        # TODO: Priority


class OpCASE(BinaryOp):
    """ CASE WHEN Operator """
    def __init__(self):
        super().__init__(b'CASE', [ExprABC, ExprABC, ExprABC], ExprABC, 12)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 2 or len(args) == 3
        qd.append(
            b'(', b'CASE', b'WHEN', args[0], b'THEN', args[1],
            (b'ELSE', args[2]) if len(args) == 3 else None,
            b'END', b')'
        )
        # TODO: Priority


class TRUNCATE_0(Func):
    """ TRUNCATE Operator (ndigits = 0) """
    def __init__(self):
        super().__init__(b'TRUNCATE', [float], float)

    def append_query_data_with_args(self, qd: QueryData, args: Tuple[ExprLike, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 1
        qd.append(b'TRUNCATE(', args[0], b',', b'0', b')')
        # TODO: Priority


class OP:
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

    MEMBER_OF = BinaryOp(b'MEMBER OF', [ExprABC, ExprABC], Optional[bool], 11)


class BasicFunc:
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
