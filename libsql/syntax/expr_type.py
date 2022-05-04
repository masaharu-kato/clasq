"""
    Definition ExprType class and subclasses
"""
from abc import abstractmethod, abstractproperty
from typing import Iterator, List, Optional, TYPE_CHECKING, Tuple, Union

from . import errors
from .expr_abc import ExprABCBase, FuncABCBase
from .keywords import OrderType, Value

if TYPE_CHECKING:
    from .query_data import QueryData
    from ..schema import Column


class FuncABC(FuncABCBase):
    """ Func Type """

    def call(self, *args) -> 'FuncExpr':
        """ Returns an expression representing a call to this function

        Returns:
            FuncExpr: Function call expression
        """
        return FuncExpr(self, *args)


class NoArgsFuncABC(FuncABC):
    """ Func with no arguments """
    def __init__(self, name: bytes, returntype=None):
        super().__init__(name)
        self._returntype = returntype

    @property
    def returntype(self):
        return self._returntype


class Func(NoArgsFuncABC):
    """ Function """
    def __init__(self, name: bytes, argtypes: Optional[list] = None, returntype=None):
        super().__init__(name, returntype)
        self._argtypes = argtypes

    @property
    def argtypes(self):
        return self._argtypes

    def check_args(self, args: Tuple[ExprABCBase, ...]) -> None:
        """ Check the arguments """
        # if self._argtypes is not None and len(self._argtypes) != len(args):
        #     raise RuntimeError('Invalid arguments.')
        # TODO: Check types


    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        qd.append(self._name + b'(', args, b')')

    def __repr__(self):
        return str(self) + '(' + ','.join(repr(t) for t in self._argtypes) if self._argtypes else '' + ')'

    def repr_with_args(self, args: Tuple[ExprABCBase, ...]) -> str:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        return str(self) + '(' + ', '.join(map(repr, args)) + ')'


class NoArgsFunc(NoArgsFuncABC):
    """ Func with no arguments """

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
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

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)

        Args:
            qd (QueryData): QueryData object to be appended
            args (Tuple[ExprABCBase, ...]): Argument values for the function
        """
        assert len(args) == 1
        qd.append(self._name, args[0])


class BinaryOp(OpABC):
    """ Binary Operator """

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments
            (Override from `FuncABC` class)
        """
        assert len(args) >= 2
        qd.append(b'(') 
        qd.append_joined(args, sep=self.name)
        qd.append(b')')
        # TODO: Priority

    def call_joined_opt(self, *opt_args) -> Optional['ExprABC']:
        return self.call_joined(*(arg for arg in opt_args if arg is not None))

    def call_joined(self, *args) -> Optional['ExprABC']:
        if not args:
            return None
        if len(args) == 1:
            return args[0]
        return self.call(*args)


class CalcBinaryOp(BinaryOp):
    """ Calculation Binary Operator """
    
    def __init__(self, name: bytes, plv: Optional[int] = None):
        super().__init__(name, [ExprABC, ExprABC], ExprABC, plv)


class CompareBinaryOp(BinaryOp):
    """ Compare Binary Operator """
    
    def __init__(self, name: bytes, plv: Optional[int] = None):
        super().__init__(name, [ExprABC, ExprABC], Optional[bool], plv)

    def call(self, *args) -> 'FuncExpr':
        if not len(args) == 2:
            raise errors.ObjectArgsError('Invalid number of arguments.', args)


class ExprABC(ExprABCBase):
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
        return self.rfunc(OP.AND, y)

    def __rxor__(self, y):
        """ Logical XOR operator (reverse) """
        return self.rfunc(OP.XOR, y)

    def __ror__(self, y):
        """ Logical OR operator (reverse) """
        return self.rfunc(OP.OR, y)

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

    def __trunc__(self):
        """ TRUNCATE function """
        return BasicFunc.TRUNCATE.call(self)

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
        return OP.IS.call(self, Value.NULL)

    def is_not_null(self):
        """ IS NOT NULL operator """
        return OP.IS_NOT.call(self, Value.NULL)

    def in_(self, expr):
        """ IN operator """
        return OP.IN.call(self, expr)

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
        """ ABS function """
        return BasicFunc.ABS.call(self)

    def ceil(self):
        """ CEIL function """
        return BasicFunc.CEIL.call(self)

    def floor(self):
        """ FLOOR function """
        return BasicFunc.FLOOR.call(self)

    def truncate(self):
        """ TRUNCATE function """
        return BasicFunc.TRUNCATE.call(self)

    def avg(self):
        """ AVG function """
        return BasicFunc.AVG.call(self)

    def count(self):
        """ COUNT function """
        return BasicFunc.COUNT.call(self)

    def max(self):
        """ MAX function """
        return BasicFunc.MAX.call(self)

    def min(self):
        """ MIN function """
        return BasicFunc.MIN.call(self)

    def stddev(self):
        """ STDDEV function """
        return BasicFunc.STDDEV.call(self)

    def sum(self):
        """ SUM function """
        return BasicFunc.SUM.call(self)

    def variance(self):
        """ VARIANCE function """
        return BasicFunc.VARIANCE.call(self)
        
    def aliased(self, alias: Union[bytes, str]) -> 'Aliased':
        """ Make a aliased object """
        return Aliased(self, alias)

    def as_(self, alias: Union[bytes, str]) -> 'Aliased':
        """ Make a aliased object
            (Synonym of `aliased` method)
        """
        return self.aliased(alias)


class Expr(ExprABC):
    """ Expression objerct with any value """
    def __init__(self, val):
        if not (val is None or isinstance(val, (Expr, bool, int, float, str))):
            raise RuntimeError('Invalid value type %s: (%s)' % (type(val), repr(val)))
        self._v = val.v if isinstance(val, Expr) else val

    @property
    def v(self):
        """ Get a value of this expression """
        return self._v

    def __repr__(self):
        return repr(self._v)

    def append_query_data(self, qd: 'QueryData') -> None:
        qd.append_values(self._v)


class FuncExpr(ExprABC): 
    """ General expression class """
    def __init__(self, func: FuncABC, *args):
        self._func = func
        self._args: Tuple[ExprABC, ...] = args

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    def append_query_data(self, qd: 'QueryData') -> None:
        self._func.append_query_data_with_args(qd, self._args)

    def iter_objects(self) -> Iterator['Object']:
        for arg in self._args:
            yield from arg.iter_objects()

    def __repr__(self):
        return self._func.repr_with_args(self._args)


class ObjectABC(ExprABC):

    @abstractproperty
    def name(self) -> bytes:
        """ Get a name """

    def __bytes__(self):
        return self.name

    def __str__(self):
        return self.name.decode()

    def __hash__(self) -> int:
        return hash(self.name)

    def append_query_data(self, qd: 'QueryData') -> None:
        qd.append_object(self.name)

    def q_select(self) -> tuple:
        """ Get a query for SELECT """
        return (self,) # Default implementation

    def q_create(self) -> tuple:
        """ Get a query for creation """
        return (self,) # Default implementation

    def __repr__(self):
        return 'Obj(%s)' % str(self)


Name = Union[bytes, str]

class Object(ObjectABC):
    """ Column expression """
    def __init__(self, name: Name):
        assert isinstance(name, (bytes, str))
        self._name = (name if isinstance(name, bytes) else name.encode())

    @property
    def name(self):
        return self._name

    def iter_objects(self) -> Iterator['Object']:
        yield self # Default implementation


class OrderedABC(ExprABC):

    @abstractproperty
    def original_expr(self) -> ExprABC:
        """ Get a original expr """
    
    @abstractproperty
    def order_type(self) -> OrderType:
        """ Return a order kind (ASC or DESC) """

    def append_query_data(self, qd: 'QueryData') -> None:
        return self.original_expr.append_query_data(qd)

    def q_order(self) -> tuple:
        return (self.original_expr, self.order_type)


class Aliased(Object, OrderedABC):
    def __init__(self, expr: ExprABC, name: Union[bytes, str]) -> None:
        super().__init__(name if isinstance(name, bytes) else name.encode())
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    def q_select(self) -> tuple:
        return (self._expr, b'AS', self)

    @property
    def original_expr(self) -> ExprABC:
        return self

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC

    def __pos__(self):
        """ Get a ASC ordered expression """
        return OrderedAliased(self, OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return OrderedAliased(self, OrderType.DESC)

    def iter_objects(self) -> Iterator['Object']:
        yield from self._expr.iter_objects()


class OrderedAliased(OrderedABC):
    """ Ordered Aliased Expr """
    def __init__(self, expr: Aliased, order: OrderType):
        self._original_expr = expr
        self._order_kind = order

    @property
    def original_expr(self) -> ExprABC:
        """ Get a original expr """
        return self._original_expr

    @property
    def order_type(self) -> OrderType:
        """ Get a order kind """
        return self._order_kind

    def iter_objects(self) -> Iterator['Object']:
        return self._original_expr.iter_objects()


class OpIN(BinaryOp):
    """ IN Operator """
    def __init__(self):
        super().__init__(b'IN', [ExprABC, ExprABC], Optional[bool], 11)

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 2 and isinstance(args[1], list)
        qd.append(b'(', args[0], b'IN', b'(', args[1], b')', b')')
        # TODO: Priority


class OpBETWEEN(BinaryOp):
    """ BETWEEN AND Operator """
    def __init__(self):
        super().__init__(b'BETWEEN', [ExprABC, ExprABC, ExprABC], Optional[bool], 11)

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 3
        qd.append(b'(', args[0], b'BETWEEN', args[1], b'AND', args[2], b')')
        # TODO: Priority


class OpCASE(BinaryOp):
    """ CASE WHEN Operator """
    def __init__(self):
        super().__init__(b'CASE', [ExprABC, ExprABC, ExprABC], ExprABC, 12)

    def append_query_data_with_args(self, qd: 'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 2 or len(args) == 3
        qd.append(
            b'(', b'CASE', b'WHEN', args[0], b'THEN', args[1],
            (b'ELSE', args[2]) if len(args) == 3 else None,
            b'END', b')'
        )
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

    EQ       = CompareBinaryOp(b'='     , plv=11)
    IS       = CompareBinaryOp(b'IS'    , plv=11)
    IS_NOT   = CompareBinaryOp(b'IS NOT', plv=11)
    LT       = CompareBinaryOp(b'<'     , plv=11)
    LT_EQ    = CompareBinaryOp(b'<='    , plv=11)
    NULL_EQ  = CompareBinaryOp(b'<=>'   , plv=11)
    GT       = CompareBinaryOp(b'>'     , plv=11)
    GT_EQ    = CompareBinaryOp(b'>='    , plv=11)
    NOT_EQ   = CompareBinaryOp(b'!='    , plv=11)
    NOT_EQ_  = CompareBinaryOp(b'<>'    , plv=11)

    BIT_AND_OP  = CalcBinaryOp(b'&' , plv=9)
    BIT_OR_OP   = CalcBinaryOp(b'|' , plv=10)
    BIT_XOR_OP  = CalcBinaryOp(b'^' , plv=5)
    BIT_RSHIFT  = CalcBinaryOp(b'>>', plv=8)
    BIT_LSHIFT  = CalcBinaryOp(b'<<', plv=8)

    IN   = OpIN()
    LIKE = CompareBinaryOp(b'LIKE')
    AND  = CompareBinaryOp(b'AND' , plv=14)
    AND_ = CompareBinaryOp(b'&&'  , plv=14)
    OR   = CompareBinaryOp(b'OR'  , plv=16)
    OR_  = CompareBinaryOp(b'||'  , plv=16)
    XOR  = CompareBinaryOp(b'XOR' , plv=15)
    
    RLIKE  = CompareBinaryOp(b'RLIKE', plv=11)
    REGEXP = CompareBinaryOp(b'REGEXP', plv=11)
    SOUNDS_LIKE = CompareBinaryOp(b'SOUNDS_LIKE', plv=11)

    COLLATE = BinaryOp(b'COLLATE', [ExprABC, ExprABC], ExprABC, 2)
    
    BETWEEN = OpBETWEEN()
    CASE    = OpCASE()
    
    MINUS    = UnaryOp(b'-', [ExprABC], ExprABC, 4)

    BIT_INV  = UnaryOp(b'~', [ExprABC], ExprABC, 4)
    NOT_OP   = UnaryOp(b'!', [ExprABC], ExprABC, 3)

    NOT      = UnaryOp(b'NOT', [ExprABC], ExprABC, 13)
    BINARY   = UnaryOp(b'BINARY', [ExprABC], ExprABC, 2)

    JSON_EXTRACT_OP = BinaryOp(b'->', [ExprABC, ExprABC], str)
    JSON_UNQUOTE_OP = BinaryOp(b'->>', [ExprABC, ExprABC], str)

    MEMBER_OF = BinaryOp(b'MEMBER OF', [ExprABC, ExprABC], Optional[bool], 11)


class BasicFunc:
    """ Definition of basic functions """

    ABS      = Func(b'ABS'    , [float], float)
    CEIL     = Func(b'CEIL'   , [float], float)
    FLOOR    = Func(b'FLOOR'  , [float], float)
    TRUNCATE = Func(b'TRUNCATE', [float, int], float)

    AVG = Func(b'AVG')
    COUNT = Func(b'COUNT')
    MAX = Func(b'MAX')
    MIN = Func(b'MIN')
    STDDEV = Func(b'STDDEV')
    SUM = Func(b'SUM')
    VARIANCE = Func(b'VARIANCE')
