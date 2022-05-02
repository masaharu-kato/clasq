"""
    Definition ExprType class and subclasses
"""
from abc import abstractmethod, abstractproperty
from typing import List, Optional, TYPE_CHECKING, Union

from .expr_abc import ExprABC, FuncABC
from .keywords import OrderType

if TYPE_CHECKING:
    from .query_data import QueryData


class FuncType(FuncABC):
    """ Func Type """
    def call(self, *args) -> 'FuncExpr':
        return FuncExpr(self, *args)


class NoArgsFuncABC(FuncType):
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

    def check_args(self, args: List[ExprABC]) -> None:
        """ Check the arguments """
        # if self._argtypes is not None and len(self._argtypes) != len(args):
        #     raise RuntimeError('Invalid arguments.')
        # TODO: Check types


    def append_query_data_with_args(self, qd: 'QueryData', args: List[ExprABC]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        qd.append(self._name + b'(', args, b')')

    def __repr__(self):
        return str(self) + '(' + ','.join(repr(t) for t in self._argtypes) if self._argtypes else '' + ')'

    def repr_with_args(self, args: List[ExprABC]) -> str:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        return str(self) + '(' + ', '.join(map(repr, args)) + ')'


class NoArgsFunc(NoArgsFuncABC):
    """ Func with no arguments """

    def append_query_data_with_args(self, qd: 'QueryData', args: List[ExprABC]) -> None:
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

    def append_query_data_with_args(self, qd: 'QueryData', args: List[ExprABC]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 1
        qd.append(self._name, args[0])


class BinaryOp(OpABC):
    """ Binary Op """

    def append_query_data_with_args(self, qd: 'QueryData', args: List[ExprABC]) -> None:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) >= 2
        qd.append(b'(') 
        qd.append_joined(args, sep=self.name)
        qd.append(b')')
        # TODO: Priority


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
        return MathBaseFunc.ABS.call(self)

    def __ceil__(self):
        return MathBaseFunc.CEIL.call(self)

    def __floor__(self):
        return MathBaseFunc.FLOOR.call(self)

    def __trunc__(self):
        return MathBaseFunc.TRUNCATE.call(self)

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
        
    def aliased(self, alias: Union[bytes, str]) -> 'Aliased':
        return Aliased(self, alias)

    def as_(self, alias: Union[bytes, str]) -> 'Aliased':
        return self.aliased(alias)


class Expr(ExprType):
    """ Expression objerct with any value """
    def __init__(self, val):
        if not (val is None or isinstance(val, (Expr, bool, int, float, str))):
            raise RuntimeError('Invalid value type %s: (%s)' % (type(val), repr(val)))
        self._v = val.v if isinstance(val, Expr) else val
        

    @property
    def v(self):
        return self._v

    def __repr__(self):
        return repr(self._v)

    def append_query_data(self, qd: 'QueryData') -> None:
        qd.append_as_value(self._v)


def make_expr(*vals, **kwargs) -> ExprType:
    """ """
    return make_expr_one(list(vals) if not kwargs else [*vals, kwargs], join_ops=[OP.AND, OP.OR], dict_op=OP.EQ)
    

def make_expr_one(val, *, join_ops: Optional[List[FuncType]]=None, dict_op: Optional[FuncType] = None) -> ExprType:
    """ """

    if isinstance(val, ExprType):
        return val

    if join_ops:
        assert len(join_ops) >= 1
        c_join_op = join_ops[0]
        next_make_expr = lambda val: make_expr_one(val, join_ops=[*join_ops[1:], c_join_op], dict_op=dict_op)
    else:
        c_join_op = None
        next_make_expr = lambda val: make_expr_one(val, dict_op=dict_op)

    if c_join_op and isinstance(val, list):
        if len(val) == 1:
            return next_make_expr(val[0])
        return c_join_op.call(*(next_make_expr(v) for v in val))

    if isinstance(val, tuple) and len(val) >= 1:
        if isinstance(val[0], FuncType):
            if len(val) == 1:
                return val[0].call()
            if len(val) == 2:
                if isinstance(val[1], list):
                    return val[0].call(*val[1])
                return val[0].call(next_make_expr(val[1]))
            
        if len(val) == 3 and isinstance(val[1], FuncType):
            return val[1].call(next_make_expr(val[0]), next_make_expr(val[2]))

        raise RuntimeError('Invalid form of tuple.')

    # if dict_op and isinstance(val, dict):
    #     return OP.AND.call(*(dict_op.call(Column(c.encode()), v) for c, v in val.items()))

    return Expr(val)


class FuncExpr(ExprType): 
    """ General expression class """
    def __init__(self, func: FuncType, *args):
        self._func = func
        self._args: List[ExprType] = [make_expr_one(arg) for arg in args]

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    def append_query_data(self, qd: 'QueryData') -> None:
        self._func.append_query_data_with_args(qd, self._args)

    def __repr__(self):
        return self._func.repr_with_args(self._args)


class ObjectExprABC(ExprType):

    @abstractproperty
    def name(self) -> bytes:
        """ Get a name """

    def __bytes__(self):
        return self.name

    def __str__(self):
        return self.name.decode()

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def stmt_bytes(self) -> bytes:
        assert not b'`' in self.name
        return b'`' + self.name + b'`'

    @abstractmethod
    def q_select(self) -> tuple:
        """ Get a query for SELECT """

class ObjectExpr(ObjectExprABC):
    """ Column expression """
    def __init__(self, name):
        assert isinstance(name, bytes)
        self._name = name

    @property
    def name(self):
        return self._name


class OrderedABC(ExprType):

    @abstractproperty
    def original_expr(self) -> ExprType:
        """ Get a original expr """
    
    @abstractproperty
    def order_kind(self) -> OrderType:
        """ Return a order kind (ASC or DESC) """

    def q_order(self) -> tuple:
        return (self.original_expr, self.order_kind)


class Aliased(ObjectExpr, OrderedABC):
    def __init__(self, expr: ExprType, name: Union[bytes, str]) -> None:
        super().__init__(name if isinstance(name, bytes) else name.encode())
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    def q_select(self) -> tuple:
        return (self._expr, b'AS', self)

    @property
    def original_expr(self) -> ExprType:
        return self

    @property
    def order_kind(self) -> OrderType:
        return OrderType.ASC

    def __pos__(self):
        """ Get a ASC ordered expression """
        return OrderedAliased(self, OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return OrderedAliased(self, OrderType.DESC)


class OrderedAliased(OrderedABC):
    """ Ordered Aliased Expr """
    def __init__(self, expr: Aliased, order: OrderType):
        self._original_expr = expr
        self._order_kind = order

    @abstractproperty
    def original_expr(self) -> ExprType:
        """ Get a original expr """
        return self._original_expr

    @property
    def order_kind(self) -> OrderType:
        return self._order_kind



class OP:
    ADD      = BinaryOp(b'+', [ExprType, ExprType], ExprType, 7)
    SUB      = BinaryOp(b'-', [ExprType, ExprType], ExprType, 7)
    MUL      = BinaryOp(b'*', [ExprType, ExprType], ExprType, 6)
    DIV      = BinaryOp(b'/', [ExprType, ExprType], ExprType, 6)
    MOD      = BinaryOp(b'%', [ExprType, ExprType], ExprType, 6)
    MOD_     = BinaryOp(b'MOD', [ExprType, ExprType], ExprType, 6)
    INTDIV   = BinaryOp(b'DIV', [ExprType, ExprType], ExprType, 6)

    EQ       = BinaryOp(b'=', [ExprType, ExprType], Optional[bool], 11)
    IS       = BinaryOp(b'IS', [ExprType, ExprType], Optional[bool], 11)
    IS_NOT   = BinaryOp(b'IS NOT', [ExprType, ExprType], Optional[bool])
    LT       = BinaryOp(b'<', [ExprType, ExprType], Optional[bool], 11)
    LT_EQ    = BinaryOp(b'<=', [ExprType, ExprType], Optional[bool], 11)
    NULL_EQ  = BinaryOp(b'<=>', [ExprType, ExprType], Optional[bool], 11)
    GT       = BinaryOp(b'>', [ExprType, ExprType], Optional[bool], 11)
    GT_EQ    = BinaryOp(b'>=', [ExprType, ExprType], Optional[bool], 11)
    NOT_EQ   = BinaryOp(b'!=', [ExprType, ExprType], Optional[bool], 11)
    NOT_EQ_  = BinaryOp(b'<>', [ExprType, ExprType], Optional[bool], 11)

    BIT_AND_OP  = BinaryOp(b'&', [ExprType, ExprType], ExprType, 9)
    BIT_OR_OP   = BinaryOp(b'|', [ExprType, ExprType], ExprType, 10)
    BIT_XOR_OP  = BinaryOp(b'^', [ExprType, ExprType], ExprType, 5)
    BIT_RSHIFT  = BinaryOp(b'>>', [ExprType, ExprType], ExprType, 8)
    BIT_LSHIFT  = BinaryOp(b'<<', [ExprType, ExprType], ExprType, 8)

    IN       = BinaryOp(b'IN', [ExprType, ExprType], Optional[bool], 11)
    LIKE     = BinaryOp(b'LIKE', [ExprType, ExprType], Optional[bool])
    AND  = BinaryOp(b'AND', [ExprType, ExprType], Optional[bool], 14)
    AND_ = BinaryOp(b'&&', [ExprType, ExprType], Optional[bool], 14)
    OR   = BinaryOp(b'OR', [ExprType, ExprType], Optional[bool], 16)
    OR_  = BinaryOp(b'||', [ExprType, ExprType], Optional[bool], 16)
    XOR  = BinaryOp(b'XOR', [ExprType, ExprType], Optional[bool], 15)
    
    RLIKE    = BinaryOp(b'RLIKE', [ExprType, ExprType], Optional[bool], 11)
    REGEXP   = BinaryOp(b'REGEXP', [ExprType, ExprType], Optional[bool], 11)
    SOUNDS_LIKE = BinaryOp(b'SOUNDS_LIKE', [ExprType, ExprType], Optional[bool], 11)

    COLLATE = BinaryOp(b'COLLATE', [ExprType, ExprType], ExprType, 2)
    
    BETWEEN = BinaryOp(b'BETWEEN', [ExprType, ExprType, ExprType], ExprType, 12), # TODO: Special
    CASE    = BinaryOp(b'CASE', [ExprType, ExprType, ExprType], ExprType, 12), # TODO:: Special
    
    MINUS    = UnaryOp(b'-', [ExprType], ExprType, 4)

    BIT_INV  = UnaryOp(b'~', [ExprType], ExprType, 4)
    NOT_OP   = UnaryOp(b'!', [ExprType], ExprType, 3)

    NOT      = UnaryOp(b'NOT', [ExprType], ExprType, 13)
    BINARY   = UnaryOp(b'BINARY', [ExprType], ExprType, 2)

    JSON_EXTRACT_OP = BinaryOp(b'->', [ExprType, ExprType], str)
    JSON_UNQUOTE_OP = BinaryOp(b'->>', [ExprType, ExprType], str)

    MEMBER_OF = BinaryOp(b'MEMBER OF', [ExprType, ExprType], Optional[bool], 11)


class MathBaseFunc:
    ABS      = Func(b'ABS'    , [float], float)
    CEIL     = Func(b'CEIL'   , [float], float)
    FLOOR    = Func(b'FLOOR'  , [float], float)
    TRUNCATE = Func(b'TRUNCATE', [float, int], float)
