""" 
    SQL expressions module
"""

from abc import abstractmethod
from typing import List, Optional, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .expr_type import FuncExpr

class ExprABC:
    """ Expression Abstract class """

    # def op(self, *args): # 1st: op, 2nd: y (optional)
    #     """ Get unary or binary expression """
    #     if len(args) > 2:
    #         raise RuntimeError('Too many arguments.')
    #     if len(args) == 2:
    #         return self.uop(args[0])
    #     return self.bop(args[0], args[1])

    @abstractmethod
    def func(self, op, y):
        """ Binary operation """

    @abstractmethod
    def rfunc(self, op, y):
        """ Reversed binary operation """

    @abstractmethod
    def infunc(self, name, *args):
        """ In-function operation """

    @abstractmethod
    def get_statement_data(self) -> Tuple[bytes, list]:
        """ Get a statement data of this expression """

    @property
    def name_expr(self) -> bytes:
        """ Name Expr """

    @property
    def column_expr(self) -> bytes:
        """ Column Expr """


class FuncABC:
    """ Function ABC """
    _FuncExpr = None

    def __init__(self, name: bytes):
        assert isinstance(name, bytes)
        self._name = name

    def call(self, *args) -> 'FuncExpr':
        return self._FuncExpr(self, *args)

    def __call__(self, *args):
        return self.call(self, *args)

    @property
    def name(self):
        return self._name

    def __bytes__(self):
        return self.name

    def __str__(self) -> str:
        return self.name.decode()

    @abstractmethod
    def get_statement_data_with_args(self, args: list) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments """


class NoArgsFuncABC(FuncABC):
    """ Func with no arguments """
    def __init__(self, name: bytes, returntype: Optional[Type] = None):
        super().__init__(name)
        self._returntype = returntype

    @property
    def returntype(self):
        return self._returntype


class Func(NoArgsFuncABC):
    """ Function """
    def __init__(self, name: bytes, argtypes: Optional[List[Type]] = None, returntype: Optional[Type] = None):
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


    def get_statement_data_with_args(self, args: List[ExprABC]) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        return self._func.name + b'(' + b','.join(b'?' for _ in args) + b')', [*args]

    def __repr__(self):
        return str(self) + '(' + ','.join(repr(t) for t in self._argtypes) if self._argtypes else '' + ')'

    def repr_with_args(self, args: List[ExprABC]) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments (Override) """
        self.check_args(args)
        return str(self) + '(' + ', '.join(map(repr, args)) + ')'


class NoArgsFunc(NoArgsFuncABC):
    """ Func with no arguments """

    def get_statement_data_with_args(self, args: List[ExprABC]) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert not args
        return self._func.name


class OpABC(Func):
    """ Operator """
    def __init__(self, name: bytes, argtypes: Optional[List[Type]] = None, returntype: Optional[Type] = None, priority: Optional[int] = None):
        super().__init__(name, argtypes, returntype)
        self._priority = priority

class UnaryOp(OpABC):
    """ Unary Op """

    def get_statement_data_with_args(self, args: List[ExprABC]) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) == 1
        return self._func.name + args[0], [args[0]]


class BinaryOp(OpABC):
    """ Binary Op """

    def get_statement_data_with_args(self, args: List[ExprABC]) -> Tuple[bytes, list]:
        """ Get a statement data of this function with a given list of arguments (Override) """
        assert len(args) >= 2
        return self._name.join(b'?' for _ in args), [*args]
        # TODO: Priority

