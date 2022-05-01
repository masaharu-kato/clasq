"""
    Definition ExprType abstract classes
"""
from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .query_data import QueryData
    from .expr_type import FuncExpr


class ExprABC:
    """ Expression Abstract class """

    # @abstractmethod
    # def func(self, op, y):
    #     """ Binary operation """

    # @abstractmethod
    # def rfunc(self, op, y):
    #     """ Reversed binary operation """

    # @abstractmethod
    # def infunc(self, name, *args):
    #     """ In-function operation """

    def column_def_expr(self) -> 'ExprABC':
        """ Get a column definition version of this expression """
        return self # Default implementation

    @property
    def name_stmt_bytes(self) -> bytes:
        """ Name Expr """

    def append_query_data(self, qd: 'QueryData') -> None:
        """ Append this to query data"""
        qd.append(self.name_stmt_bytes)


class FuncABC:
    """ Function ABC """

    def __init__(self, name: bytes):
        assert isinstance(name, bytes)
        self._name = name

    @abstractmethod
    def call(self, *args) -> 'FuncExpr':
        """ Call this function (return its expression) """

    def __call__(self, *args):
        return self.call(*args)

    @property
    def name(self):
        return self._name

    def __bytes__(self):
        return self.name

    def __str__(self) -> str:
        return self.name.decode()

    @abstractmethod
    def append_query_data_with_args(self, qd: 'QueryData', args: list) -> None:
        """ Append a statement data of this function with a given list of arguments """

