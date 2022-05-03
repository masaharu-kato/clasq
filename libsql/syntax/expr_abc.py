"""
    Definition ExprType abstract classes
"""
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Tuple

if TYPE_CHECKING:
    from .query_data import QueryData
    from .expr_type import FuncExpr, Object


class ExprABCBase(ABC):
    """ Expression Abstract class """

    def iter_objects(self) -> Iterator['Object']:
        """ Get a columns used in this expression """
        return iter([]) # Default implementation

    @abstractmethod
    def append_query_data(self, qd: 'QueryData') -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """


class FuncABCBase(ABC):
    """ Function ABC """

    def __init__(self, name: bytes):
        assert isinstance(name, bytes)
        self._name = name

    @abstractmethod
    def call(self, *args) -> 'FuncExpr':
        """ Returns an expression representing a call to this function
            (Abstract method)

        Returns:
            FuncExpr: Function call expression
        """

    def __call__(self, *args) -> 'FuncExpr':
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
    def append_query_data_with_args(self, qd:'QueryData', args: Tuple[ExprABCBase, ...]) -> None:
        """ Append a statement data of this function with a given list of arguments
            (Abstract method)
        
        Args:
            qd (QueryData): QueryData object to be appended
            args (list): Argument values for the function
        """

