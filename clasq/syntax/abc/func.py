"""

"""
from __future__ import annotations
from abc import abstractmethod
from typing import Iterator

from .object import NameLike, ObjectABC, Object 
from .query import QueryABC, QueryDataABC
from .exprs import ExprABC, ExprLike, QueryExprABC

class FuncABC(Object):
    """ Function ABC """

    @abstractmethod
    def _func_call(self, *nn_args: ExprLike) -> FuncCallABC:
        """ Internal function call

        Returns:
            ExprABC: Function Call object
        """

    @abstractmethod
    def check_args(self, args: tuple[ExprLike, ...]) -> None:
        """ Check the arguments
            (Raise exception if there are errors)

        Args:
            args (tuple[ExprLike, ...]): Argument values
        """

    @abstractmethod
    def append_query_data_with_args(self, qd: QueryDataABC, args: tuple[ExprLike, ...]) -> None:
        """ Append a statement data of this function with a given list of arguments
            (Abstract method)
        
        Args:
            qd (QueryData): QueryData object to be appended
            args (list): Argument values for the function
        """

    @abstractmethod
    def repr_with_args(self, args: tuple[ExprLike, ...]) -> str:
        """ Get a statement data of this function with a given list of arguments """

    def call(self, *args: ExprLike) -> ExprABC:
        """ Returns an expression representing a call to this function

        Returns:
            FuncExpr: Function call expression
        """
        self.check_args(args)
        return self._func_call(*args)

    def __call__(self, *args) -> ExprABC:
        """ Returns an expression representing a call to this function
            Synonym of `call` method.

        Returns:
            FuncExpr: Function call expression
        """
        return self.call(*args)

    def __bytes__(self):
        """ Returns a function name
            Synonym of `name` method.

        Returns:
            bytes: Function name
        """
        return bytes(self.name)

    def __str__(self) -> str:
        """ Returns a function name as string

        Returns:
            str: Function name
        """
        return str(self.name)


class FuncWithNoArgsABC(FuncABC):
    """ Func with no arguments """
    def __init__(self, name: NameLike, returntype=None):
        super().__init__(name)
        self._returntype = returntype

    @property
    def returntype(self):
        return self._returntype

    def repr_with_args(self, args: tuple[ExprLike, ...]) -> str:
        """ Get a statement data of this function with a given list of arguments
            (Overrided from `FuncABC` class)
        """
        return str(self)


class FuncCallABC(QueryExprABC): 
    """ Function call ABC """

    @property
    @abstractmethod
    def func(self) -> FuncABC:
        """ Get a function to be called """ 

    @property
    @abstractmethod
    def args(self) -> tuple:
        """ Get a tuple of arguments for call """

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        self.func.append_query_data_with_args(qd, self.args)

    def _iter_objects(self) -> Iterator[ObjectABC]:
        for arg in self.args:
            if isinstance(arg, QueryABC):
                yield from arg._iter_objects()

    def __repr__(self):
        return self._func.repr_with_args(self._args)
