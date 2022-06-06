"""
    Query abstract class
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Collection, Iterable, Iterator

if TYPE_CHECKING:
    from .exprs import QueryArgName, QueryLike, QueryParamOrArg
    from .values import SQLValue
    from .object import ObjectABC


class QueryABC(ABC):
    """ Query abstract class """

    @abstractmethod
    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        """ Append this expression to the QueryABC object

        Args:
            qd (QueryABC): QueryABC object to be appended
        """

    def _iter_objects(self) -> Iterator[ObjectABC]:
        """ Get a columns used in this expression """
        return iter([]) # Default implementation

    def consists_of(self, object_set: Iterable[QueryABC]) -> bool:
        return all(obj in object_set for obj in self._iter_objects())


class QueryDataABC(QueryABC):
    """ Query data abstract class """
    
    @abstractmethod
    def append(self, *vals: QueryLike | None) -> QueryDataABC:
        """ Append values

        Args:
            *vals (QueryLike | None): Value(s) or object(s) to append.
                `None` will be ignored (do nothing).

        Raises:
            ObjectArgTypeError: Invalid type of value.

        Returns:
            QueryData: Self object
        """

    @property
    @abstractmethod
    def stmt(self) -> bytes:
        """ Get a raw SQL statement """

    @property
    @abstractmethod
    def prms(self) -> tuple[SQLValue, ...]:
        """ Get a parameter values """

    @abstractmethod
    def append_one(self, val: QueryLike | None) -> QueryDataABC:
        """ Append a single value

        Args:
            val (QueryLike | None): A value or object to append.
                `None` will be ignored (do nothing).

        Raises:
            QueryTypeError: Invalid type of value.

        Returns:
            QueryData: Self object
        """

    @abstractmethod
    def append_keyword(self, keyword: bytes) -> QueryDataABC:
        """ Append SQL keyword or raw statements
            (Quotes and multiple statements are not allowed.)

        Args:
            keyword (bytes): Keyword bytes to append

        Raises:
            errors.QueryValueError: Keyword has invalid character(s).

        Returns:
            QueryData: Self object
        """

    @abstractmethod
    def append_query_data(self, qd: QueryDataABC) -> QueryDataABC:
        """ Append the other QueryData object 

        Args:
            qd (QueryData): QueryData to append

        Returns:
            QueryData: Self object
        """

    @abstractmethod
    def append_value(self, val: QueryParamOrArg) -> QueryDataABC:
        """ Append as value

        Args:
            val (QueryParamOrArg): Value or query argument object to append

        Raises:
            QueryArgumentError: Different arguments with same name are specified.

        Returns:
            QueryData: Self object
        """

    def append_values(self, *vals: QueryParamOrArg) -> QueryDataABC:
        """ Append multiple args as values

        Args:
            *vals (QueryParamOrArg): Value(s) or query argument object(s) to append

        Returns:
            QueryData: Self object
        """
        for val in vals:
            self.append_value(val)
        return self

    @abstractmethod
    def append_object_name(self, val: bytes) -> QueryDataABC:
        """ Append as object name

        Args:
            val (bytes): Object name to append

        Returns:
            QueryData: Self object
        """
        
    @abstractmethod
    def append_joined_object(self, *vals: QueryLike | None) -> QueryDataABC:
        """ Join values with a default separator (, ) and append

        Returns:
            QueryData: Self object
        """

    @abstractmethod
    def append_joined(self, vals: Iterable, *, sep: bytes | None = None) -> QueryDataABC:
        """ Join a iterable with a specific separator

        Args:
            vals (Iterable): Iterable to join
            sep (bytes, optional): Separator. Defaults to DEFAULT_SEP (, ).

        Returns:
            QueryData: Self object
        """

    @abstractmethod
    def calc_prms(self, argvals: Collection[SQLValue] | dict[QueryArgName, SQLValue], *, ignore_unused=False) -> tuple[SQLValue, ...]:
        pass
    
    @abstractmethod
    def _call(self, argvals: tuple[SQLValue, ...], kwargvals: dict[str, SQLValue], *, ignore_unused=False) -> QueryDataABC:
        """ # TODO: Add docs """


    def __call__(self, *argvals, **kwargvals):
        """ Create a new QueryData instance with query argument values 
            (Synonym of `call` method)
        """
        return self.call(*argvals, **kwargvals)
        
    def call(self, *argvals: SQLValue, **kwargvals: SQLValue) -> QueryDataABC:
        """ Create a new QueryData instance with query argument values

        Args:
            argvals: Positional query argument values (Argument which name type is integer)
            kwargvals: Keyword query argument values (Argument which name type is string)

        Raises:
            QueryArgumentError: The specified argument name does not exists.

        Returns:
            QueryData: New QueryData instance with query argument values
        """
        return self._call(argvals, kwargvals)

    def call_ignore_unused(self, *argvals: SQLValue, **kwargvals: SQLValue) -> QueryDataABC:
        """ Create a new QueryData instance with query argument values
            (Ignore unused arguments)
        """
        return self._call(argvals, kwargvals, ignore_unused=True)

    def calc_prms_many(self, iter_argvals: Iterable[Collection[SQLValue] | dict[QueryArgName, SQLValue]], *, ignore_unused=False) -> Iterator[tuple[SQLValue, ...]]:
        for argvals in iter_argvals:
            yield self.calc_prms(argvals, ignore_unused=ignore_unused)

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        qd.append_query_data(self)


def iter_objects(*exprs: QueryABC | None):
    for expr in exprs:
        if expr is not None:
            yield from expr._iter_objects()

