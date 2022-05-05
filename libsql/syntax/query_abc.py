"""
    Query abstract class
"""
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Optional

if TYPE_CHECKING:
    from .query_data import QueryData
    from .object_abc import ObjectABC


class QueryABC(ABC):
    """ Query abstract class """

    @abstractmethod
    def append_query_data(self, qd: 'QueryData') -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """

    def iter_objects(self) -> Iterator['ObjectABC']:
        """ Get a columns used in this expression """
        return iter([]) # Default implementation


def iter_objects(*exprs: Optional[QueryABC]):
    for expr in exprs:
        if expr is not None:
            yield from expr.iter_objects()

