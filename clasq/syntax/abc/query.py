"""
    Query abstract class
"""
from __future__ import annotations
from abc import abstractmethod
from typing import TYPE_CHECKING, Iterable, Iterator

if TYPE_CHECKING:
    from ..query_data import QueryData
    from .object import ObjectABC


class QueryABC:
    """ Query abstract class """

    @abstractmethod
    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """

    def iter_objects(self) -> Iterator[ObjectABC]:
        """ Get a columns used in this expression """
        return iter([]) # Default implementation

    def consists_of(self, object_set: Iterable[QueryABC]) -> bool:
        return all(obj in object_set for obj in self.iter_objects())


def iter_objects(*exprs: QueryABC | None):
    for expr in exprs:
        if expr is not None:
            yield from expr.iter_objects()

