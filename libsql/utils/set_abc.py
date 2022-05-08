"""
    Set ABC Definition
"""

from abc import ABC, abstractmethod
from typing import FrozenSet, Generic, Iterable, TypeVar, Union, Set

T = TypeVar('T')


class _FrozenSetABC(ABC, Generic[T]):

    @abstractmethod
    def __contains__(self, val: T) -> bool:
        """ Contains """
        
    @abstractmethod
    def __len__(self) -> int:
        """ Get length """

    @abstractmethod
    def __iter__(self):
        """ Iterate """

    @abstractmethod
    def __and__(self, oset: 'SetLike[T]'):
        """ Return AND """

    @abstractmethod
    def __or__(self, oset: 'SetLike[T]'):
        """ Return OR """

    @abstractmethod
    def __sub__(self, oset: 'SetLike[T]'):
        """ Return Sub """

    @abstractmethod
    def __xor__(self, oset: 'SetLike[T]'):
        """ Return XOR """

    @abstractmethod
    def __rand__(self, oset: 'SetLike[T]'):
        """ Return reversed AND """

    @abstractmethod
    def __ror__(self, oset: 'SetLike[T]'):
        """ Return reversed OR """

    @abstractmethod
    def __rsub__(self, oset: 'SetLike[T]'):
        """ Return reversed Sub """

    @abstractmethod
    def __rxor__(self, oset: 'SetLike[T]'):
        """ Return reversed XOR """
        

class FrozenSetABC(_FrozenSetABC[T], Generic[T]):
    """ Frozen Set ABC """


class SetABC(_FrozenSetABC[T], Generic[T]):
    """ Set ABC """

    @abstractmethod
    def __iand__(self, oset: 'SetLike[T]'):
        """ AND """

    @abstractmethod
    def __ior__(self, oset: 'SetLike[T]'):
        """ OR """

    @abstractmethod
    def __isub__(self, oset):
        """ Sub """

    @abstractmethod
    def __ixor__(self, oset):
        """ XOR """
        
    @abstractmethod
    def add(self, val: T) -> None:
        """ Add value """

    @abstractmethod
    def update(self, *vals_list: Iterable[T]) -> None:
        """ Update with values """

    @abstractmethod
    def remove(self, val: T) -> None:
        """ Remove value """

FrozenSetLike = Union[FrozenSetABC[T], FrozenSet[T]]

SetLike = Union[FrozenSetLike[T], SetABC[T], Set[T]]
