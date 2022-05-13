"""
    Set ABC Definition
"""

from abc import ABC, abstractmethod
from typing import FrozenSet, Generic, Iterable, TypeVar, Union, Set

T = TypeVar('T')


class _FrozenSetABC(ABC, Generic[T]):

    @abstractmethod
    def __init__(self, _iterable: Iterable[T] = ()) -> None:
        """ Init """
        
    @abstractmethod
    def __len__(self) -> int:
        """ Get length """

    @abstractmethod
    def __iter__(self):
        """ Iterate """

    @abstractmethod
    def __contains__(self, val: T) -> bool:
        """ Contains """

    @abstractmethod
    def __le__(self, oset: 'SetLike[T]') -> bool:
        """ Returns if oset contains all values of self """

    @abstractmethod
    def __lt__(self, oset: 'SetLike[T]') -> bool:
        """ Returns if oset contains all values of self and self != oset """

    @abstractmethod
    def __ge__(self, oset: 'SetLike[T]') -> bool:
        """ Returns if self contains all values of oset """

    @abstractmethod
    def __gt__(self, oset: 'SetLike[T]') -> bool:
        """ Returns if self contains all values of oset and self != oset """

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

    def isdisjoint(self, oset: 'SetLike[T]') -> bool:
        return bool(self.__and__(oset))

    def issubset(self, oset: 'SetLike[T]') -> bool:
        """ Returns if oset contains all values of self """
        return self.__le__(oset)

    def issuperset(self, oset: 'SetLike[T]') -> bool:
        """ Returns if self contains all values of oset """
        return self.__ge__(oset)

    def intersection(self, oset: 'SetLike[T]'):
        """ Return intersection (equivalent to `__and__`) """
        return self.__and__(oset)

    def union(self, oset: 'SetLike[T]'):
        """ Return union (equivalent to `__or__`) """
        return self.__or__(oset)

    def difference(self, oset: 'SetLike[T]'):
        """ Return difference (equivalent to `__sub__`) """
        return self.__sub__(oset)

    def symmetric_difference(self, oset: 'SetLike[T]'):
        """ Return symmetric difference (equivalent to `__xor__`) """
        return self.__xor__(oset)
        

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
    def remove(self, val: T) -> None:
        """ Remove value """

    @abstractmethod
    def discard(self, val: T) -> None:
        """ Remove value if value exists """

    # @abstractmethod
    # def pop(self) -> T:
    #     """ Pop last value (Get and remove last value) """

    # @abstractmethod
    # def pop_first(self) -> T:
    #     """ Pop first value (Get and remove first value) """

    @abstractmethod
    def clear(self) -> None:
        """ Clear """

    def update(self, *vals_list: Iterable[T]) -> None:
        """ Update with values (equivalent to `__ior__`) """
        for objs in vals_list:
            self.__ior__(type(self)(objs))

    def intersection_update(self, *vals_list: Iterable[T]) -> None:
        """ Update with values intersection (equivalent to `__iand__`) """
        for objs in vals_list:
            self.__iand__(type(self)(objs))

    def difference_update(self, *vals_list: Iterable[T]) -> None:
        """ Update with values difference (equivalent to `__isub__`) """
        for objs in vals_list:
            self.__isub__(type(self)(objs))

    def symmetric_difference_update(self, *vals_list: Iterable[T]) -> None:
        """ Update with values symmetric difference (equivalent to `__ixor__`) """
        for objs in vals_list:
            self.__ixor__(type(self)(objs))

FrozenSetLike = Union[FrozenSetABC[T], FrozenSet[T]]

SetLike = Union[FrozenSetLike[T], SetABC[T], Set[T]]
