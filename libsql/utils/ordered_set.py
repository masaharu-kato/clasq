"""
    Ordered Set
"""

from typing import Generic, Iterable, TypeVar

from .set_abc import FrozenSetABC, SetABC, SetLike

T = TypeVar('T')

class FrozenOrderedSet(FrozenSetABC[T], Generic[T]):
    """ Frozen Ordered Set """

    def __init__(self, _iterable: Iterable[T]) -> None:
        """ Init """
        self._dict = {v: None for v in _iterable}

    def __contains__(self, val: T) -> bool:
        return val in self._dict
        
    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __le__(self, oset: SetLike[T]) -> bool:
        """ Returns if oset contains all values of self """
        return all(v in oset for v in self)

    def __lt__(self, oset: SetLike[T]) -> bool:
        """ Returns if oset contains all values of self and self != oset """
        return self.__le__(oset) and (oset - self)

    def __ge__(self, oset: SetLike[T]) -> bool:
        """ Returns if self contains all values of oset """
        return all(v in self for v in oset)

    def __gt__(self, oset: SetLike[T]) -> bool:
        """ Returns if self contains all values of oset and self != oset """
        return self.__ge__(oset) and (self - oset)

    def __and__(self, oset: SetLike[T]):
        return type(self)(v for v in self._dict if v in oset)

    def __or__(self, oset: SetLike[T]):
        return type(self)((*self._dict, *oset))

    def __sub__(self, oset: SetLike[T]):
        return type(self)(v for v in self._dict if v not in oset)

    def __xor__(self, oset: SetLike[T]):
        return type(self)((*(v for v in self._dict if v not in oset), *(v for v in oset if v not in self._dict)))

    def __rand__(self, oset: SetLike[T]):
        return OrderedSet(v for v in oset if v in self._dict)

    def __ror__(self, oset: SetLike[T]):
        return OrderedSet((*oset, *(v for v in self._dict if v not in oset)))

    def __rsub__(self, oset: SetLike[T]):
        return OrderedSet(v for v in oset if v not in self._dict)

    def __rxor__(self, oset: SetLike[T]):
        return type(self)((*(v for v in oset if v not in self._dict), *(v for v in self._dict if v not in oset)))


class OrderedSet(SetABC[T], FrozenOrderedSet[T], Generic[T]):
    """ Ordered Set """
    
    def __iand__(self, oset: SetLike[T]):
        for v in list(self._dict):
            if v not in oset:
                del self._dict[v]
        return self

    def __ior__(self, oset: SetLike[T]):
        for v in oset:
            self.add(v)
        return self

    def __isub__(self, oset: SetLike[T]):
        for v in list(self._dict):
            if v in oset:
                self.remove(v)
        return self

    def __ixor__(self, oset: SetLike[T]):
        com = self.__and__(oset)
        self.__ior__(oset)
        self.__isub__(com)
        return self
        
    def add(self, val: T) -> None:
        if val not in self._dict:
            self._dict[val] = None

    def discard(self, val: T) -> None:
        if val in self._dict:
            self.remove(val)

    def remove(self, val: T) -> None:
        del self._dict[val]

    def pop(self) -> T:
        last_val = list(self._dict)[-1]
        self.remove(last_val)
        return last_val

    def pop_first(self) -> T:
        first_val = list(self._dict)[0]
        self.remove(first_val)
        return first_val

    def clear(self) -> None:
        self._dict.clear()
