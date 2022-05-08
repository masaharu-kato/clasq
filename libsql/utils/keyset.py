"""
    KeySet class
"""

from abc import ABC, abstractproperty, abstractmethod
from typing import Generic, Hashable, Iterable, Iterator, Optional, TypeVar

from .set_abc import FrozenSetABC, SetABC, SetLike
from .ordered_set import FrozenOrderedSet, OrderedSet

T = TypeVar('T')

class _FrozenKeySetABC(FrozenSetABC[T], Generic[T]):
    """ Frozen Set for object ABC (Method definitions) """

    def __init__(self, *objs: T) -> None:
        """ Init """
        self._key_to_obj = {self._key(obj): obj for obj in objs}
        self._key_set = self._to_keys(objs)

    @abstractmethod
    def _key(self, obj) -> Hashable:
        """ Get a key from object """

    def _make_set(self, keys) -> SetLike[T]:
        """ Make Set from keys """
        return set(list(keys)) # Default Implementation

    def _to_keys(self, objs: Iterable[T]):
        return self._make_set(self._key(obj) for obj in objs)

    def _to_objs(self, keys) -> Iterator[T]:
        return (self._key_to_obj[key] for key in keys)

    def __contains__(self, obj:T) -> bool:
        key = self._key(obj)
        return key in self._key_set

    def __getitem__(self, key: Hashable) -> T:
        return self._key_to_obj[key]

    def get(self, key: Hashable) -> Optional[T]:
        return self._key_to_obj.get(key)
        
    def __len__(self) -> int:
        return len(self._key_set)

    def __iter__(self) -> Iterator[T]:
        return self._to_objs(self._key_set)

    def __and__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__and__(self._to_keys(objs))))

    def __or__(self, objs: SetLike[T]):
        return type(self)(*self, *(objs - self))

    def __sub__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__sub__(self._to_keys(objs))))

    def __xor__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__xor__(self._to_keys(objs))))

    def __rand__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__rand__(self._to_keys(objs))))

    def __ror__(self, objs: SetLike[T]):
        return type(self)(*objs, *(self - objs))

    def __rsub__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__rsub__(self._to_keys(objs))))

    def __rxor__(self, objs: SetLike[T]):
        return type(self)(*self._to_objs(self._key_set.__rxor__(self._to_keys(objs))))

    def __le__(self, objs: SetLike[T]) -> bool:
        """ Returns if objs contains all values of self """
        return all(v in objs for v in self)

    def __lt__(self, objs: SetLike[T]) -> bool:
        """ Returns if objs contains all values of self and self != objs """
        return self.__le__(objs) and (objs - self)

    def __ge__(self, objs: SetLike[T]) -> bool:
        """ Returns if self contains all values of objs """
        return all(v in self for v in objs)

    def __gt__(self, objs: SetLike[T]) -> bool:
        """ Returns if self contains all values of objs and self != objs """
        return self.__ge__(objs) and (self - objs)

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join(map(repr, self)))


class FrozenKeySetABC(_FrozenKeySetABC[T], Generic[T]):
    """ Frozen Set for object ABC """


class KeySetABC(SetABC[T], _FrozenKeySetABC[T], Generic[T]):

    def __iand__(self, objs: SetLike[T]):
        self._key_set.__iand__(self._to_keys(objs))
        self._clean_objs()
        return self

    def __ior__(self, objs: SetLike[T]):
        for obj in objs:
            self.add(obj)
        return self

    def __isub__(self, objs: SetLike[T]):
        for obj in objs:
            self.remove(obj)
        return self

    def __ixor__(self, objs: SetLike[T]):
        self._key_set.__ixor__(self._to_keys(objs))
        self._clean_objs()
        return self

    def add(self, obj: T) -> None:
        key = self._key(obj)
        self._key_to_obj[key] = obj
        self._key_set.add(key)

    def update(self, *objs_list: Iterable[T]) -> None:
        for objs in objs_list:
            for obj in objs:
                self.add(obj)

    def remove(self, obj: T) -> None:
        key = self._key(obj)
        self._key_set.remove(key)
        del self._key_to_obj[key]


    def _clean_objs(self) -> None:
        for key in self._key_to_obj:
            if key not in self._key_set:
                del self._key_to_obj[key]


    # def difference(self, *s: Iterable[Any]) -> set[_T]: ...
    # def difference_update(self, *s: Iterable[Any]) -> None: ...
    # def discard(self, __element: _T) -> None: ...
    # def intersection(self, *s: Iterable[Any]) -> set[_T]: ...
    # def intersection_update(self, *s: Iterable[Any]) -> None: ...
    # def isdisjoint(self, __s: Iterable[Any]) -> bool: ...
    # def issubset(self, __s: Iterable[Any]) -> bool: ...
    # def issuperset(self, __s: Iterable[Any]) -> bool: ...
    # def symmetric_difference(self, __s: Iterable[_T]) -> set[_T]: ...
    # def symmetric_difference_update(self, __s: Iterable[_T]) -> None: ...
    # def __le__(self, __s: AbstractSet[object]) -> bool: ...
    # def __lt__(self, __s: AbstractSet[object]) -> bool: ...
    # def __ge__(self, __s: AbstractSet[object]) -> bool: ...
    # def __gt__(self, __s: AbstractSet[object]) -> bool: ...



class OrderedFrozenKeySetABC(FrozenKeySetABC[T], Generic[T]):

    def _make_set(self, keys):
        return FrozenOrderedSet(keys)


class OrderedKeySetABC(KeySetABC[T], Generic[T]):

    def _make_set(self, keys):
        return OrderedSet(keys)

