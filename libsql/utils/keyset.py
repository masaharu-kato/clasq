"""
    KeySet class
"""

from abc import ABC, abstractproperty, abstractmethod
from typing import Iterator

from .ordered_set import OrderedSet


class _FrozenKeySetABC(ABC):
    """ Frozen Set for object ABC (Method definitions) """

    def __init__(self, *objs) -> None:
        """ Init """
        self._key_to_obj = {self._key(obj): obj for obj in objs}
        self._key_set = self._to_keys(objs)

    @abstractmethod
    def _key(self, obj):
        """ Get a key from object """

    def _make_set(self, keys):
        """ Make Set from keys """
        return set(list(keys)) # Default Implementation

    def _to_keys(self, objs):
        return self._make_set(self._key(obj) for obj in objs)

    def _to_objs(self, keys) -> Iterator:
        return (self._key_to_obj[key] for key in keys)

    def __contains__(self, obj):
        key = self._key(obj)
        return key in self._key_set

    def __getitem__(self, key):
        return self._key_to_obj[key]

    def get(self, key):
        return self._key_to_obj.get(key)
        
    def __len__(self) -> int:
        return len(self._key_set)

    def __iter__(self):
        return self._to_objs(self._key_set)

    def __and__(self, objs):
        return type(self)(*self._to_objs(self._key_set.__and__(self._to_keys(objs))))

    def __or__(self, objs):
        return type(self)(*self, *(objs - self))

    def __sub__(self, objs):
        return type(self)(*self._to_objs(self._key_set.__sub__(self._to_keys(objs))))

    def __xor__(self, objs):
        return type(self)(*self._to_objs(self._key_set.__xor__(self._to_keys(objs))))

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join(map(repr, self)))


class FrozenKeySetABC(_FrozenKeySetABC):
    """ Frozen Set for object ABC """


class KeySetABC(_FrozenKeySetABC):

    def __iand__(self, objs):
        self._key_set.__iand__(self._to_keys(objs))
        self._clean_objs()
        return self

    def __ior__(self, objs):
        for obj in objs:
            self.add(obj)
        return self

    def __isub__(self, objs):
        for obj in objs:
            self.remove(obj)
        return self

    def __ixor__(self, objs):
        self._key_set.__ixor__(self._to_keys(objs))
        self._clean_objs()
        return self

    def add(self, obj) -> None:
        key = self._key(obj)
        self._key_to_obj[key] = obj
        self._key_set.add(key)

    def update(self, *objs_list) -> None:
        for objs in objs_list:
            self.__ior__(objs)

    def remove(self, obj) -> None:
        key = self._key(obj)
        self._key_set.remove(key)
        del self._key_to_obj[key]


    def _clean_objs(self):
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



class OrderedFrozenKeySetABC(FrozenKeySetABC):

    def _make_set(self, keys):
        return OrderedSet(keys)


class OrderedKeySetABC(KeySetABC):

    def _make_set(self, keys):
        return OrderedSet(keys)

