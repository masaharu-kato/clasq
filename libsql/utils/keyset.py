"""
    KeySet class
"""

from abc import abstractproperty, abstractmethod
import itertools
from typing import Dict, Generic, Hashable, Iterable, Iterator, Optional, TypeVar, Union

from .abc.set import FrozenSetABC, FrozenSetLike, NonFrozenSetLike, SetABC, SetLike
from .ordered_set import FrozenOrderedSet, OrderedSet

K = TypeVar('K', bound=Hashable)
T = TypeVar('T')

class _FrozenKeySetABC(FrozenSetABC[T], Generic[K, T]):
    """ Frozen Set for object ABC (Method definitions) """

    @abstractproperty
    def _key_to_obj(self) -> Dict[K, T]:
        pass

    @abstractproperty
    def _key_set(self) -> SetLike[K]:
        pass

    @abstractmethod
    def _key(self, obj: T) -> K:
        """ Get a key from object """

    @abstractmethod
    def _key_or_none(self, obj: Union[K, T]) -> Optional[K]:
        """ Get a key from object if obj is a valid type """

    def _make_fset(self, keys) -> FrozenSetLike[K]:
        """ Make Set from keys """
        return frozenset(list(keys)) # Default Implementation

    def _to_key_fset(self, objs: Iterable[T]):
        return self._make_fset(self._key(obj) for obj in objs)

    def _to_objs(self, keys) -> Iterator[T]:
        return (self._key_to_obj[key] for key in keys)

    def __contains__(self, obj: Union[K, T]) -> bool:
        if key := self._key_or_none(obj):
            return key in self._key_set and obj is self._key_to_obj[key]
        return obj in self._key_set

    def __getitem__(self, key: K) -> T:
        return self._key_to_obj[key]

    def get(self, key: K) -> Optional[T]:
        return self._key_to_obj.get(key)
        
    def __len__(self) -> int:
        return len(self._key_set)

    def __iter__(self) -> Iterator[T]:
        return self._to_objs(self._key_set)

    def __and__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._key_set.__and__(self._to_key_fset(objs))))

    def __or__(self, objs: SetLike[T]):
        return type(self)(itertools.chain(self, objs - self))

    def __sub__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._key_set.__sub__(self._to_key_fset(objs))))

    def __xor__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._key_set.__xor__(self._to_key_fset(objs))))

    def __rand__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._to_key_fset(objs).__and__(self._key_set)))

    def __ror__(self, objs: SetLike[T]):
        return type(self)(itertools.chain(objs, self - objs))

    def __rsub__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._to_key_fset(objs).__sub__(self._key_set)))

    def __rxor__(self, objs: SetLike[T]):
        return type(self)(self._to_objs(self._to_key_fset(objs).__xor__(self._key_set)))

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


class FrozenKeySetABC(_FrozenKeySetABC[K, T], Generic[K, T]):
    """ Frozen Set for object ABC """

    def __init__(self, objs: Iterable[T] = ()) -> None:
        """ Init """
        self.__key_to_obj = {self._key(obj): obj for obj in objs}
        self.__key_fset = self._make_fset(self._key_to_obj.keys())

    @property
    def _key_to_obj(self) -> Dict[K, T]:
        return self.__key_to_obj

    @property
    def _key_set(self) -> SetLike[K]:
        return self.__key_fset


class KeySetABC(SetABC[T], _FrozenKeySetABC[K, T], Generic[K, T]):

    def __init__(self, objs: Iterable[T] = ()) -> None:
        """ Init """
        self.__key_to_obj = {self._key(obj): obj for obj in objs}
        self.__key_fset = self._make_set(self._key_to_obj.keys())

    @property
    def _key_to_obj(self) -> Dict[K, T]:
        return self.__key_to_obj

    @property
    def _key_set(self) -> SetLike[K]:
        return self.__key_fset

    @property
    def _key_nfset(self) -> NonFrozenSetLike[K]:
        return self.__key_fset

    def _make_set(self, keys) -> NonFrozenSetLike[K]:
        """ Make Set from keys """
        return set(list(keys)) # Default Implementation

    def __iand__(self, objs: SetLike[T]):
        self._key_nfset.__iand__(self._to_key_fset(objs))
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
        self._key_nfset.__ixor__(self._to_key_fset(objs))
        self._clean_objs()
        return self

    def add(self, obj: T) -> None:
        key = self._key(obj)
        self._key_to_obj[key] = obj
        self._key_nfset.add(key)

    def update(self, *objs_list: Iterable[T]) -> None:
        for objs in objs_list:
            for obj in objs:
                self.add(obj)

    def remove(self, obj: T) -> None:
        key = self._key(obj)
        self._key_nfset.remove(key)
        del self._key_to_obj[key]

    def _clean_objs(self) -> None:
        for key in self._key_to_obj:
            if key not in self._key_nfset:
                del self._key_to_obj[key]

    def discard(self, obj: T) -> None:
        key = self._key(obj)
        if key in self._key_to_obj:
            self.remove(obj)

    def clear(self) -> None:
        self._key_nfset.clear()
        self._key_to_obj.clear()


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



class FrozenOrderedKeySetABC(FrozenKeySetABC[K, T], Generic[K, T]):

    def _make_fset(self, keys):
        return FrozenOrderedSet(keys)


class OrderedKeySetABC(KeySetABC[K, T], Generic[K, T]):

    def _make_set(self, keys):
        return OrderedSet(keys)

