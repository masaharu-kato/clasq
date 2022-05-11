"""
    Definition Object class and subclasses
"""
from abc import abstractproperty
from typing import Generic, Iterator, TYPE_CHECKING, Type, TypeVar, Union

from ..utils.keyset import FrozenKeySetABC, KeySetABC, OrderedFrozenKeySetABC, OrderedKeySetABC
from .query_abc import QueryABC
from . import errors

if TYPE_CHECKING:
    from .query_data import QueryData

T = TypeVar('T')


class ObjectName(QueryABC):
    """ Object name """
    def __init__(self, val: 'NameLike'):
        if isinstance(val, ObjectName):
            self._raw_name = val.raw_name
        elif isinstance(val, bytes):
            self._raw_name = val
        elif isinstance(val, str):
            self._raw_name = val.encode()
        else:
            raise TypeError('Invalid type of value.')

    @property
    def raw_name(self):
        return self._raw_name

    def append_to_query_data(self, qd: 'QueryData') -> None:
        qd.append_object_name(self.raw_name)

    def __bytes__(self) -> bytes:
        return self.raw_name

    def __str__(self) -> str:
        return self.raw_name.decode()

    def __eq__(self, obj: object) -> bool:
        return isinstance(obj, ObjectName) and self.raw_name == obj.raw_name

    def __hash__(self) -> int:
        return hash(self.raw_name)


NameLike = Union[bytes, str, ObjectName]


class ObjectABC(QueryABC):

    @abstractproperty
    def name(self) -> ObjectName:
        """ Get a name """

    def __bytes__(self):
        return bytes(self.name)

    def __str__(self):
        return str(self.name)

    def iter_objects(self) -> Iterator['ObjectABC']:
        yield self # Default implementation

    def __eq__(self, val) -> bool:
        if isinstance(val, ObjectABC):
            return type(self) == type(val) and self.name == val.name # Default Implementation
        raise TypeError('Invalid type value', val)

    def __ne__(self, val) -> bool:
        return not self.__eq__(val)

    def __repr__(self):
        return 'Obj(%s)' % self.name.decode()


class Object(ObjectABC):
    """ Column expression """
    def __init__(self, name: NameLike):
        self._name = ObjectName(name)

    @property
    def name(self) -> ObjectName:
        return self._name

    def append_to_query_data(self, qd: 'QueryData') -> None:
        qd.append(self.name) # Default Implementation



def object_key(obj):
    return id(obj)


class FrozenObjectSet(FrozenKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class ObjectSet(KeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class OrderedFrozenObjectSet(OrderedFrozenKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class OrderedObjectSet(OrderedKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)
