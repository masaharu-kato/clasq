"""
    Definition Object class and subclasses
"""
from abc import abstractproperty
from typing import Generic, Iterator, TYPE_CHECKING, Type, TypeVar, Union

from ..utils.keyset import FrozenKeySetABC, KeySetABC, OrderedFrozenKeySetABC, OrderedKeySetABC
from .query_abc import QueryABC

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
            raise TypeError('Invalid type of value.', val)

    @property
    def raw_name(self) -> bytes:
        return self._raw_name

    def append_to_query_data(self, qd: 'QueryData') -> None:
        qd.append_object_name(self.raw_name)

    def __bytes__(self) -> bytes:
        return self.raw_name

    def __str__(self) -> str:
        return self.raw_name.decode()

    def __eq__(self, obj: object) -> bool:
        if isinstance(obj, ObjectName):
            return self.raw_name == obj.raw_name
        if isinstance(obj, str):
            return str(self) == obj
        if isinstance(obj, bytes):
            return bytes(self) == obj
        return False

    def __ne__(self, obj: object) -> bool:
        return not self.__eq__(obj)

    def __hash__(self) -> int:
        return hash(self.raw_name)

    def __repr__(self) -> str:
        return 'ObjName(%s)' % str(self)


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


class FrozenObjset(FrozenKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class Objset(KeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class OrderedFrozenObjset(FrozenObjset[T], OrderedFrozenKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)


class OrderedObjset(Objset[T], OrderedKeySetABC[T], Generic[T]):

    def _key(self, obj: T):
        return object_key(obj)
