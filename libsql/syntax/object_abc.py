"""
    Definition Object class and subclasses
"""
from abc import abstractproperty
from typing import Iterator, TYPE_CHECKING, Union

from .query_abc import QueryABC

if TYPE_CHECKING:
    from .query_data import QueryData


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

    def __add__(self, obj: object) -> 'ObjectName':
        if isinstance(obj, ObjectName):
            return ObjectName(self.raw_name + b'.' + obj.raw_name)
        if isinstance(obj, bytes):
            return ObjectName(self.raw_name + obj)
        return ObjectName(str(self) + str(obj))

    def __iadd__(self, obj: object) -> 'ObjectName':
        if isinstance(obj, ObjectName):
            self._raw_name += b'.' + obj.raw_name
        if isinstance(obj, bytes):
            self._raw_name += obj
        else:
            self._raw_name = (str(self) + str(obj)).encode()
        return self

    def __mod__(self, vals) -> 'ObjectName':
        if isinstance(vals, (tuple, list)):
            return ObjectName(self.raw_name % (*(bytes(v) for v in vals),))
        return ObjectName(self.raw_name % bytes(vals))


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
