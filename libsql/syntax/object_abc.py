"""
    Definition Object class and subclasses
"""
from abc import abstractproperty
from typing import Iterator, TYPE_CHECKING, Type, Union

from .query_abc import QueryABC

if TYPE_CHECKING:
    from .query_data import QueryData

Name = Union[bytes, str]


class ObjectABC(QueryABC):

    @abstractproperty
    def name(self) -> bytes:
        """ Get a name """

    def __bytes__(self):
        return self.name

    def __str__(self):
        return self.name.decode()

    def query_for_select_column(self) -> tuple:
        """ Get a query for SELECT """
        return (self,) # Default implementation

    def iter_objects(self) -> Iterator['ObjectABC']:
        yield self # Default implementation

    # def q_create(self) -> tuple:
    #     """ Get a query for creation """
    #     return (self,) # Default implementation

    # def __eq__(self, value) -> bool:
    #     if isinstance(value, type(self)):
    #         return self.name == value.name
    #     return super().__eq__(value)

    # @property
    # def view_or_none(self) -> Optional['ViewABC']:
    #     return None # Default Implementation

    # @property
    # def table_or_none(self) -> Optional['Table']:
    #     return None # Default Implementation

    def __eq__(self, val) -> bool:
        if isinstance(val, ObjectABC):
            return type(self) == type(val) and self.name == val.name # Default Implementation
        raise TypeError('Invalid type value', val)

    def __ne__(self, val) -> bool:
        return not self.__eq__(val)

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self):
        return 'Obj(%s)' % str(self)


def to_name(val) -> bytes:
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return val.encode()
    raise TypeError('Invalid type of value.')


class Object(ObjectABC):
    """ Column expression """
    def __init__(self, name: Name):
        self._name = to_name(name)

    @property
    def name(self):
        return self._name

    def append_query_data(self, qd: 'QueryData') -> None:
        qd.append_object(self.name) # Default Implementation
