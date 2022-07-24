"""
    Definition Object class and subclasses
"""
from __future__ import annotations
from abc import abstractmethod
from typing import Hashable, Iterator

from ...errors import ObjectNotSetError
from .query import QueryABC, QueryDataABC


class ObjectName(QueryABC):
    """ Object name """
    def __init__(self, val: NameLike):
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

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
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

    def __add__(self, obj: object) -> ObjectName:
        if isinstance(obj, ObjectName):
            return ObjectName(self.raw_name + b'.' + obj.raw_name)
        if isinstance(obj, bytes):
            return ObjectName(self.raw_name + obj)
        return ObjectName(str(self) + str(obj))

    def __iadd__(self, obj: object) -> ObjectName:
        if isinstance(obj, ObjectName):
            self._raw_name += b'.' + obj.raw_name
        if isinstance(obj, bytes):
            self._raw_name += obj
        else:
            self._raw_name = (str(self) + str(obj)).encode()
        return self

    def __mod__(self, vals) -> ObjectName:
        if isinstance(vals, (tuple, list)):
            return ObjectName(self.raw_name % (*(bytes(v) for v in vals),))
        return ObjectName(self.raw_name % bytes(vals))


    def __hash__(self) -> int:
        return hash(self.raw_name)

    def __repr__(self) -> str:
        return 'ObjName(%s)' % str(self)


NameLike = bytes | str | ObjectName



class ObjectABC(QueryABC, Hashable):
    """ Object abstract class """

    @property
    @abstractmethod
    def _name(self) -> ObjectName:
        """ Get a object name """
        raise NotImplementedError()
        
    # @abstractmethod
    # def _set_name(self, name: NameLike | None) -> None:
    #     """ Set a object name """
    #     raise NotImplementedError()
        
    def get_raw_name(self):
        return self._name.raw_name

    def __bytes__(self):
        return bytes(self._name)

    def __str__(self):
        return str(self._name)

    def _iter_objects(self) -> Iterator[ObjectABC]:
        yield self # Default implementation

    def __eq__(self, val) -> bool:
        try:
            if isinstance(val, ObjectABC):
                return type(self) == type(val) and self._name == val._name # Default Implementation
            # raise TypeError('Invalid type value', self, val)
        except (TypeError, NotImplementedError, ObjectNotSetError):
            pass
        return super().__eq__(val)

    def __ne__(self, val) -> bool:
        return not self.__eq__(val)

    def __hash__(self):
        try:
            _name = self._name
        # `self._name` may reads uninitialized properties or attributes
        except (NotImplementedError, AttributeError, ObjectNotSetError):
            _name = None
        if _name is not None:
            return hash((self.__class__, _name.raw_name))
        return super().__hash__()


class ObjectWithNamePropABC(ObjectABC):
    """ Object abstract class """

    @property
    def name(self) -> ObjectName:
        """ Get a object name """
        return self._name

    @property
    def raw_name(self):
        """ Get a object name """
        return self.get_raw_name()


class Object(ObjectWithNamePropABC):
    """ Column expression """
    def __init__(self, name: NameLike):
        self.__name = ObjectName(name)

    # def __init__(self, name: NameLike | None):
    #     self.__name = ObjectName(name) if name is not None else None

    @property
    def _name(self) -> ObjectName:
        """ Get a object name (Override from `ObjectABC`) """
        # if self.__name is None:
        #     raise ObjectNotSetError('Name is not set.')
        return self.__name

    # def _set_name(self, name: NameLike | None) -> None:
    #     self.__name = ObjectName(name) if name is not None else None

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        qd.append(self.name) # Default Implementation
        
    def __repr__(self) -> str:
        try:
            _name = str(self._name)
        except (AttributeError, NotImplementedError):
            _name = '<unnamed>'
        return '%s(%s)' % (type(self).__name__, _name)
