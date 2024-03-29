"""
    Generic class utils
"""
from __future__ import annotations
import types
import typing

def _ns_set_origin_class(ns, target):
    ns['__orig_class__'] = target


def is_original_generic_cls(cls: typing.Type) -> bool:
    return isinstance(cls, type) and issubclass(cls, typing.Generic) # type: ignore

class GenericArgsBinded:
    """ Generic args are binded """

    def _get_orig_class(self):
        return getattr(self, '__orig_class__')


_binds_cache: dict[typing.Type, typing.Type] = {}

T = typing.TypeVar('T')
def bind_generic_args(cls: typing.Type[T]) -> typing.Type[T]:
    """ Bind generic arguments """

    if isinstance(cls, type) and issubclass(cls, GenericArgsBinded):
        # Already binded
        return cls # type: ignore

    _is_original = is_original_generic_cls(cls)
    _generic_origin = typing.get_origin(cls)

    if not (_is_original or _generic_origin):
        # No binds needed
        return cls
    
    if cls not in _binds_cache:
        origin = cls if _is_original else _generic_origin
        _binds_cache[cls] = types.new_class(
            repr(cls),
            (origin, GenericArgsBinded),
            {},
            lambda ns: _ns_set_origin_class(ns, cls)
        )
        
    return _binds_cache[cls]
