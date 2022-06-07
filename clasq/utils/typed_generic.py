"""
    Generic class utils
"""
from __future__ import annotations
from abc import ABC, ABCMeta
# import enum
import types
import typing


class GenericArgumentsNotSpecifiedError(RuntimeError):
    def __init__(self, *args: object) -> None:
        super().__init__('Generic arguments are not specified.')

class TypedGenericMeta(type):
    """ Typed Generic class """
    
    def __new__(cls, name: str, bases: tuple, attrs: dict):
        """ New class """
        origin: type[TypedGenericMeta] | None = attrs.get('__generic_origin')
        if origin is None:
            # Read the typing generic parameter names (TypeVar objects)
            if orig_bases := attrs.get('__orig_bases__'):
                latest_base = orig_bases[-1]
                typing_typevars: tuple[typing.TypeVar, ...] = getattr(latest_base, '__parameters__')
            else:
                typing_typevars = ()
            argkeys = tuple(typevar.__name__ for typevar in typing_typevars)
            arg_index_from_key = {key: i for i, key in enumerate(argkeys)}
            if not len(argkeys) == len(arg_index_from_key):
                raise RuntimeError('Duplicate generic parameter names.')
            attrs['_generic_argkeys'] = argkeys
            attrs['_generic_arg_index_from_key'] = arg_index_from_key

            # Init a new class cache
            generic_cls_cache: dict[tuple, type[TypedGenericMeta]] = {}
            attrs['_generic_cls_cache'] = generic_cls_cache

        else:
            attrs['_generic_argkeys'] = getattr(origin, '_generic_argkeys')
            attrs['_generic_arg_index_from_key'] = getattr(origin, '_generic_arg_index_from_key')

        return type.__new__(cls, name, bases, attrs)

    @property
    def _typing_generic(cls) -> type:
        if (type_ := getattr(cls, '__typing_generic', None)) is None:
            raise GenericArgumentsNotSpecifiedError()
        return type_

    @property
    def _generic_origin(cls) -> type[TypedGenericMeta]:
        if (origin := getattr(cls, '__generic_origin', None)) is None:
            raise GenericArgumentsNotSpecifiedError()
        return origin
        
    @property
    def _generic_argvals(cls) -> tuple:
        if (argvals := getattr(cls, '__generic_argvals', None)) is None:
            raise GenericArgumentsNotSpecifiedError()
        return argvals

    def _generic_arg(cls, _val: typing.TypeVar) -> typing.Any:
        if isinstance(_val, typing.TypeVar):
            return cls._generic_arg_of_index(getattr(cls, '_generic_arg_index_from_key')[_val.__name__])
        raise TypeError('Invalid type of value.')

    def _generic_arg_of_index(cls, i: int) -> typing.Any:
        val = cls._generic_argvals[i]
        if typing.get_origin(val) is typing.Literal:
            _vals = typing.get_args(val)
            return _vals[0] if len(_vals) == 1 else _vals
        return val

    def __rmatmul__(cls, _val):
        if isinstance(_val, typing.TypeVar):
            return cls._generic_arg(_val)
        return NotImplemented

    def __getitem__(cls, _val):
        argvals = _val if isinstance(_val, tuple) else (_val, )
        contains_typevars = any(isinstance(v, typing.TypeVar) for v in argvals)
        _typing_generic = cls.__class_getitem__(argvals)
        if contains_typevars:
            return _typing_generic

        if not len(cls._generic_argkeys) == len(argvals):
            raise RuntimeError('Invalid number of generic arguments.', cls._generic_argkeys, argvals)

        # argdict = dict(zip(cls._generic_argkeys, argvals))
        # print(cls, '__getitem__ called.', argdict, _typing_generic)
        cls_name = '%s[%s]' % (cls.__name__, ', '.join((_to_name(c) for c in argvals)))

        if not argvals in cls._generic_cls_cache:
            cls._generic_cls_cache[argvals] = types.new_class(
                cls_name, #  repr(_typing_generic),
                (cls, ),
                {},
                lambda ns: cls.__bind_generics(ns, _typing_generic, argvals)
            )
        return cls._generic_cls_cache[argvals]

    def __bind_generics(cls, ns: dict, _typing_generic: type, argvals: tuple):
        ns['__typing_generic'] = _typing_generic
        ns['__generic_origin'] = cls # typing.get_origin(_typing_generic)
        ns['__generic_argvals'] = argvals # typing.get_args(_typing_generic)

    def __repr__(self):
        return '<class %s>' % self.__name__

def _to_name(cls: type) -> str:
    """ Convert type to name """
    if origin := typing.get_origin(cls):
        return '%s[%s]' % (_to_name(origin), ', '.join(_to_name(v) for v in typing.get_args(cls)))

    if hasattr(cls, '__name__'):
        return getattr(cls, '__name__')

    return str(cls)


class TypedGenericABCMeta(TypedGenericMeta, ABCMeta):
    """ """


class OptionalTypedGenericMeta(TypedGenericMeta):
        
    @property
    def _generic_argvals(cls) -> tuple:
        try:
            return super()._generic_argvals
        except GenericArgumentsNotSpecifiedError:
            pass
        return ()

    def _generic_arg_of_index(cls, i: int) -> typing.Any:
        try:
            return super()._generic_arg_of_index(i)
        except IndexError:
            pass
        return None


class OptionalTypedGenericABCMeta(OptionalTypedGenericMeta, TypedGenericABCMeta):
    """ """

T = typing.TypeVar('T')

class TypedGenericABC(ABC, typing.Generic[T], metaclass=TypedGenericABCMeta):
    """ Typed Generic ABC """    
    def __rmatmul__(self, _val):
        if isinstance(_val, typing.TypeVar):
            return type(self)._generic_arg(_val)
        return NotImplemented

class TypedGeneric(typing.Generic[T], metaclass=TypedGenericMeta):
    """ Typed Generic """
    def __rmatmul__(self, _val):
        if isinstance(_val, typing.TypeVar):
            return type(self)._generic_arg(_val)
        return NotImplemented
    
class OptionalTypedGenericABC(TypedGenericABC[T], typing.Generic[T], metaclass=OptionalTypedGenericABCMeta):
    """ Optional Typed Generic ABC """
    
class OptionalTypedGeneric(TypedGeneric[T], typing.Generic[T], metaclass=OptionalTypedGenericMeta):
    """ Optional Typed Generic ABC """

# # GenericParamType = typing.Literal | type
# # GenericLiteralType = None | bool | int | str | bytes | enum.Enum


# def is_original_generic_cls(cls: typing.Type) -> bool:
#     return isinstance(cls, type) and issubclass(cls, typing.Generic) # type: ignore

# # T = typing.TypeVar('T')
# class GenericArgsBinded(typing.Generic[T]):
#     """ Generic args are binded """

#     @property
#     def _orig_class(cls):
#         return getattr(cls, '__orig_class__')

#     @property
#     def _generic_origin(cls) -> type:
#         return getattr(cls, '__generic_origin')
        
#     @property
#     def _generic_args(cls) -> tuple:
#         return getattr(cls, '__generic_args')


# _binds_cache: dict[typing.Type, type[GenericArgsBinded]] = {}


# def __bind_generics(ns: dict[str, typing.Any], orig_class: type) -> None:
#     ns['__orig_class__'] = orig_class
#     ns['__generic_origin'] = typing.get_origin(orig_class)
#     ns['__generic_args'] = typing.get_args(orig_class)


# def bind_generic_args(cls: type[T]) -> type[GenericArgsBinded[T]]:
#     """ Bind generic arguments """

#     if isinstance(cls, type) and issubclass(cls, GenericArgsBinded):
#         # Already binded
#         return cls # type: ignore

#     _is_original = is_original_generic_cls(cls)
#     _generic_origin = typing.get_origin(cls)

#     # if not (_is_original or _generic_origin):
#     #     # No binds needed
#     #     return cls
    
#     if cls not in _binds_cache:
#         origin = cls if _is_original else _generic_origin
#         _binds_cache[cls] = types.new_class(
#             repr(cls),
#             (origin, GenericArgsBinded),
#             {},
#             lambda ns: __bind_generics(ns, cls)
#         )
        
#     return _binds_cache[cls]
