"""
    Generic class utils
"""
from __future__ import annotations
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
            cls._generic_argkeys = argkeys
            cls._generic_arg_index_from_key = arg_index_from_key

            # Init a new class cache
            cls.__generic_cls_cache: dict[tuple, type[TypedGenericMeta]] = {}

        else:
            cls._generic_argkeys = origin._generic_argkeys
            cls._generic_arg_index_from_key = origin._generic_arg_index_from_key

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
            return cls._generic_argvals[cls._generic_arg_index_from_key[_val.__name__]]
        raise TypeError('Invalid type of value.')

    def __rmatmul__(cls, _val):
        if isinstance(_val, typing.TypeVar):
            return cls._generic_arg(_val)
        return NotImplemented

    def __getitem__(cls, _val):
        argvals = _val if isinstance(_val, tuple) else (_val, )
        _typing_generic = cls.__class_getitem__(argvals)
        if not len(cls._generic_argkeys) == len(argvals):
            raise RuntimeError('Invalid number of generic arguments.')

        argdict = dict(zip(cls._generic_argkeys, argvals))
        print(cls, '__getitem__ called.', argdict, _typing_generic)

        if not argvals in cls.__generic_cls_cache:
            cls.__generic_cls_cache[argvals] = types.new_class(
                repr(_typing_generic),
                (cls, ),
                {},
                lambda ns: cls.__bind_generics(ns, _typing_generic, argvals)
            )
        return cls.__generic_cls_cache[argvals]

    def __bind_generics(cls, ns: dict, _typing_generic: type, argvals: tuple):
        ns['__typing_generic'] = _typing_generic
        ns['__generic_origin'] = cls # typing.get_origin(_typing_generic)
        ns['__generic_argvals'] = argvals # typing.get_args(_typing_generic)


T = typing.TypeVar('T')
class TypedGeneric(typing.Generic[T], metaclass=TypedGenericMeta):
    """ Typed Generic """
    
    def __rmatmul__(self, _val):
        if isinstance(_val, typing.TypeVar):
            return type(self)._generic_arg(_val)
        return NotImplemented
            

    # def __class_getitem__(cls, *args, **kwargs):
    #     print(cls, '__class_getitem__ called.')
    #     return super().__class_getitem__(*args, **kwargs)



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