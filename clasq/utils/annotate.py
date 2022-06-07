"""
    Python type annotations
"""
import types
import typing

from clasq.utils.typed_generic import TypedGeneric, TypedGenericABC

def annotate_python_type(t: type) -> str:
    if typing.get_origin(t) is types.UnionType:
        return ' | '.join(annotate_python_type(_t) for _t in typing.get_args(t))

    if t is types.NoneType:
        return 'None'
        
    if issubclass(t, (TypedGenericABC, TypedGeneric)):
        return '%s[%s]' % (
            annotate_python_type(t._generic_origin),
            ', '.join(map(annotate_python_type, t._generic_argvals)))

    return t.__name__

