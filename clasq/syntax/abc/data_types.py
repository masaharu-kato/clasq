"""
    Basic classes definitions for built-in data types in SQL
"""
from __future__ import annotations
from abc import ABC, ABCMeta, abstractmethod
import datetime
import decimal
import typing

from ...utils.typed_generic import OptionalTypedGenericABCMeta, TypedGeneric, TypedGenericABCMeta
from .values import SQLValue

# ParamType = LiteralType | GenericParamType

class _DataTypeABCMeta(TypedGenericABCMeta):
    """ SQL Type ABC Metaclass """

    @abstractmethod
    def get_sql_base_name(cls) -> bytes:
        raise NotImplementedError()

    @property
    def sql_base_name(cls) -> bytes:
        return cls.get_sql_base_name()

    # @abstractmethod
    # def get_base_python_type(cls) -> type:
    #     raise NotImplementedError()

    @property
    @abstractmethod
    def base_python_type(cls) -> type:
        raise NotImplementedError()

    @property
    def base_sql(cls) -> bytes:
        return cls.sql_base_name   # Default Implementation

    @property
    def nullable(cls) -> bool:
        return False  # Default Implementation

    @property
    def sql(cls) -> bytes:
        if not cls.nullable:
            return cls.base_sql + b' NOT NULL'
        return cls.base_sql

    @property
    def python_type(cls):
        if cls.nullable:
            return cls.base_python_type | None
        return cls.base_python_type

    @property
    def annotation(cls) -> str:
        return cls.python_type.__name__
 
    def convert_value_for_sql(cls, v: typing.Any) -> SQLValue:
        """ Convert a value for SQL """
        return v  # Default Implementation

    # def _repr_for_compare(cls):
    #     try:
    #         sql = cls.sql
    #     except NotImplementedError:
    #         sql = None
    #     return (cls, sql)

    # def __eq__(cls, other_cls) -> bool:
    #     if isinstance(other_cls, _DataTypeABCMeta):
    #         return cls.sql == other_cls.sql
    #     return super().__eq__(other_cls)

    # def __hash__(cls) -> int:
    #     return hash(cls.sql)
        

class DataTypeABC(ABC, TypedGeneric, metaclass=_DataTypeABCMeta):
    """ SQL Type ABC """

    def __init__(self, v) -> None:
        super().__init__()
        self._orig_v = v

    @property
    def orig_value(self) -> SQLValue:
        return self._orig_v

    @property
    def sql_value(self) -> SQLValue:
        return type(self).convert_value_for_sql(self._orig_v)

    # @classmethod
    # def with_params(cls, params) -> type[DataTypeABC]:
    #     return cls.__class_getitem__(params)

    # @classmethod
    # def __class_getitem__(cls, params) -> type[DataTypeABC]:
    #     raise RuntimeError('This type does not accept parameters.', cls)

    # def __init_subclass__(cls) -> None:
    #     super().__init_subclass__()
    #     try:
    #         name = cls.sql_base_name
    #     except NotImplementedError:
    #         pass
    #     else:
    #         if name in _DATA_TYPE_FROM_BASE_NAME:
    #             raise RuntimeError('Duplicate type base name.', name);
    #         _DATA_TYPE_FROM_BASE_NAME[name] = cls

    def __eq__(self, obj) -> bool:
        if isinstance(obj, DataTypeABC):
            return type(self) is type(obj) and self.orig_value == obj.orig_value
        return super().__eq__(obj)

    def __hash__(self) -> int:
        return hash((type(self), self.orig_value))

# _DATA_TYPE_FROM_BASE_NAME: dict[bytes, type[DataTypeABC]] = {}


class _WithParamsABCMeta(_DataTypeABCMeta):
    """ SQL type with custom parameters """

    @property
    @abstractmethod
    def params_for_sql(cls) -> tuple:
        raise NotImplementedError()

    @property
    def base_sql(cls) -> bytes:
        if params := cls.params_for_sql:
            return b'%s(%s)' % (super().base_sql, ', '.join(map(str, params)).encode())
        return super().base_sql

    @property
    def annotation(cls) -> str:
        if params := cls._generic_argvals:
            return '%s[%s]' % (super().annotation + ', '.join(map(cls.__to_annotation, params)))
        return super().annotation

    @staticmethod
    def __to_annotation(val) -> str:
        if isinstance(val, type):
            if issubclass(val, DataTypeABC):
                return val.annotation
            return val.__name__
        return 'Literal[%s]' % repr(val)  # TODO: more imp

class _NumericABCMeta(_DataTypeABCMeta):
    """ Numeric type ABC Metaclass """

class NumericABC(DataTypeABC, metaclass=_NumericABCMeta):
    """ Numeric type ABC """

class _IntegerABCMeta(_NumericABCMeta):
    """ Integer type ABC Metaclass """

    @abstractmethod
    def get_v_limit_min(cls) -> int:
        """ Get a minimum value """
        raise NotImplementedError()

    @abstractmethod
    def get_v_limit_max(cls) -> int:
        """ Get a minimum value """
        raise NotImplementedError()

    @property
    def v_limit_min(cls) -> int:
        """ Get a minimum value """
        return cls.get_v_limit_min()

    @property
    def v_limit_max(cls) -> int:
        """ Get a minimum value """
        return cls.get_v_limit_max()

    @property
    def base_python_type(cls) -> type:
        return int

    def convert_value_for_sql(cls, v):
        v = super().convert_value_for_sql(v)
        if not (cls.v_limit_min <= v and v <= cls.v_limit_max):
            raise ValueError('Out of range of integer.', v)
        return v

class IntegerABC(NumericABC, metaclass=_IntegerABCMeta):
    """ Integer type ABC """

class _UnsignedIntegerABCMeta(_IntegerABCMeta):
    """ Unsigned integer type ABC Metaclass """

    @abstractmethod
    def get_signed_type(cls) -> type[IntegerABC]:
        """ Get a base signed integer type """
        raise NotImplementedError()

    @property
    def signed_type(cls) -> _IntegerABCMeta:
        """ Get a base signed integer type """
        return cls.get_signed_type()

    def get_sql_base_name(cls) -> bytes:
        return b'%s UNSIGNED' % cls.signed_type.get_sql_base_name()

    def get_v_limit_max(cls) -> int:
        signed_type = cls.signed_type
        return -signed_type.get_v_limit_min() + signed_type.get_v_limit_max()
    
    def get_v_limit_min(cls) -> int:
        return 0

class UnsignedIntegerABC(NumericABC, metaclass=_UnsignedIntegerABCMeta):
    """ Unsigned integer type ABC """

class _FloatABCMeta(_NumericABCMeta):
    """ Floating-point decimal type ABC Metaclass """

    @property
    def base_python_type(cls) -> type:
        return float

class FloatABC(NumericABC, metaclass=_FloatABCMeta):
    """ Floating-point decimal type ABC """

PREC = typing.TypeVar('PREC', bound=int)
SCALE = typing.TypeVar('SCALE', bound=int)

class _DecimalABCMeta(_NumericABCMeta, _WithParamsABCMeta):
    """ Fixed decimal type ABC Metaclass """
    
    @property
    def base_python_type(cls):
        return decimal.Decimal

    @property
    def prec(cls):
        if (v := PREC@cls) is None:
            raise RuntimeError('Precision is not set.')
        return int(v)

    @property
    def scale(cls):
        if (v := SCALE@cls) is None:
            raise RuntimeError('Scale is not set.')
        return int(v)

    @property
    def params_for_sql(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        return (cls.prec, cls.scale)
        
class DecimalABC(NumericABC, typing.Generic[PREC, SCALE], metaclass=_DecimalABCMeta):
    """ Fixed decimal type ABC """

class _DateTimeABCMeta(_DataTypeABCMeta):
    """ Date and time type ABC Metaclass """
    @property
    def base_python_type(cls) -> type:
        return datetime.datetime

class _DateABCMeta(_DataTypeABCMeta):
    """ Date type ABC Metaclass """
    @property
    def base_python_type(cls) -> type:
        return datetime.date

class _TimeABCMeta(_DataTypeABCMeta):
    """ Time type ABC Metaclass """
    @property
    def base_python_type(cls) -> type:
        return datetime.time

class DateTimeABC(DataTypeABC, metaclass=_DateTimeABCMeta):
    """ Date and time type ABC """

class DateABC(DataTypeABC, metaclass=_DateABCMeta):
    """ Date type ABC """

class TimeABC(DataTypeABC, metaclass=_TimeABCMeta):
    """ Time type ABC """

class _StringTypeABCMeta(_DataTypeABCMeta):
    """ String type ABC Metaclass """

    @abstractmethod
    def get_max_length(cls) -> int:
        """ Get a max length """
        raise NotImplementedError()

    @property
    def max_length(cls) -> int:
        """ Get a max length """
        return cls.get_max_length()
        
    def convert_value_for_sql(cls, v):
        v = super().convert_value_for_sql(v)
        if not ((lmax := cls.max_length) is None or len(v) <= lmax):
            raise ValueError('The length of a value exceeds the limit.', v)
        return v

class _StringABCMeta(_StringTypeABCMeta):
    @property
    def base_python_type(cls) -> type:
        return str

class _BinaryABCMeta(_StringTypeABCMeta):
    @property
    def base_python_type(cls) -> type:
        return bytes

class StringTypeABC(DataTypeABC, metaclass=_StringTypeABCMeta):
    """ String type ABC """

class StringABC(StringTypeABC, metaclass=_StringABCMeta):
    """ """

class BinaryABC(StringTypeABC, metaclass=_BinaryABCMeta):
    """ """

# class BlobABC(StringABC):
#     """ Blob type ABC """
#     @classmethod
#     @property
#     def base_python_type(cls) -> type:
#         return bytes

# class TextABC(StringABC):
#     """ Text type ABC """
#     @classmethod
#     @property
#     def base_python_type(cls) -> type:
#         return str

L = typing.TypeVar('L', bound=int)
class _WithLengthABCMeta(_WithParamsABCMeta):
    """ SQL type with required length ABC Metaclass """

    @property
    def specified_length(cls) -> int | None:
        return int(v) if (v := L@cls) is not None else None

    @property
    def max_length(cls) -> int:
        v = cls.specified_length
        if v is None:
            raise ValueError('Length is not set.')
        if not isinstance(v, int):
            raise TypeError('Invalid type of length.', v)
        return v

    @property
    def params_for_sql(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name
            (Override)
        """
        return (cls.max_length, )

class _WithOptionalLengthABCMeta(_WithLengthABCMeta, OptionalTypedGenericABCMeta):
    """ SQL type with optional length ABC Metaclass """

    @property
    def max_length(cls) -> int:
        """ Get a max length 
            (Override for _WithLengthABCMeta)
        """
        return int(v) if (v := cls.specified_length) is not None else cls.default_length

    @property
    @abstractmethod
    def default_length(cls) -> int:
        """ Get a default length """
        raise NotImplementedError()

    @property
    def params_for_sql(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        return (cls.max_length, ) if cls.specified_length is not None else ()



class _BitABCMeta(_NumericABCMeta, _WithLengthABCMeta):
    """ Bit type """
    @property
    def base_python_type(cls) -> type:
        return int

    def convert_value_for_sql(cls, v: typing.Any) -> SQLValue:
        v = super().convert_value_for_sql(v)
        if v is None or v is True or v is False:
            return v
        if isinstance(v, int):
            if v < 2**cls.max_length:
                return v
            raise ValueError('Data too long.', v)
        raise TypeError('Invalid type.', v)

class BitABC(NumericABC, typing.Generic[L], metaclass=_BitABCMeta):
    """ SQL type with required length ABC for Bit """

class _StringTypeWithLengthABCMeta(_StringTypeABCMeta, _WithLengthABCMeta):
    """ SQL type with length ABC Metaclass for String """
    
    @property
    def max_length(cls) -> int:
        """ Get a max length 
            (Override for _WithLengthABCMeta)
        """
        if (l := cls.specified_length) is None:
            raise RuntimeError('Length not specified.')
        return l

class _StringWithLengthABCMeta(_StringTypeWithLengthABCMeta, _StringABCMeta):
    """ """

class _BinaryWithLengthABCMeta(_StringTypeWithLengthABCMeta, _BinaryABCMeta):
    """ """

class _StringTypeWithOptionalLengthABCMeta(_StringTypeABCMeta, _WithOptionalLengthABCMeta):
    """ SQL type with optional length ABC Metaclass for String """

    @property
    def max_length(cls):
        """ Get a max length 
            (Override for _WithLengthABCMeta)
        """
        return cls.specified_length

class _StringWithOptionalLengthABCMeta(_StringTypeWithOptionalLengthABCMeta, _StringABCMeta):
    """ """

class _BinaryWithOptionalLengthABCMeta(_StringTypeWithOptionalLengthABCMeta, _BinaryABCMeta):
    """ """

class StringWithOptionalLengthABC(StringABC, typing.Generic[L], metaclass=_StringWithOptionalLengthABCMeta):
    """ SQL type with optional length ABC for String """

class StringWithLengthABC(StringABC, typing.Generic[L], metaclass=_StringWithLengthABCMeta):
    """ SQL type with required length ABC for String """

class BinaryWithOptionalLengthABC(BinaryABC, typing.Generic[L], metaclass=_BinaryWithOptionalLengthABCMeta):
    """ SQL type with optional length ABC for Bytes """

class BinaryWithLengthABC(BinaryABC, typing.Generic[L], metaclass=_BinaryWithLengthABCMeta):
    """ SQL type with required length ABC for Bytes """

# class BinaryABC(StringWithLengthABC[L], typing.Generic[L]):
#     """ Binary types (with required length) """
    
#     @classmethod
#     @property
#     def base_python_type(cls) -> type:
#         return bytes

# class CharABC(StringWithLengthABC[L], typing.Generic[L]):
#     """ Char types (with required length) """

#     @classmethod
#     @property
#     def base_python_type(cls) -> type:
#         return str

# class SQLTypeWithValues(typing.Generic[T]):
#     """ SQL data types with patameter values """

# class TypedTableColumnWithType(TypedTableColumnABC, typing.Generic[T]):
#     """ SQL type data with another sql type """


class _SpecialABCMeta(_DataTypeABCMeta):
    """ Metaclass for Special ABC """

    @abstractmethod
    def get_base_type(cls) -> _DataTypeABCMeta:
        """ Get a base type """
        # return SQT@cls  # type: ignore

    @property
    def base_type(cls) -> _DataTypeABCMeta:
        return cls.get_base_type()

    def get_sql_base_name(cls) -> bytes:
        return cls.base_type.get_sql_base_name()
        
    @property
    def base_python_type(cls) -> type:
        return cls.base_type.base_python_type

    @property
    def base_sql(cls) -> bytes:
        return cls.base_type.base_sql

    # @property
    # def sql(cls) -> bytes:
    #     return cls.base_type.sql

    # @property
    # def python_type(cls):
    #     return cls.base_type.python_type

    # @property
    # def annotation(cls) -> str:
    #     return cls.base_type.annotation
 
    def convert_value_for_sql(cls, v: typing.Any) -> SQLValue:
        """ Convert a value for SQL """
        return cls.base_type.convert_value_for_sql(v)


SQT = typing.TypeVar('SQT', bound=DataTypeABC)
class TypeSpecialABC(DataTypeABC, typing.Generic[SQT], metaclass=_SpecialABCMeta):
    """ Type with special attributes abstract class """
    @classmethod
    def get_base_type(cls) -> _DataTypeABCMeta:
        _ret = SQT@cls  # type: ignore
        if not isinstance(_ret, _DataTypeABCMeta):
            raise RecursionError('Invalid generic argument.', _ret)
        return _ret


class _NullableABCMeta(_SpecialABCMeta):
    """ Metaclass for Nullable ABC """
    @property
    def nullable(cls) -> bool:
        return True


class NullableABC(TypeSpecialABC[SQT], typing.Generic[SQT], metaclass=_NullableABCMeta):
    """ Nullable ABC """


class AnyDataType(DataTypeABC):
    """ Any SQL Type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        raise RuntimeError('Cannot get a type name of `AnySQLType`.')
