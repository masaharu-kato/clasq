"""
    Basic classes definitions for built-in data types in SQL
"""
from abc import ABC, ABCMeta, abstractmethod
import typing

from ..utils.generic_cls import bind_generic_args

if typing.TYPE_CHECKING:
    from ..syntax.sql_values import SQLValue

class _SQLTypeABCMeta(ABCMeta):
    """ SQL Type ABC Metaclass """

    @abstractmethod
    def get_sql_type_name(cls) -> bytes:
        """ Get a sql type name"""
        raise NotImplementedError()

    @property
    def sql_type_name(cls) -> bytes:
        return cls.get_sql_type_name()

    @abstractmethod
    def get_python_type(cls) -> typing.Type:
        """ Get a python type """
        raise NotImplementedError()

    @property
    def python_type(cls) -> typing.Type:
        return cls.get_python_type()

    def convert_value_for_sql(cls, v: typing.Any) -> 'SQLValue':
        """ Convert a value for SQL """
        return v  # Default Implementation

    def get_generic_args(cls) -> tuple:
        """ Get a tuple of generic arguments (if exists) """
        if hasattr(cls, '__orig_bases__'):
            if not hasattr(cls, '__orig_class__'):
                raise RuntimeError('Cannot get generic arguments from a generic class variable.')
            return typing.get_args(getattr(cls, '__orig_class__'))    
        return ()

    def get_generic_arg(cls, i: int, default: typing.Any = None) -> typing.Any:
        """ Get a generic argument value of a specific index """
        if len(args := cls.get_generic_args()) > i:
            arg = args[i]
            if typing.get_origin(arg) is typing.Literal:
                vals = typing.get_args(arg)
                return vals[0] if len(vals) == 1 else vals
            return arg
        return default

class SQLTypeABC(ABC, metaclass=_SQLTypeABCMeta):
    """ SQL Type ABC """

    def __init__(self, v) -> None:
        super().__init__()
        self._orig_v = v

    @property
    def orig_value(self) -> 'SQLValue':
        return self._orig_v

    @property
    def cls(self) -> typing.Type['SQLTypeABC']:
        return bind_generic_args(getattr(self, '__orig_class__', type(self)))

    @property
    def sql_value(self) -> 'SQLValue':
        return self.cls.convert_value_for_sql(self._orig_v)


class _WithParamsABCMeta(_SQLTypeABCMeta):
    """ SQL type with custom parameters """
    @abstractmethod
    def get_base_sql_type_name(cls) -> bytes:
        """ Get a base SQL type name
            (type name without length)
        """
        raise NotImplementedError()

    @property
    def base_sql_type_name(cls) -> bytes:
        return cls.get_base_sql_type_name()

    @abstractmethod
    def get_params_for_sql_type_name(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        raise NotImplementedError()

    @property
    def params_for_sql_type_name(cls) -> tuple:
        return cls.get_params_for_sql_type_name()

    def get_sql_type_name(cls) -> bytes:
        if params := cls.params_for_sql_type_name:
            return b'%s(%s)' % (cls.base_sql_type_name, ', '.join(map(str, params)).encode())
        return cls.base_sql_type_name

class _NumericABCMeta(_SQLTypeABCMeta):
    """ Numeric type ABC Metaclass """

class NumericABC(SQLTypeABC, metaclass=_NumericABCMeta):
    """ Numeric type ABC """

class _IntegerABCMeta(_NumericABCMeta):
    """ Integer type ABC Metaclass """

    @abstractmethod
    def get_min_value(cls) -> int:
        """ Get a minimum value """
        raise NotImplementedError()

    @property
    def min_value(cls) -> int:
        return cls.get_min_value()

    @abstractmethod
    def get_max_value(cls) -> int:
        """ Get a minimum value """
        raise NotImplementedError()

    @property
    def max_value(cls) -> int:
        return cls.get_max_value()

    def get_python_type(cls) -> typing.Type:
        return int

    def convert_value_for_sql(cls, v):
        v = super().convert_value_for_sql(v)
        if not (cls.min_value <= v and v <= cls.max_value):
            raise ValueError('Out of range of integer.', v)
        return v

class IntegerABC(NumericABC, metaclass=_IntegerABCMeta):
    """ Integer type ABC """

class _UnsignedIntegerABCMeta(_IntegerABCMeta):
    """ Unsigned integer type ABC Metaclass """

    @abstractmethod
    def get_signed_type(cls) -> typing.Type[IntegerABC]:
        """ Get a base signed integer type """
        raise NotImplementedError()

    @property
    def signed_type(cls) -> typing.Type[IntegerABC]:
        return cls.get_signed_type()

    def get_sql_type_name(cls) -> bytes:
        return b'%s UNSIGNED' % cls.get_signed_type().get_sql_type_name()

    def get_max_value(cls) -> int:
        signed_type = cls.get_signed_type()
        return -signed_type.min_value + signed_type.max_value
    
    def get_min_value(cls) -> int:
        return 0

class UnsignedIntegerABC(NumericABC, metaclass=_UnsignedIntegerABCMeta):
    """ Unsigned integer type ABC """

class _FloatABCMeta(_NumericABCMeta):
    """ Floating-point decimal type ABC Metaclass """

    def get_python_type(cls) -> typing.Type:
        return float

class FloatABC(NumericABC, metaclass=_FloatABCMeta):
    """ Floating-point decimal type ABC """

class _DecimalABCMeta(_NumericABCMeta, _WithParamsABCMeta):
    """ Fixed decimal type ABC Metaclass """

    def get_prec(cls):
        if (v := cls.get_generic_arg(0)) is None:
            raise RuntimeError('Precision is not set.')
        return int(v)

    @property
    def prec(cls):
        return cls.get_prec()

    def get_scale(cls):
        if (v := cls.get_generic_arg(1)) is None:
            raise RuntimeError('Scale is not set.')
        return int(v)

    @property
    def scale(cls):
        return cls.get_scale()

    def get_params_for_sql_type_name(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        return (cls.prec, cls.scale)
        
PREC = typing.TypeVar('PREC', bound=int)
SCALE = typing.TypeVar('SCALE', bound=int)
class DecimalABC(NumericABC, typing.Generic[PREC, SCALE], metaclass=_DecimalABCMeta):
    """ Fixed decimal type ABC """

class _DateTimeABCMeta(_SQLTypeABCMeta):
    """ Date and time type ABC Metaclass """

class DateTimeABC(SQLTypeABC, metaclass=_DateTimeABCMeta):
    """ Floating-point decimal type ABC """

class _StringABCMeta(_SQLTypeABCMeta):
    """ String type ABC Metaclass """

    @abstractmethod
    def get_max_length(cls) -> typing.Optional[int]:
        """ Get a max length """
        raise NotImplementedError()

    @property
    def max_length(cls) -> typing.Optional[int]:
        return cls.get_max_length()
        
    def convert_value_for_sql(cls, v):
        v = super().convert_value_for_sql(v)
        if not ((lmax := cls.get_max_length()) is None or len(v) <= lmax):
            raise ValueError('The length of a value exceeds the limit.', v)
        return v

class StringABC(SQLTypeABC, metaclass=_StringABCMeta):
    """ String type ABC """

class BlobABC(StringABC):
    """ Blob type ABC """
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return bytes

class TextABC(StringABC):
    """ Text type ABC """
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return str

class _WithLengthABCMeta(_WithParamsABCMeta):
    """ SQL type with required length ABC Metaclass """

    def get_specified_length(cls) -> typing.Optional[int]:
        return int(v) if (v := cls.get_generic_arg(0)) is not None else None

    @property
    def specified_length(cls) -> typing.Optional[int]:
        return cls.get_specified_length()

    def get_length(cls) -> int:
        v = cls.specified_length
        if v is None:
            raise ValueError('Length is not set.')
        if not isinstance(v, int):
            raise TypeError('Invalid type of length.', v)
        return v

    @property
    def length(cls) -> int:
        return cls.get_length()

    def get_params_for_sql_type_name(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        return (cls.length, )

class _WithOptionalLengthABCMeta(_WithLengthABCMeta):
    """ SQL type with optional length ABC Metaclass """

    def get_length(cls) -> int:
        return int(v) if (v := cls.specified_length) is not None else cls.default_length

    @abstractmethod
    def get_default_length(cls) -> int:
        """ Get a default length """
        raise NotImplementedError()

    @property
    def default_length(cls) -> int:
        return cls.get_default_length()

    def get_params_for_sql_type_name(cls) -> tuple:
        """ Get a tuple of parameter values for sql type name """
        return (cls.length, ) if cls.specified_length is not None else ()


L = typing.TypeVar('L', bound=int)

class _BitABCMeta(_NumericABCMeta, _WithLengthABCMeta):
    """ Bit type """

    def convert_value_for_sql(cls, v: typing.Any) -> 'SQLValue':
        v = super().convert_value_for_sql(v)
        if v is None or v is True or v is False:
            return v
        if isinstance(v, int):
            if v < 2**cls.length:
                return v
            raise ValueError('Data too long.', v)
        raise TypeError('Invalid type.', v)

class BitABC(NumericABC, typing.Generic[L], metaclass=_BitABCMeta):
    """ SQL type with required length ABC for Bit """

class _StringWithLengthABCMeta(_StringABCMeta, _WithLengthABCMeta):
    """ SQL type with length ABC Metaclass for String """
    
    def get_max_length(cls) -> typing.Optional[int]:
        return cls.get_length()

class _StringWithOptionalLengthABCMeta(_StringABCMeta, _WithOptionalLengthABCMeta):
    """ SQL type with optional length ABC Metaclass for String """

    def get_max_length(cls) -> typing.Optional[int]:
        return cls.get_length()

class StringWithOptionalLengthABC(StringABC, typing.Generic[L], metaclass=_StringWithOptionalLengthABCMeta):
    """ SQL type with optional length ABC for String """

class StringWithLengthABC(StringABC, typing.Generic[L], metaclass=_StringWithLengthABCMeta):
    """ SQL type with required length ABC for String """

class BinaryABC(StringWithLengthABC[L], typing.Generic[L]):
    """ Binary types (with required length) """
    
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return bytes

class CharABC(StringWithLengthABC[L], typing.Generic[L]):
    """ Char types (with required length) """

    @classmethod
    def get_python_type(cls) -> typing.Type:
        return str

# class SQLTypeWithValues(typing.Generic[T]):
#     """ SQL data types with patameter values """

# class TypedTableColumnWithType(TypedTableColumnABC, typing.Generic[T]):
#     """ SQL type data with another sql type """
