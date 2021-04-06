""" SQL build-in data types """
import abc
import types
from typing import final, Type
from functools import lru_cache
from . import record


class _IntValueType:
    """ Int value type """
    _VALUE_ = None

def _int_value_type(v:int):
    if not isinstance(v, int):
        raise TypeError('`v` is not an integer.')
    return types.new_class('intv_{}'.format(v), bases=(_IntValueType,))


class SQLType:
    """ SQL data types """
    _PY_TYPE_ = None    # Corresponding python type
    __TYPE_SQL__ = None # Code of this type in SQL. Specify None if it is the same as the class name

    def __init__(self, v):
        self.v = v

    @classmethod
    def _validate_value(cls, _): # Default implementation
        return True 

    @classmethod
    def __type_sql__(cls): # Default implementation
        assert issubclass(cls, Final)
        return cls.__TYPE_SQL__ or cls.__name__.upper()

    @final
    @classmethod
    def validate_value(cls, v):
        return isinstance(v, cls._PY_TYPE_) and cls._validate_value(v)

    @final
    def validate(self):
        return self.validate_value(self.v)


class SQLGenericType:
    """ SQL generic data types """


class SQLTypeWithKey(SQLGenericType):
    """ SQL data type with key (length or value(s) etc.) """

    @classmethod
    def new_subclass(cls, classname:str, bases=None, **classprops):
        return types.new_class(
            classname,
            (cls, *(bases or [])),
            {},
            lambda ns: ns.update(classprops)
        )


class SQLTypeWithOptionalLength(SQLTypeWithKey):
    """ SQL data types with optional length """
    _MAX_LEN_ = None

    @classmethod
    @lru_cache
    def __class_getitem__(cls, l):
        assert isinstance(l, int)
        return cls.new_subclass(
            f'{cls.__name__}_len{l}',
            _MAX_LEN_ = l,
            __TYPE_SQL__ = f'{cls.__type_sql__()}({l})',
        )


class SQLTypeWithLength(SQLTypeWithOptionalLength):
    """ SQL data types with length (required) """

    def __new__(cls, *args, **kwargs):
        if not cls._MAX_LEN_:
            raise RuntimeError('Length is not specified.')
        return super().__new__(cls)


class NumericType(SQLType):
    """ Numeric types """

class IntegerType(NumericType):
    """ Integer types """
    _PY_TYPE_ = int
    _MIN_VALUE_ = None
    _MAX_VALUE_ = None

    @classmethod
    def _validate_value(cls, v):
        return cls._MIN_VALUE_ <= v and v <= cls._MAX_VALUE_

    def __int__(self):
        return int(self.v)

class UnsignedIntegerType(IntegerType):
    """ Unsigned integer types """
    _MIN_VALUE_ = 0


class FloatType(NumericType):
    """ Floating-point decimal types """
    _PY_TYPE_ = float

    def __float__(self):
        return float(self.v)


class DecimalType(NumericType):
    """ Fixed Decimal types """


class DatetimeType(SQLType):
    """ Date and time types """


class StringType(SQLType):
    """ String types """
    _PY_TYPE_ = str
    _MAX_LEN_ = None

    def __str__(self):
        return str(self.v)


class CharType(StringType, SQLTypeWithLength):
    """ Char types (with required length) """
    @classmethod
    def _validate_value(cls, v):
        return len(v) <= cls._MAX_LEN_


class TextType(StringType, SQLTypeWithOptionalLength):
    """ Text types (with optional length) """
    @classmethod
    def _validate_value(cls, v):
        return not cls._MAX_LEN_ or len(v) <= cls._MAX_LEN_


class SQLTypeWithValues(StringType, SQLTypeWithLength):
    """ SQL data types with optional length """
    _TYPE_VALUES_ = None

    @classmethod
    @lru_cache
    def __class_getitem__(cls, values):
        assert isinstance(values, tuple) and all(isinstance(v, str) for v in values)
        return cls.new_subclass(
            f'{cls.__name__}_vals_' + '_'.join(values),
            _TYPE_VALUES_ = values,
            __TYPE_SQL__  = f'{cls.__type_sql__()}(' + ', '.join(f"'{v}'" for v in values) + ')',
        )

    def __new__(cls, *args, **kwargs):
        if not cls._TYPE_VALUES_:
            raise RuntimeError('Values is not specified.')
        return super().__new__(cls)



class Record:
    """ Record class (declaration) """


class SQLTypeEnv:
    """ SQL Type environment definition """
    type_aliases = {}
    default_not_null = True

    def __new__(cls, *args, **kwargs):
        raise RuntimeError('This class cannot be initialized.')

    @classmethod
    def set_type_alias(cls, tf:Type, tt:Type):
        assert issubclass(tt, SQLType)
        cls.type_aliases[tf] = tt

    @classmethod
    def actual_type(cls, t):
        if t in cls.type_aliases:
            return cls.type_aliases.get(t)
        if issubclass(t, Record):
            return ForeignTableKey[t]
        return t

    @classmethod
    def final_type(cls, t):
        t = cls.actual_type(t)
        if cls.default_not_null and not isinstance(t, (Optional, NotNull)):
            t = NotNull[t]
        return t

    @classmethod
    def type_sql(cls, t) -> str:
        t = cls.actual_type(t)
        return t.__type_sql__()


class SQLTypeWithType(SQLTypeWithKey):
    """ SQL data types with optional length """
    _TYPE_BASE_ = None
    _TYPE_SQL_SUFFIX_ = None

    @classmethod
    @lru_cache
    def __class_getitem__(cls, t):
        assert cls._TYPE_SQL_SUFFIX_ is not None
        assert isinstance(t, type)
        t = SQLTypeEnv.actual_type(t)
        return cls.new_subclass(
            f'{cls.__name__}__{t.__name__}',
            (t,),
            _TYPE_BASE_  = t,
            __TYPE_SQL__ = t.__type_sql__() + cls._TYPE_SQL_SUFFIX_,
        )

    def __new__(cls, *args, **kwargs):
        if not cls._TYPE_BASE_:
            raise RuntimeError('Type is not specified.')
        return super().__new__(cls)


class Final:
    """ Represents the class is final (= actual SQL type) """


class Int(Final, IntegerType):
    _MIN_VALUE_ = -2**31
    _MAX_VALUE_ = 2**31-1


class Optional(Final, SQLTypeWithType):
    """ SQL optional type """
    _TYPE_SQL_SUFFIX_ = ''


class NotNull(Final, SQLTypeWithType):
    """ SQL NOT NULL type """
    _TYPE_SQL_SUFFIX_ = ' NOT NULL'


class PrimaryKey(Final, SQLTypeWithType):
    """ SQL Primary Key type """
    _TYPE_SQL_SUFFIX_ = ' PRIMARY KEY'


class ForeignTableKey(Final, SQLTypeWithType):
    """ Foreign table key (Primary key) """

    @classmethod
    @lru_cache
    def __class_getitem__(cls, t):
        assert issubclass(t, record.Record)
        return cls.new_subclass(
            f'FK__{t.__name__}',
            (Int,),
            _TYPE_BASE_  = t,
            __TYPE_SQL__ = Int.__type_sql__(),
        )


class RecordEnv(SQLTypeEnv):

    common_columns = {
        'id' : PrimaryKey[NotNull[Int]],
    }

    @classmethod
    def set_common_columns(cls, tf:Type, tt:Type):
        assert isinstance(tt, SQLType)
        cls.common_columns[tf] = tt
