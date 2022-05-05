"""
    Basic classes definitions for built-in data types in SQL
"""
import types
import typing
from functools import lru_cache

T = typing.TypeVar('T')

def is_child_class(target, base):
    """ Check if the target type is a subclass of the base type and not base type itself """
    return issubclass(target, base) and target is not base


def _isinstance(v, t):
    assert isinstance(t, type)
    return isinstance(v, t)


class SQLTypeABC:
    """ SQL data types """
    _PY_TYPE_            : typing.Optional[typing.Type[object]] = None # Corresponding python type
    __TYPE_SQL__         : typing.Optional[str]  = None # Code of this type in SQL. Specify None if it is the same as the class name
    _TYPE_HAS_DEFAULT_   : bool = True # Type has a default value
    _TYPE_DEFAULT_VALUE_ : typing.Optional[typing.Any] = None # Type's default value (if has)
    _TYPE_COMMENT_       : typing.Optional[str] = None # Type's comment
    _TYPE_FOREIGN_KEY_   : typing.Optional[str] = None

    def __init__(self, v):
        self.v = v

    @classmethod
    def _validate_value(cls, _): # Default implementation
        return True 

    @classmethod
    def __type_sql__(cls): # Default implementation
        assert issubclass(cls, Final)
        return cls.__TYPE_SQL__ or cls.__name__.upper()

    @typing.final
    @classmethod
    def validate_value(cls, v):
        """ Validate a given value """
        assert cls._PY_TYPE_ is not None
        return _isinstance(v, cls._PY_TYPE_) and cls._validate_value(v)

    @typing.final
    def validate(self):
        """ Validate a given value (with type check) """
        return self.validate_value(self.v)

    @classmethod
    def default(cls, *args):
        """ Set/clear default value """
        if len(args) > 1:
            raise RuntimeError('Cannot specify multiple arguments.')
        if args:
            cls._TYPE_HAS_DEFAULT_ = True
            cls._TYPE_DEFAULT_VALUE_ = args[0]
        else:
            cls._TYPE_HAS_DEFAULT_ = False
            cls._TYPE_DEFAULT_VALUE_ = None

    @classmethod
    def has_default_value(cls):
        """ Get this type has a default value or not """
        return cls._TYPE_HAS_DEFAULT_

    @classmethod
    def default_value(cls):
        """ Get the default value (if exists) """
        if not cls.has_default_value():
            raise RuntimeError('This type does not have a default value.')
        return cls._TYPE_DEFAULT_VALUE_

    @classmethod
    def comment(cls, comment=None):
        """ Set comment """
        cls._TYPE_COMMENT_ = comment

    @classmethod
    def get_comment(cls):
        """ Get comment """
        return cls._TYPE_COMMENT_

    @classmethod
    def get_foreign_key(cls):
        """ Get foreign key if exists """
        return cls._TYPE_FOREIGN_KEY_

    @classmethod
    def has_foreign_key(cls) -> bool:
        return bool(cls._TYPE_FOREIGN_KEY_)



def get_type_sql(t):
    """ Get the SQL of type in a given SQLTypeABC subclass """
    assert issubclass(t, SQLTypeABC)
    return t.__type_sql__()


class SQLGenericType:
    """ SQL generic data types """


class SQLTypeWithOptionalKey(SQLGenericType):
    """ SQL data type with key (length or value(s) etc.) """
    _TYPE_HAS_KEY_ = False

    @classmethod
    def new_subclass(cls, classname:str, bases=None, **classprops):
        """ Create a new subclass (using a given key) """
        return types.new_class(
            classname,
            (cls, *(bases or [])),
            {},
            lambda ns: ns.update({'_TYPE_HAS_KEY_': True, **classprops})
        )


class SQLTypeWithKey(SQLTypeWithOptionalKey):
    """ SQL data type with key (length or value(s) etc.) """

    def __new__(cls, *_args, **_kwargs):
        if not cls._TYPE_HAS_KEY_:
            raise RuntimeError('Generic type key is not specified.')
        return super().__new__(cls)


class SQLTypeWithOptionalLength(SQLTypeWithOptionalKey, typing.Generic[T]):
    """ SQL data types with optional length """
    _MAX_LEN_ = None

    @classmethod
    @lru_cache
    def __class_getitem__(cls, l):
        assert isinstance(l, int) and issubclass(cls, SQLTypeABC)
        return cls.new_subclass(
            f'{cls.__name__}_len{l}',
            _MAX_LEN_ = l,
            __TYPE_SQL__ = f'{get_type_sql(cls)}({l})',
        )

    @classmethod
    def with_length(cls, l):
        return cls.__class_getitem__(l)


class SQLTypeWithLength(SQLTypeWithOptionalLength, SQLTypeWithKey):
    """ SQL data types with length (required) """

class NumericType(SQLTypeABC):
    """ Numeric types """

class IntegerType(NumericType):
    """ Integer types """
    _PY_TYPE_ = int
    _MIN_VALUE_:typing.Optional[int] = None
    _MAX_VALUE_:typing.Optional[int] = None

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


class DecimalType(NumericType, SQLTypeWithKey, typing.Generic[T]):
    """ Fixed Decimal types """
    _TYPE_DECI_PREC_  : typing.Optional[int] = None
    _TYPE_DECI_SCALE_ : typing.Optional[int] = None

    @classmethod
    @lru_cache
    def with_prec_scale(cls, pr, sc): # Precision and scale
        assert isinstance(pr, int) and isinstance(sc, int)
        return cls.new_subclass(
            f'{cls.__name__}_{pr}_{sc}',
            _TYPE_DECI_PREC_  = pr,
            _TYPE_DECI_SCALE_ = sc,
        )

    @classmethod
    def __class_getitem__(cls, pr_sc):
        assert isinstance(pr_sc, tuple) and len(pr_sc) == 2
        return cls.with_prec_scale(*pr_sc)

    def __new__(cls, *_args, **_kwargs):
        if not cls._TYPE_DECI_PREC_ or not cls._TYPE_DECI_SCALE_:
            raise RuntimeError('Precisoin and/or scale is not specified.')
        return super().__new__(cls)


class DatetimeType(SQLTypeABC):
    """ Date and time types """


class StringType(SQLTypeABC):
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


class SQLTypeWithValues(SQLTypeWithKey, typing.Generic[T]):
    """ SQL data types with optional length """
    _TYPE_VALUES_ = None

    @classmethod
    @lru_cache
    def with_values(cls, *values):
        assert all(isinstance(v, str) for v in values)
        return cls.new_subclass(
            f'{cls.__name__}_vals_' + '_'.join(values),
            _TYPE_VALUES_ = values,
            __TYPE_SQL__  = f'{get_type_sql(cls)}(' + ', '.join(f"'{v}'" for v in values) + ')',
        )

    @classmethod
    def __class_getitem__(cls, values):
        assert isinstance(values, (tuple, list))
        return cls.with_values(*values)


class Record:
    """ Record class (declaration) """


class SQLTypeEnv:
    """ SQL Type environment definition """
    type_aliases:typing.Dict[typing.Type[object], typing.Type[object]] = {}
    default_not_null = True

    def __new__(cls, *args, **kwargs):
        raise RuntimeError('This class cannot be initialized.')

    @classmethod
    def set_type_alias(cls, tf:typing.Type[object], tt:typing.Type[object]) -> None:
        """ Set an alias type of a given type """
        assert is_child_class(tt, SQLTypeABC)
        cls.type_aliases[tf] = tt

    @staticmethod
    def _is_typing_type(t:typing.Type[object]) -> bool:
        return typing.get_origin(t) is not None

    @staticmethod
    def _is_typing_optional(t:typing.Type[object]) -> bool:
        if typing.get_origin(t) is typing.Union:
            tpargs = typing.get_args(t)
            return len(tpargs) == 2 and tpargs[1] is type(None)
        return False

    @staticmethod
    def _is_typing_list(t:typing.Type[object]) -> bool:
        return typing.get_origin(t) is typing.List

    @staticmethod
    def _typing_basetype(t:typing.Type) -> typing.Type[object]:
        return t.__args__[0]

    @staticmethod
    def _typing_basetypes(t:typing.Type) -> typing.List[typing.Type[object]]:
        return t.__args__

    @classmethod
    def actual_type(cls, t:typing.Type[object], *, ensure_nullable:bool=False) -> typing.Type[object]:
        """ Get an actual type (subclass of SQLTypeABC) of a given type """
        if cls._is_typing_optional(t):
            return cls.actual_type(cls._typing_basetype(t), ensure_nullable=False)
        if is_child_class(t, Record):
            return ForeignTableKey[t] #type: ignore
        if t in cls.type_aliases:
            t = cls.type_aliases[t]
        if ensure_nullable and cls.default_not_null and not issubclass(t, (Nullable, NotNull)):
            t = NotNull[t] #type: ignore
        return t

    @classmethod
    def is_compatible_column_type(cls, t:typing.Type[object]) -> bool:
        """ Check if `t` is a compatible column type or not """
        if cls._is_typing_type(t):
            return cls._is_typing_optional(t) and cls.is_compatible_column_type(cls._typing_basetype(t))
        return is_child_class(t, SQLTypeABC) or is_child_class(t, Record) or t in cls.type_aliases

    @classmethod
    def is_compatible_table_type(cls, t:typing.Type[object]) -> bool:
        """ Check if the type specified in the type hint is vaild """
        return cls._is_typing_type(t) and cls._is_typing_list(t) and is_child_class(cls.table_basetype(t), Record)

    @classmethod
    def table_basetype(cls, t:typing.Type[object]) -> typing.Type[Record]:
        """ Get the original table type from the type hint """
        _t = cls._typing_basetype(t)
        assert is_child_class(_t, Record)
        return typing.cast(typing.Type[Record], _t)

    @classmethod
    def type_sql(cls, t:typing.Type[object]) -> str:
        """ Get a SQL of type for table creation """
        t = cls.actual_type(t)
        return t.__type_sql__() #type: ignore


class SQLTypeWithType(SQLTypeWithKey, typing.Generic[T]):
    """ SQL data types with optional length """
    _TYPE_BASE_:typing.Optional[typing.Type[object]] = None
    _TYPE_SQL_SUFFIX_:typing.Optional[str] = None
    _TYPE_ENSURE_NULLABLE_ = False

    @classmethod
    # @lru_cache
    def with_type(cls, t:typing.Type[object]):
        assert cls._TYPE_SQL_SUFFIX_ is not None
        assert isinstance(t, type)
        t = SQLTypeEnv.actual_type(t, ensure_nullable=cls._TYPE_ENSURE_NULLABLE_)
        return cls.new_subclass(
            f'{cls.__name__}__{t.__name__}',
            (t,),
            _TYPE_BASE_  = t,
            __TYPE_SQL__ = t.__type_sql__() + cls._TYPE_SQL_SUFFIX_, #type: ignore
        )

    @classmethod
    def __class_getitem__(cls, t:typing.Type[object]):
        return cls.with_type(t)


class Final:
    """ Represents the class is final (= actual SQL type) """


class Int(Final, IntegerType):
    """ Normal Int type """
    _MIN_VALUE_ = -2**31
    _MAX_VALUE_ = 2**31-1


class Nullable(Final, SQLTypeWithType):
    """ SQL optional type """
    _TYPE_SQL_SUFFIX_ = ''


class NotNull(Final, SQLTypeWithType, typing.Generic[T]):
    """ SQL NOT NULL type """
    _TYPE_SQL_SUFFIX_ = ' NOT NULL'
    _TYPE_HAS_DEFAULT_ = False
    _TYPE_DEFAULT_VALUE_ = None


class PrimaryKey(Final, SQLTypeWithType):
    """ SQL Primary Key type """
    _TYPE_SQL_SUFFIX_ = ' PRIMARY KEY'
    _TYPE_ENSURE_NULLABLE_ = True


class ForeignTableKey(Final, SQLTypeWithType, typing.Generic[T]):
    """ Foreign table key (Primary key) """

    @classmethod
    # @lru_cache
    def __class_getitem__(cls, t:typing.Type[object]):
        assert issubclass(t, Record)
        return cls.new_subclass(
            f'FK__{t.__name__}',
            (Int,),
            _TYPE_BASE_  = t,
            __TYPE_SQL__ = Int.__type_sql__(),
            _TYPE_FOREIGN_KEY_ = t,
        )
