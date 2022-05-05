"""
    Definitions of built-in data types in SQL
"""
import typing
import datetime
from functools import lru_cache
from . import sqltypebases as tb
from .sqltypebases import Nullable, NotNull, PrimaryKey, Int, SQLTypeABC, SQLTypeEnv, get_type_sql

Optional = typing.Optional

class TinyInt(tb.Final, tb.IntegerType):
    """ TINTINT type """
    _MIN_VALUE_ = -2**7
    _MAX_VALUE_ = 2**7-1
    
class SmallInt(tb.Final, tb.IntegerType):
    """ SMALLINT type """
    _MIN_VALUE_ = -2**15
    _MAX_VALUE_ = 2**15-1
    
class MediumInt(tb.Final, tb.IntegerType):
    """ MEDIUMINT type """
    _MIN_VALUE_ = -2**23
    _MAX_VALUE_ = 2**23-1

class BigInt(tb.Final, tb.IntegerType):
    """ BIGINT type """
    _MIN_VALUE_ = -2**63
    _MAX_VALUE_ = 2**63-1

class UnsignedTinyInt(tb.Final, tb.UnsignedIntegerType):
    """ Unsigned TINYINT type """
    _MAX_VALUE_ = 2**8-1
    
class UnsignedSmallInt(tb.Final, tb.UnsignedIntegerType):
    """ Unsigned SMALLINT type """
    _MAX_VALUE_ = 2**16-1
    
class UnsignedMediumInt(tb.Final, tb.UnsignedIntegerType):
    """ Unsigned MEDIUMINT type """
    _MAX_VALUE_ = 2**24-1

class UnsignedInt(tb.Final, tb.UnsignedIntegerType):
    """ Unsigned INT type """
    _MAX_VALUE_ = 2**32-1

class UnsignedBigInt(tb.Final, tb.UnsignedIntegerType):
    """ Unsigned BIGINT type """
    _MAX_VALUE_ = 2**64-1

class Unsigned(tb.SQLTypeABC):
    """ Unsigned sql type """

    @classmethod
    @lru_cache
    def __class_getitem__(cls, t):
        if t == TinyInt:
            return UnsignedTinyInt
        if t == SmallInt:
            return UnsignedSmallInt
        if t == MediumInt:
            return UnsignedMediumInt
        if t == Int:
            return UnsignedInt
        if t == BigInt:
            return UnsignedBigInt
        raise RuntimeError('Invalid type.')

    def __new__(cls, *_args, **_kwargs):
        if not issubclass(cls, tb.Final):
            raise RuntimeError('Type is not specified.')
        return super().__new__(cls)


class Float(tb.Final, tb.FloatType):
    """ Float type """

class Double(tb.Final, tb.FloatType):
    """ Double type """


class Decimal(tb.Final, tb.DecimalType):
    """ Decimal type """


class Bit(tb.Final, tb.NumericType):
    """ Bit type """

    def __bytes__(self):
        return bytes(self.v)


class DateTime(tb.Final, tb.DatetimeType):
    """ DateTime type """

class Date(tb.Final, tb.DatetimeType):
    """ Date type """

class Time(tb.Final, tb.DatetimeType):
    """ Time type """


class Char(tb.Final, tb.CharType):
    """ CHAR string type """

class VarChar(tb.Final, tb.CharType):
    """ VARCHAR string type """

class Binary(tb.Final, tb.CharType):
    """ CHAR string type """

class VarBinary(tb.Final, tb.CharType):
    """ VARCHAR string type """


class Blob(tb.Final, tb.TextType):
    """ BLOB type """

class TinyBlob(Blob):
    """ Tiny blob type """

class MediumBlob(Blob):
    """ Medium blob type """

class LongBlob(Blob):
    """ Long blob type """


class Text(tb.Final, tb.TextType):
    """ TEXT type """

class TinyText(Text):
    """ Tiny text type """

class MediumText(Text):
    """ Medium text type """

class LongText(Text):
    """ Long Text type """


class Enum(tb.Final, tb.StringType, tb.SQLTypeWithValues):
    """ ENUM type """
    @classmethod
    def _validate_value(cls, v):
        return v in cls._TYPE_VALUES_


class Set(tb.Final, tb.StringType, tb.SQLTypeWithValues):
    """ SET type """
    @classmethod
    def _validate_value(cls, v):
        return all(cv.strip() in cls._TYPE_VALUES_ for cv in v.split(','))


# MySQL aliases of types

class Bool(TinyInt):
    """ Bool type (alias) """

class Boolean(Bool):
    """ Boolean type (alias) """

class CharacterVarying(VarChar):
    """ Character varying type (alias) """

class Fixed(Decimal):
    """ Fixed type (alias) """

class Float4(Float):
    """ Float type (alias) """

class Float8(Double):
    """ Double type (alias) """

class INT1(TinyInt):
    """ INT1 type (alias) """

class INT2(SmallInt):
    """ INT2 type (alias) """

class INT3(MediumInt):
    """ INT3 type (alias) """

class INT4(Int):
    """ INT4 type (alias) """

class INT8(BigInt):
    """ INT8 type (alias) """

class LongVarBinary(MediumBlob):
    """ LongVarBinary type (alias) """

class LongVarchar(MediumText):
    """ LongVarchar type (alias) """

class Long(MediumText):
    """ Long type (alias) """

class MiddleInt(MediumInt):
    """ MiddleInt type (alias) """

class Numeric(Decimal):
    """ Numeric type (alias of Decimal type) """

class Real(Double):
    """ Real type (alias of Double) """


tb.SQLTypeEnv.set_type_alias(bool , Bool  )
tb.SQLTypeEnv.set_type_alias(int  , Int   )
tb.SQLTypeEnv.set_type_alias(float, Double)
tb.SQLTypeEnv.set_type_alias(str  , Text  )
tb.SQLTypeEnv.set_type_alias(bytes, Blob  )

tb.SQLTypeEnv.set_type_alias(datetime.date, Date)
tb.SQLTypeEnv.set_type_alias(datetime.time, Time)
tb.SQLTypeEnv.set_type_alias(datetime.datetime, DateTime)


TypeLike = typing.Optional[typing.Union[SQLTypeABC, typing.Type, bytes, str]]

SQLType = typing.Union[SQLTypeABC, typing.Type]

def make_sql_type(typelike: TypeLike) -> SQLType:

    if (isinstance(typelike, type) and issubclass(typelike, SQLTypeABC))\
        or isinstance(typelike, SQLTypeABC):
        return typelike

    if isinstance(typelike, str):
        if not typelike:
            return Text
        try:
            return VarChar.with_length(int(typelike))
        except ValueError:
            pass
    
    elif isinstance(typelike, bytes):
        if not typelike:
            return Blob
        try:
            return VarBinary.with_length(int(typelike))
        except ValueError:
            pass
    
    else:

        if typelike is datetime.datetime:
            return DateTime
        if typelike is datetime.date:
            return Date
        if typelike is datetime.time:
            return Time
        if typelike is str:
            return Text
        if typelike is bytes:
            return Blob
        if typelike is float:
            return Double
        if typelike is int:
            return Int
        if typelike is bool:
            return Bool

    raise RuntimeError('Invalid typelike:', typelike)
