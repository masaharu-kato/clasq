"""
    Definitions of built-in data types in SQL
"""
from __future__ import annotations
import typing
import datetime
import decimal
from .abc import sqltype as sqtabc
from ..utils.generic_cls import bind_generic_args

L = typing.TypeVar('L', bound=int)

class TinyInt(sqtabc.IntegerABC):
    """ TINY INT type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'TINYINT'
    @classmethod
    def get_min_value(cls) -> int:
        return -2**7
    @classmethod
    def get_max_value(cls) -> int:
        return 2**7-1
    
class SmallInt(sqtabc.IntegerABC):
    """ SMALLINT type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'SMALLINT'
    @classmethod
    def get_min_value(cls) -> int:
        return -2**15
    @classmethod
    def get_max_value(cls) -> int:
        return 2**15-1
    
class MediumInt(sqtabc.IntegerABC):
    """ MEDIUMINT type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'MEDIUMINT'
    @classmethod
    def get_min_value(cls) -> int:
        return -2**23
    @classmethod
    def get_max_value(cls) -> int:
        return 2**23-1
    
class Int(sqtabc.IntegerABC):
    """ INT type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'INT'
    @classmethod
    def get_min_value(cls) -> int:
        return -2**31
    @classmethod
    def get_max_value(cls) -> int:
        return 2**31-1

class BigInt(sqtabc.IntegerABC):
    """ BIGINT type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'BIGINT'
    @classmethod
    def get_min_value(cls) -> int:
        return -2**63
    @classmethod
    def get_max_value(cls) -> int:
        return 2**63-1

class UnsignedTinyInt(sqtabc.UnsignedIntegerABC):
    """ Unsigned TINYINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[sqtabc.IntegerABC]:
        return TinyInt
    
class UnsignedSmallInt(sqtabc.UnsignedIntegerABC):
    """ Unsigned SMALLINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[sqtabc.IntegerABC]:
        return SmallInt
    
class UnsignedMediumInt(sqtabc.UnsignedIntegerABC):
    """ Unsigned MEDIUMINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[sqtabc.IntegerABC]:
        return MediumInt

class UnsignedInt(sqtabc.UnsignedIntegerABC):
    """ Unsigned INT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[sqtabc.IntegerABC]:
        return Int

class UnsignedBigInt(sqtabc.UnsignedIntegerABC):
    """ Unsigned BIGINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[sqtabc.IntegerABC]:
        return BigInt

class Float(sqtabc.FloatABC):
    """ Float type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'FLOAT'

class Double(sqtabc.FloatABC):
    """ Double type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'DOUBLE'

PREC = typing.TypeVar('PREC', bound=int)
SCALE = typing.TypeVar('SCALE', bound=int)
class Decimal(sqtabc.DecimalABC[PREC, SCALE], typing.Generic[PREC, SCALE]):
    """ Fixed Decimal types """
    @classmethod
    def get_python_type(cls):
        return decimal.Decimal

    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'DECIMAL'


class Bit(sqtabc.BitABC[L], typing.Generic[L]):
    """ Bit type """
    @classmethod
    def get_python_type(cls):
        return int

    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'BIT'

class DateTime(sqtabc.DateTimeABC):
    """ DateTime type """
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return datetime.datetime

    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'DATETIME'

class Date(sqtabc.DateTimeABC):
    """ Date type """
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return datetime.date

    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'DATE'

class Time(sqtabc.DateTimeABC):
    """ Time type """
    @classmethod
    def get_python_type(cls) -> typing.Type:
        return datetime.time

    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'TIME'

class Char(sqtabc.CharABC[L], typing.Generic[L]):
    """ CHAR string type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'CHAR'

class VarChar(sqtabc.CharABC[L], typing.Generic[L]):
    """ VARCHAR string type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'VARCHAR'

class Binary(sqtabc.BinaryABC[L], typing.Generic[L]):
    """ CHAR string type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'BINARY'

class VarBinary(sqtabc.BinaryABC[L], typing.Generic[L]):
    """ VARCHAR string type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'VARBINARY'

class Blob(sqtabc.BlobABC, sqtabc.StringWithOptionalLengthABC[L], typing.Generic[L]):
    """ BLOB type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'BLOB'

    @classmethod
    def get_default_length(cls) -> int:
        return 2**16-1

class TinyBlob(sqtabc.BlobABC):
    """ Tiny blob type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'TINYBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**8-1

class MediumBlob(sqtabc.BlobABC):
    """ Medium blob type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'MEDIUMBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**24-1

class LongBlob(sqtabc.BlobABC):
    """ Long blob type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'LONGBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**32-1


class Text(sqtabc.TextABC, sqtabc.StringWithOptionalLengthABC[L], typing.Generic[L]):
    """ TEXT type """
    @classmethod
    def get_base_sql_type_name(cls) -> bytes:
        return b'TEXT'

    @classmethod
    def get_default_length(cls) -> int:
        return 2**16-1

class TinyText(sqtabc.TextABC):
    """ Tiny text type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'TINYTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**8-1

class MediumText(sqtabc.TextABC):
    """ Medium text type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'MEDIUMTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**24-1

class LongText(sqtabc.TextABC):
    """ Long Text type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        return b'LONGTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**32-1


class AnySQLType(sqtabc.SQLTypeABC):
    """ Any SQL Type """
    @classmethod
    def get_sql_type_name(cls) -> bytes:
        raise RuntimeError('Cannot get a type name of `AnySQLType`.')


# class Enum(sqtabc.StringABC, SQLTypeWithValues):
#     """ ENUM type """
#     @classmethod
#     def _validate_value(cls, v):
#         return v in cls.get_types()


# class Set(sqtabc.StringABC, SQLTypeWithValues):
#     """ SET type """
#     @classmethod
#     def _validate_value(cls, v):
#         if not isinstance(v, str):
#             raise RuntimeError('Invalid type.')
#         return all(cv.strip() in cls.get_types() for cv in v.split(','))

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


T = typing.TypeVar('T')
class Nullable(typing.Generic[T]):
    """ SQL optional type """
    @property
    def is_nullable(self) -> bool:
        return True

class NotNull(typing.Generic[T]):
    """ SQL NOT NULL type """
    @property
    def is_nullable(self) -> bool:
        return False

class Unique(typing.Generic[T]):
    """ SQL Unique type """
    @property
    def is_unique(self) -> bool:
        return True

class PrimaryKey(typing.Generic[T]):
    """ SQL Primary Key type """
    @property
    def is_primary(self) -> bool:
        return True


# class ForeignTableKey(sqtabc.Final, SQLTypeWithType[T], typing.Generic[T]):
#     """ Foreign table key (Primary key) """

#     @classmethod
#     # @lru_cache
#     def __class_getitem__(cls, item: typing.Type[object]):
#         assert issubclass(item, Record)
#         return cls.new_subclass(
#             f'FK__{item.__name__}',
#             (Int,),
#             _TYPE_BASE_  = item,
#             __TYPE_SQL__ = Int.__type_sql__(),
#             _TYPE_FOREIGN_KEY_ = item,
#         )


# SQLTypeEnv.set_type_alias(bool , Bool  )
# SQLTypeEnv.set_type_alias(int  , Int   )
# SQLTypeEnv.set_type_alias(float, Double)
# SQLTypeEnv.set_type_alias(str  , Text  )
# SQLTypeEnv.set_type_alias(bytes, Blob  )

# SQLTypeEnv.set_type_alias(datetime.date, Date)
# SQLTypeEnv.set_type_alias(datetime.time, Time)
# SQLTypeEnv.set_type_alias(datetime.datetime, DateTime)


# TypeLike = typing.Optional[typing.Union[typing.Type, bytes, str]]

# SQLType = typing.Union[TypedTableColumnABC, typing.Type]

def make_sql_type(typelike: typing.Type) -> typing.Type[sqtabc.SQLTypeABC]:

    _origin = typing.get_origin(typelike) or typelike

    if isinstance(_origin, type) and issubclass(_origin, sqtabc.SQLTypeABC):
        _ret = bind_generic_args(typelike)
        assert issubclass(_ret, sqtabc.SQLTypeABC)
        return _ret

    # if isinstance(typelike, str):
    #     if not typelike:
    #         return Text
    #     try:0
    #         return VarChar.with_length(int(typelike))
    #     except ValueError:
    #         pass
    
    # elif isinstance(typelike, bytes):
    #     if not typelike:
    #         return Blob
    #     try:
    #         return VarBinary.with_length(int(typelike))
    #     except ValueError:
    #         pass
    
    # else:

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

    raise RuntimeError('Invalid type for SQL type.', typelike)
