"""
    Definitions of built-in data types in SQL
"""
from __future__ import annotations
import typing
from .abc import data_types as dabc

L = typing.TypeVar('L', bound=int)

class TinyInt(dabc.IntegerABC):
    """ TINY INT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'TINYINT'
    @classmethod
    def get_v_limit_min(cls) -> int:
        return -2**7
    @classmethod
    def get_v_limit_max(cls) -> int:
        return 2**7-1
    
class SmallInt(dabc.IntegerABC):
    """ SMALLINT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'SMALLINT'
    @classmethod
    def get_v_limit_min(cls) -> int:
        return -2**15
    @classmethod
    def get_v_limit_max(cls) -> int:
        return 2**15-1
    
class MediumInt(dabc.IntegerABC):
    """ MEDIUMINT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'MEDIUMINT'
    @classmethod
    def get_v_limit_min(cls) -> int:
        return -2**23
    @classmethod
    def get_v_limit_max(cls) -> int:
        return 2**23-1
    
class Int(dabc.IntegerABC):
    """ INT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'INT'
    @classmethod
    def get_v_limit_min(cls) -> int:
        return -2**31
    @classmethod
    def get_v_limit_max(cls) -> int:
        return 2**31-1

class BigInt(dabc.IntegerABC):
    """ BIGINT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'BIGINT'
    @classmethod
    def get_v_limit_min(cls) -> int:
        return -2**63
    @classmethod
    def get_v_limit_max(cls) -> int:
        return 2**63-1

class UnsignedTinyInt(dabc.UnsignedIntegerABC):
    """ Unsigned TINYINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[dabc.IntegerABC]:
        return TinyInt
    
class UnsignedSmallInt(dabc.UnsignedIntegerABC):
    """ Unsigned SMALLINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[dabc.IntegerABC]:
        return SmallInt
    
class UnsignedMediumInt(dabc.UnsignedIntegerABC):
    """ Unsigned MEDIUMINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[dabc.IntegerABC]:
        return MediumInt

class UnsignedInt(dabc.UnsignedIntegerABC):
    """ Unsigned INT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[dabc.IntegerABC]:
        return Int

class UnsignedBigInt(dabc.UnsignedIntegerABC):
    """ Unsigned BIGINT type """
    @classmethod
    def get_signed_type(cls) -> typing.Type[dabc.IntegerABC]:
        return BigInt

class Float(dabc.FloatABC):
    """ Float type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'FLOAT'

class Double(dabc.FloatABC):
    """ Double type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'DOUBLE'

PREC = typing.TypeVar('PREC', bound=int)
SCALE = typing.TypeVar('SCALE', bound=int)
class Decimal(dabc.DecimalABC[PREC, SCALE], typing.Generic[PREC, SCALE]):
    """ Fixed Decimal types """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'DECIMAL'


class Bit(dabc.BitABC[L], typing.Generic[L]):
    """ Bit type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'BIT'

class DateTime(dabc.DateTimeABC):
    """ DateTime type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'DATETIME'

class Date(dabc.DateABC):
    """ Date type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'DATE'

class Time(dabc.TimeABC):
    """ Time type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'TIME'

class Char(dabc.StringWithLengthABC[L], typing.Generic[L]):
    """ CHAR string type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'CHAR'

class VarChar(dabc.StringWithLengthABC[L], typing.Generic[L]):
    """ VARCHAR string type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'VARCHAR'

class Binary(dabc.BinaryWithLengthABC[L], typing.Generic[L]):
    """ CHAR string type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'BINARY'

class VarBinary(dabc.BinaryWithLengthABC[L], typing.Generic[L]):
    """ VARCHAR string type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'VARBINARY'

class Blob(dabc.BinaryWithOptionalLengthABC[L], typing.Generic[L]):
    """ BLOB type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'BLOB'

    @classmethod
    @property
    def default_length(cls) -> int:
        return 2**16-1

class TinyBlob(dabc.BinaryABC):
    """ Tiny blob type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'TINYBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**8-1

class MediumBlob(dabc.BinaryABC):
    """ Medium blob type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'MEDIUMBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**24-1

class LongBlob(dabc.BinaryABC):
    """ Long blob type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'LONGBLOB'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**32-1


class Text(dabc.StringWithOptionalLengthABC[L], typing.Generic[L]):
    """ TEXT type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'TEXT'

    @classmethod
    @property
    def default_length(cls) -> int:
        return 2**16-1

class TinyText(dabc.StringABC):
    """ Tiny text type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'TINYTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**8-1

class MediumText(dabc.StringABC):
    """ Medium text type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'MEDIUMTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**24-1

class LongText(dabc.StringABC):
    """ Long Text type """
    @classmethod
    def get_sql_base_name(cls) -> bytes:
        return b'LONGTEXT'

    @classmethod
    def get_max_length(cls) -> int:
        return 2**32-1

SQT = typing.TypeVar('SQT', bound=dabc.DataTypeABC)
class Nullable(dabc.NullableABC[SQT], typing.Generic[SQT]):
    """ Nullable type """


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

ALL_TYPES: list[type[dabc.DataTypeABC]] = [
    TinyInt, SmallInt, MediumInt, Int, BigInt,
    UnsignedTinyInt, UnsignedSmallInt, UnsignedMediumInt, UnsignedInt, UnsignedBigInt,
    Float, Double, Decimal, Bit,
    DateTime, Date, Time,
    Char, VarChar, Binary, VarBinary,
    Text, TinyText, MediumText, LongText,
    Blob, TinyBlob, MediumBlob, LongBlob,
    Bool, Boolean, CharacterVarying, Fixed,
    Float4, Float8, INT1, INT2, INT3, INT4, INT8,
    LongVarBinary, LongVarchar, Long, MiddleInt, Numeric, Real,
]

__all__ = [cls.__name__ for cls in ALL_TYPES]
