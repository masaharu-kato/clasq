""" SQL build-in data types """
import abc
import types
from typing import final
from functools import lru_cache

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
    def _validate_value(cls, v): # Default implementation
        return True 

    @classmethod
    def __type_sql__(cls): # Default implementation
        return cls.__TYPE_SQL__ or f'{cls.__name__} NOT NULL'

    @final
    @classmethod
    def validate_value(cls, v):
        return isinstance(v, cls._PY_TYPE_) and cls._validate_value(v)

    @final
    def validate(self):
        return self.validate_value(self.v)


class Optional(SQLType):
    """ SQL optional type """

    def __class_getitem__(cls, t):
        if not isinstance(t, type):
            raise RuntimeError('Invalid key type.')

        return types.new_class(
            f'{cls.__name__}_opt',
            (cls,),
            {},
            lambda ns: ns.update({
                '_MAX_LEN_': l,
                '__TYPE_SQL__': cls.__name__,
            })
        )





class SQLTypeWithOptionalLength(SQLType):
    """ SQL data types with optional length """

    def __class_getitem__(cls, l):
        if isinstance(l, int): 
            return cls._with_length(l)
        raise RuntimeError('Invalid key type.')

    @classmethod
    @lru_cache
    def _with_length(cls, l):
        assert isinstance(l, int)
        return types.new_class(
            f'{cls.__name__}_len{l}',
            (cls,),
            {},
            lambda ns: ns.update({
                '_MAX_LEN_': l,
                '__TYPE_SQL__': f'{cls.__name__}({l})',
            })
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

    def __int__(self, v):
        return int(self.v)


class UnsignedIntegerType(IntegerType):
    """ Unsigned integer types """
    _MIN_VALUE_ = 0

class TinyInt(IntegerType):
    _MIN_VALUE_ = -2**7
    _MAX_VALUE_ = 2**7-1
    
class SmallInt(IntegerType):
    _MIN_VALUE_ = -2**15
    _MAX_VALUE_ = 2**15-1
    
class MediumInt(IntegerType):
    _MIN_VALUE_ = -2**23
    _MAX_VALUE_ = 2**23-1

class Int(IntegerType):
    _MIN_VALUE_ = -2**31
    _MAX_VALUE_ = 2**31-1

class BigInt(IntegerType):
    _MIN_VALUE_ = -2**63
    _MAX_VALUE_ = 2**63-1

class UnsignedTinyInt(UnsignedIntegerType):
    _MAX_VALUE_ = 2**8-1
    
class UnsignedSmallInt(UnsignedIntegerType):
    _MAX_VALUE_ = 2**16-1
    
class UnsignedMediumInt(UnsignedIntegerType):
    _MAX_VALUE_ = 2**24-1

class UnsignedInt(UnsignedIntegerType):
    _MAX_VALUE_ = 2**32-1

class UnsignedBigInt(UnsignedIntegerType):
    _MAX_VALUE_ = 2**64-1


class FloatType(NumericType):
    """ Floating-point decimal types """
    _PY_TYPE_ = float

    def __float__(self):
        return float(self.v)

class Float(FloatType):
    """ Float type """

class Double(FloatType):
    """ Double type """


class DecimalType(NumericType):
    """ Fixed Decimal types """

class Decimal(DecimalType):
    """ Decimal type """
    # TODO: Specify precision and scale


class Bit(NumericType):
    """ Bit type """

    def __bytes__(self):
        return bytes(self.v)


class DatetimeType(SQLType):
    """ Date and time types """

class DateTime(DatetimeType):
    """ DateTime type """

class Date(DatetimeType):
    """ Date type """

class Time(DatetimeType):
    """ Time type """


class StringType():
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

class Char(CharType):
    """ CHAR string type """

class VarChar(CharType):
    """ VARCHAR string type """

class Binary(CharType):
    """ CHAR string type """

class VarBinary(CharType):
    """ VARCHAR string type """


class TextType(StringType, SQLTypeWithOptionalLength):
    """ Text types (with optional length) """
    @classmethod
    def _validate_value(cls, v):
        return not cls._MAX_LEN_ or len(v) <= cls._MAX_LEN_

class Blob(TextType):
    """ BLOB type """

class TinyBlob(Blob):
    """ Tiny blob type """

class MediumBlob(Blob):
    """ Medium blob type """

class LongBlob(Blob):
    """ Long blob type """


class Text(TextType):
    """ TEXT type """

class TinyText(Text):
    """ Tiny text type """

class MediumText(Text):
    """ Medium text type """

class LongText(Text):
    """ Long Text type """


class SQLTypeWithValues(TextType):
    """ SQL data types with optional length """

    @lru_cache
    def __class_getitem__(cls, values):
        if not isinstance(values, tuple) and all(isinstance(v, str) for v in values): 
            raise RuntimeError('Invalid values type.')
        return types.new_class(
            f'{cls.__name__}_vals_' + '_'.join(values),
            (cls,),
            {},
            lambda ns: ns.update({
                '_TYPE_VALUES_': values,
                '__TYPE_SQL__': f'{cls.__name__}(' + ', '.join(f"'{v}'" for v in values) + ')',
            })
        )

    def __new__(cls, *args, **kwargs):
        if not cls._TYPE_VALUES_:
            raise RuntimeError('Values is not specified.')
        return super().__new__(cls)


class Enum(StringType, SQLTypeWithValues):
    """ ENUM type """
    @classmethod
    def _validate_value(cls, v):
        return v in cls._TYPE_VALUES_


class Set(StringType, SQLTypeWithValues):
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


PY_DEFAULT_TYPES = {
    bool : Bool,
    int  : Int,
    float: Double,
    str  : Text,
    bytes: Blob,
} 



def main():
    vi = Int(25)
    print(vi)

    vchars = [
        VarChar[64]('hogefuga'),
        VarChar[32]('piyofoo'),
        VarChar[32]('baaaa'),
        VarChar[64]('efwaw'),
        # VarChar('awfefwea'),
        # VarChar('bbb'),
        Text[128]('hogefugapiyopiyo'),
        Text('afefwefjbifjawe32'),
    ]

    for i, vc in enumerate(vchars, 1):
        print(i, type(vc), id(type(vc)), vc.__type_sql__(), id(vc), vc)


if __name__ == '__main__':
    main()
